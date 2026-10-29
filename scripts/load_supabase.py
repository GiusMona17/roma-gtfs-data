#!/usr/bin/env python3
"""Load the Roma Mobilita static GTFS feed into Supabase Postgres (schema `gtfs`).

Usage:
    DATABASE_URL=postgresql://... python scripts/load_supabase.py [local_gtfs.zip]

Requires the schema migration supabase/migrations/0001_gtfs_schema.sql to be
applied first (it owns the RPC functions and public.immutable_unaccent).

Strategy: COPY into schema `gtfs_staging`, build indexes, then swap schemas in
one transaction. TRUNCATE + COPY was rejected because TRUNCATE holds an
ACCESS EXCLUSIVE lock until commit, so every RPC call would block for the whole
load (about a minute, daily at 04:00 UTC). With the swap, readers keep seeing the
old `gtfs` schema until commit, and a failed run rolls back with nothing changed.

Stdlib only at import time. psycopg and requests are imported where used, so the
pure helpers can be unit-tested without them.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import tempfile
import time
import traceback
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

GTFS_URL = "https://romamobilita.it/sites/default/files/rome_static_gtfs.zip"
STAGING_SCHEMA = "gtfs_staging"
SERVICE_TZ = "Europe/Rome"
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_TIME_RE = re.compile(r"^(\d+):([0-5]\d):([0-5]\d)$")

# (stop_sequence, stop_id, arrival_s or None, departure_s or None)
StopTime = tuple[int, str, int | None, int | None]
# ((stop_id, arrival_offset_s, departure_offset_s), ...) relative to first arrival
RelativeStops = tuple[tuple[str, int, int], ...]


# --- pure parsing helpers -----------------------------------------------------

def parse_gtfs_time(value: str | None) -> int | None:
    """Parse 'HH:MM:SS' into seconds since the service-day base. Hours may exceed 23."""
    if value is None or not value.strip():
        return None
    match = _TIME_RE.match(value.strip())
    if match is None:
        raise ValueError(f"invalid GTFS time: {value!r}")
    hours, minutes, seconds = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def parse_gtfs_date(value: str) -> date:
    """Parse a GTFS YYYYMMDD date."""
    return datetime.strptime(value.strip(), "%Y%m%d").date()


def _text(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip() or None


def _int(value: str | None) -> int | None:
    text = _text(value)
    return None if text is None else int(text)


# --- pattern compression ------------------------------------------------------

def relative_stops(stop_times: Sequence[StopTime]) -> tuple[int, RelativeStops] | None:
    """Return (first timed arrival in GTFS seconds, stops relative to it).

    Stops with no arrival and no departure time are dropped. Returns None when no
    stop of the trip has a time. Offsets are relative to the first timed arrival,
    which is the trip's start_sec, so arrivals are start_sec + offset.
    """
    timed: list[tuple[str, int, int]] = []
    for _, stop_id, arr, dep in sorted(stop_times, key=lambda row: row[0]):
        if arr is None and dep is None:
            continue
        arrival = arr if arr is not None else dep
        departure = dep if dep is not None else arr
        timed.append((stop_id, arrival, departure))
    if not timed:
        return None
    base = timed[0][1]
    return base, tuple((stop_id, arr - base, dep - base) for stop_id, arr, dep in timed)


@dataclass(frozen=True)
class Patterns:
    patterns: list[tuple]       # (pattern_id, route_id, direction_id, headsign, shape_id)
    pattern_stops: list[tuple]  # (pattern_id, seq, stop_id, arr_offset, dep_offset)
    trips: list[tuple]          # (trip_id, pattern_id, service_id, start_sec)


def build_patterns(
    trips: Iterable[Mapping[str, str]],
    stop_times: Mapping[str, Sequence[StopTime]],
) -> Patterns:
    """Group trips that share route, direction, headsign, shape and identical timed stops.

    Pattern ids are assigned in sorted key order, so they are stable across runs
    for the same input.
    """
    groups: dict[tuple, list[tuple[str, str, int]]] = defaultdict(list)
    for trip in trips:
        timed = relative_stops(stop_times.get(trip["trip_id"], ()))
        if timed is None:
            continue
        start_sec, stops = timed
        key = (
            trip["route_id"],
            _int(trip.get("direction_id")),
            _text(trip.get("trip_headsign")),
            _text(trip.get("shape_id")),
            stops,
        )
        groups[key].append((trip["trip_id"], trip["service_id"], start_sec))

    patterns: list[tuple] = []
    pattern_stops: list[tuple] = []
    trip_rows: list[tuple] = []
    for pattern_id, key in enumerate(sorted(groups, key=repr), start=1):
        route_id, direction_id, headsign, shape_id, stops = key
        patterns.append((pattern_id, route_id, direction_id, headsign, shape_id))
        pattern_stops.extend(
            (pattern_id, seq, stop_id, arr, dep)
            for seq, (stop_id, arr, dep) in enumerate(stops, start=1)
        )
        trip_rows.extend(
            (trip_id, pattern_id, service_id, start_sec)
            for trip_id, service_id, start_sec in groups[key]
        )
    return Patterns(patterns, pattern_stops, trip_rows)


# --- service calendar ---------------------------------------------------------

def expand_service_dates(
    calendar: Iterable[Mapping[str, str]],
    calendar_dates: Iterable[Mapping[str, str]],
) -> set[tuple[date, str]]:
    """Expand calendar.txt weekday ranges, then apply calendar_dates (1 add, 2 remove).

    Exceptions apply after all base ranges, whatever the file order. Either input
    may be empty (calendar.txt is optional).
    """
    active: set[tuple[date, str]] = set()
    for row in calendar:
        flags = [row[day].strip() == "1" for day in WEEKDAYS]
        first = parse_gtfs_date(row["start_date"])
        last = parse_gtfs_date(row["end_date"])
        for offset in range((last - first).days + 1):
            day = first + timedelta(days=offset)
            if flags[day.weekday()]:
                active.add((day, row["service_id"]))
    for row in calendar_dates:
        key = (parse_gtfs_date(row["date"]), row["service_id"])
        exception = row["exception_type"].strip()
        if exception == "1":
            active.add(key)
        elif exception == "2":
            active.discard(key)
    return active


# --- feed reading -------------------------------------------------------------

@dataclass(frozen=True)
class Feed:
    stops: list[Mapping[str, str]]
    routes: list[Mapping[str, str]]
    trips: list[Mapping[str, str]]
    stop_times: dict[str, list[StopTime]]
    calendar: list[Mapping[str, str]]
    calendar_dates: list[Mapping[str, str]]
    shapes: list[Mapping[str, str]]


@dataclass(frozen=True)
class Tables:
    routes: list[tuple]
    stops: list[tuple]
    patterns: Patterns
    service_dates: list[tuple]
    shapes: list[tuple]


def _csv_rows(zf: zipfile.ZipFile, name: str) -> Iterator[Mapping[str, str]]:
    """Yield rows of a CSV in the zip, or nothing when the file is absent."""
    if name not in zf.namelist():
        return
    with zf.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))


def _group_stop_times(rows: Iterable[Mapping[str, str]]) -> dict[str, list[StopTime]]:
    grouped: dict[str, list[StopTime]] = defaultdict(list)
    for row in rows:
        grouped[row["trip_id"]].append((
            int(row["stop_sequence"]),
            row["stop_id"],
            parse_gtfs_time(row.get("arrival_time")),
            parse_gtfs_time(row.get("departure_time")),
        ))
    return dict(grouped)


def read_feed(zip_path: str) -> Feed:
    with zipfile.ZipFile(zip_path) as zf:
        return Feed(
            stops=list(_csv_rows(zf, "stops.txt")),
            routes=list(_csv_rows(zf, "routes.txt")),
            trips=list(_csv_rows(zf, "trips.txt")),
            stop_times=_group_stop_times(_csv_rows(zf, "stop_times.txt")),
            calendar=list(_csv_rows(zf, "calendar.txt")),
            calendar_dates=list(_csv_rows(zf, "calendar_dates.txt")),
            shapes=list(_csv_rows(zf, "shapes.txt")),
        )


def _stop_row(stop: Mapping[str, str]) -> tuple | None:
    lat, lon = _text(stop.get("stop_lat")), _text(stop.get("stop_lon"))
    if lat is None or lon is None:
        return None  # geog is NOT NULL, so coordinate-less stops are skipped
    lat_f, lon_f = float(lat), float(lon)
    return (
        stop["stop_id"],
        _text(stop.get("stop_code")),
        _text(stop.get("stop_name")) or stop["stop_id"],
        lat_f,
        lon_f,
        f"SRID=4326;POINT({lon_f} {lat_f})",
    )


def _route_row(route: Mapping[str, str]) -> tuple:
    return (
        route["route_id"],
        _text(route.get("route_short_name")),
        _text(route.get("route_long_name")),
        _int(route.get("route_type")),
        _text(route.get("route_color")),
    )


def _shape_row(point: Mapping[str, str]) -> tuple:
    # shape_id is stripped like trips.shape_id, so the two join on equal text.
    return (
        point["shape_id"].strip(),
        int(point["shape_pt_sequence"]),
        float(point["shape_pt_lat"]),
        float(point["shape_pt_lon"]),
    )


def build_tables(feed: Feed) -> Tables:
    return Tables(
        routes=[_route_row(route) for route in feed.routes],
        stops=[row for row in map(_stop_row, feed.stops) if row is not None],
        patterns=build_patterns(feed.trips, feed.stop_times),
        service_dates=sorted(expand_service_dates(feed.calendar, feed.calendar_dates)),
        shapes=[_shape_row(point) for point in feed.shapes],
    )


# --- source acquisition -------------------------------------------------------

def fetch_zip(url: str, workdir: str) -> tuple[str, str]:
    """Download the zip into workdir. Returns (path, Last-Modified or current UTC time)."""
    import requests  # imported here so the pure helpers do not need it

    path = os.path.join(workdir, "rome_static_gtfs.zip")
    with requests.get(url, timeout=180, stream=True) as response:
        response.raise_for_status()
        with open(path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
        version = response.headers.get("Last-Modified") or datetime.now(timezone.utc).isoformat()
    return path, version


def resolve_source(local_path: str | None, workdir: str) -> tuple[str, str]:
    """Return (zip path, feed version). A local file uses its mtime as the version."""
    if local_path:
        mtime = datetime.fromtimestamp(os.path.getmtime(local_path), timezone.utc)
        return local_path, mtime.isoformat()
    return fetch_zip(GTFS_URL, workdir)


# --- database -----------------------------------------------------------------

# Staging DDL mirrors supabase/migrations/0001_gtfs_schema.sql. Keep them in sync.
# Indexes are created after COPY (faster bulk load); they are in _index_ddl().
def _table_ddl(schema: str) -> list[str]:
    tables = (
        "routes", "stops", "patterns", "pattern_stops", "trips", "service_dates", "feed_info", "shapes",
    )
    statements = [
        f"""create table {schema}.routes (
            route_id text primary key, short_name text, long_name text,
            route_type smallint, color text)""",
        f"""create table {schema}.stops (
            stop_id text primary key, stop_code text, name text not null,
            lat double precision, lon double precision,
            geog extensions.geography(Point,4326) not null)""",
        f"""create table {schema}.patterns (
            pattern_id integer primary key,
            route_id text not null references {schema}.routes (route_id),
            direction_id smallint, headsign text, shape_id text)""",
        f"""create table {schema}.pattern_stops (
            pattern_id integer not null, seq smallint not null, stop_id text not null,
            arr_offset integer not null, dep_offset integer not null,
            primary key (pattern_id, seq))""",
        f"""create table {schema}.trips (
            trip_id text primary key, pattern_id integer not null,
            service_id text not null, start_sec integer not null)""",
        f"""create table {schema}.service_dates (
            service_date date not null, service_id text not null,
            primary key (service_date, service_id))""",
        f"""create table {schema}.feed_info (
            version text primary key, loaded_at timestamptz not null default now())""",
        f"""create table {schema}.shapes (
            shape_id text not null, seq integer not null,
            lat double precision not null, lon double precision not null,
            primary key (shape_id, seq))""",
    ]
    statements += [f"alter table {schema}.{t} enable row level security" for t in tables]
    statements.append(f"revoke all on all tables in schema {schema} from anon, authenticated")
    return statements


def _index_ddl(schema: str) -> list[str]:
    return [
        f"create index stops_geog_gist on {schema}.stops using gist (geog)",
        f"create index stops_code_idx on {schema}.stops (stop_code)",
        f"create index stops_name_trgm on {schema}.stops using gin "
        "((public.immutable_unaccent(lower(name))) extensions.gin_trgm_ops)",
        f"create index pattern_stops_stop_idx on {schema}.pattern_stops (stop_id, pattern_id)",
        f"create index trips_pattern_idx on {schema}.trips (pattern_id)",
        f"create index patterns_route_idx on {schema}.patterns (route_id)",
    ]


# Runs in one transaction. gtfs_old is dropped only after gtfs has been renamed,
# so a crash before commit leaves the previous data in place.
SWAP_STATEMENTS = (
    "drop schema if exists gtfs_old cascade",
    "create schema if not exists gtfs",  # first run: nothing to rename yet
    "alter schema gtfs rename to gtfs_old",
    f"alter schema {STAGING_SCHEMA} rename to gtfs",
    "drop schema gtfs_old cascade",
)

ROUTE_COLS = ("route_id", "short_name", "long_name", "route_type", "color")
STOP_COLS = ("stop_id", "stop_code", "name", "lat", "lon", "geog")
PATTERN_COLS = ("pattern_id", "route_id", "direction_id", "headsign", "shape_id")
PATTERN_STOP_COLS = ("pattern_id", "seq", "stop_id", "arr_offset", "dep_offset")
TRIP_COLS = ("trip_id", "pattern_id", "service_id", "start_sec")
SERVICE_COLS = ("service_date", "service_id")
SHAPE_COLS = ("shape_id", "seq", "lat", "lon")


def _copy(cur, table: str, columns: Sequence[str], rows: Iterable[tuple]) -> int:
    count = 0
    with cur.copy(f"COPY {STAGING_SCHEMA}.{table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count


def _require_migration(cur) -> None:
    cur.execute("select to_regprocedure('public.immutable_unaccent(text)') is not null")
    (ready,) = cur.fetchone()
    if not ready:
        raise RuntimeError(
            "schema not found: apply supabase/migrations/0001_gtfs_schema.sql first"
        )


def load_database(db_url: str, tables: Tables, version: str) -> dict[str, int]:
    """COPY everything into the staging schema, then swap it in. Returns row counts."""
    import psycopg  # imported here so the pure helpers do not need it

    plan = (
        ("routes", ROUTE_COLS, tables.routes),
        ("stops", STOP_COLS, tables.stops),
        ("patterns", PATTERN_COLS, tables.patterns.patterns),
        ("pattern_stops", PATTERN_STOP_COLS, tables.patterns.pattern_stops),
        ("trips", TRIP_COLS, tables.patterns.trips),
        ("service_dates", SERVICE_COLS, tables.service_dates),
        ("shapes", SHAPE_COLS, tables.shapes),
    )
    counts: dict[str, int] = {}
    # psycopg commits on normal exit and rolls back on an exception.
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        _require_migration(cur)
        cur.execute(f"drop schema if exists {STAGING_SCHEMA} cascade")
        cur.execute(f"create schema {STAGING_SCHEMA}")
        for statement in _table_ddl(STAGING_SCHEMA):
            cur.execute(statement)
        for table, columns, rows in plan:
            counts[table] = _copy(cur, table, columns, rows)
        cur.execute(f"insert into {STAGING_SCHEMA}.feed_info (version) values (%s)", (version,))
        for statement in _index_ddl(STAGING_SCHEMA):
            cur.execute(statement)
        for statement in SWAP_STATEMENTS:
            cur.execute(statement)
    return counts


def _check_not_empty(tables: Tables) -> None:
    """Refuse to swap in an empty or truncated feed."""
    if not tables.stops or not tables.patterns.trips or not tables.service_dates:
        raise RuntimeError("feed looks empty (no stops, trips or service days); not loading")


# --- entry point --------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load Roma GTFS into Supabase.")
    parser.add_argument("source", nargs="?", help="local GTFS zip (default: download)")
    args = parser.parse_args(argv)

    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2

    started = time.monotonic()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            zip_path, version = resolve_source(args.source, workdir)
            print(f"Reading feed {zip_path} (version {version})")
            tables = build_tables(read_feed(zip_path))
            _check_not_empty(tables)
            counts = load_database(db_url, tables, version)
    except Exception:  # any failure must exit non-zero, with the traceback
        traceback.print_exc()
        return 1

    for table, rows in counts.items():
        print(f"  {table}: {rows:,} rows")
    print(f"Loaded schema gtfs in {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
