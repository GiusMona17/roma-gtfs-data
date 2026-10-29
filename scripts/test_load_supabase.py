"""Unit tests for the pure parts of load_supabase.py (no database, no network).

Run from the repo root: python -m pytest satellite-repo-template/scripts/test_load_supabase.py
"""

from datetime import date

from load_supabase import (
    Feed,
    _shape_row,
    build_patterns,
    build_tables,
    expand_service_dates,
    parse_gtfs_date,
    parse_gtfs_time,
    relative_stops,
)


def _trip(trip_id, headsign="Centro", service="WK"):
    return {
        "trip_id": trip_id,
        "route_id": "R1",
        "service_id": service,
        "direction_id": "0",
        "trip_headsign": headsign,
        "shape_id": "S1",
    }


# (stop_sequence, stop_id, arrival_s, departure_s) for a two-stop trip 08:00 -> 08:05
TWO_STOPS = [
    (1, "A", parse_gtfs_time("08:00:00"), parse_gtfs_time("08:00:30")),
    (2, "B", parse_gtfs_time("08:05:00"), parse_gtfs_time("08:05:00")),
]


def test_parse_gtfs_time_accepts_hours_past_24():
    assert parse_gtfs_time("08:05:30") == 8 * 3600 + 5 * 60 + 30
    assert parse_gtfs_time("25:10:00") == 90600  # next day 01:10 in service-day terms
    assert parse_gtfs_time(" 26:00:00 ") == 26 * 3600


def test_parse_gtfs_time_blank_is_none_and_garbage_raises():
    assert parse_gtfs_time("") is None
    assert parse_gtfs_time(None) is None
    try:
        parse_gtfs_time("8:5")
    except ValueError:
        pass
    else:
        raise AssertionError("malformed GTFS time must raise")


def test_parse_gtfs_date():
    assert parse_gtfs_date("20260301") == date(2026, 3, 1)


def test_identical_trips_share_one_pattern():
    result = build_patterns(
        [_trip("t1"), _trip("t2")],
        {"t1": TWO_STOPS, "t2": TWO_STOPS},
    )
    assert [p[0] for p in result.patterns] == [1]
    assert {t[1] for t in result.trips} == {1}
    assert len(result.pattern_stops) == 2


def test_different_headsign_splits_patterns():
    result = build_patterns(
        [_trip("t1", "Centro"), _trip("t2", "Termini")],
        {"t1": TWO_STOPS, "t2": TWO_STOPS},
    )
    assert len(result.patterns) == 2


def test_same_stops_with_different_timing_splits_patterns():
    slower = [(1, "A", 28800, 28800), (2, "B", 29160, 29160)]  # 6 min instead of 5
    result = build_patterns(
        [_trip("t1"), _trip("t2")],
        {"t1": TWO_STOPS, "t2": slower},
    )
    assert len(result.patterns) == 2


def test_offsets_are_relative_to_first_arrival():
    stop_times = {"t1": [(1, "A", 28800, 28830), (2, "B", 29100, 29100)]}
    result = build_patterns([_trip("t1")], stop_times)
    assert result.trips == [("t1", 1, "WK", 28800)]
    assert result.pattern_stops == [(1, 1, "A", 0, 30), (1, 2, "B", 300, 300)]


def test_times_past_midnight_keep_relative_offsets():
    start, stops = relative_stops([
        (1, "A", parse_gtfs_time("25:10:00"), parse_gtfs_time("25:10:00")),
        (2, "B", parse_gtfs_time("25:12:00"), None),
    ])
    assert start == 90600
    assert stops == (("A", 0, 0), ("B", 120, 120))


def test_calendar_ranges_and_calendar_dates_add_and_remove():
    calendar = [{
        "service_id": "WK",
        "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1",
        "saturday": "0", "sunday": "0",
        "start_date": "20260302",  # Monday
        "end_date": "20260308",    # Sunday
    }]
    calendar_dates = [
        {"service_id": "WK", "date": "20260307", "exception_type": "1"},  # add Saturday
        {"service_id": "WK", "date": "20260303", "exception_type": "2"},  # remove Tuesday
        {"service_id": "WK", "date": "20260101", "exception_type": "2"},  # no-op
    ]
    active = expand_service_dates(calendar, calendar_dates)
    assert active == {
        (date(2026, 3, 2), "WK"),
        (date(2026, 3, 4), "WK"),
        (date(2026, 3, 5), "WK"),
        (date(2026, 3, 6), "WK"),
        (date(2026, 3, 7), "WK"),
    }


def test_calendar_dates_only_feed_without_calendar_file():
    active = expand_service_dates([], [{"service_id": "X", "date": "20260301", "exception_type": "1"}])
    assert active == {(date(2026, 3, 1), "X")}


def test_shape_row_parses_and_strips_shape_id():
    point = {"shape_id": " S1 ", "shape_pt_sequence": "3", "shape_pt_lat": "41.9028", "shape_pt_lon": "12.4964"}
    assert _shape_row(point) == ("S1", 3, 41.9028, 12.4964)


def test_build_tables_carries_shape_points():
    shape = {"shape_id": "S1", "shape_pt_sequence": "1", "shape_pt_lat": "41.5", "shape_pt_lon": "12.5"}
    feed = Feed(stops=[], routes=[], trips=[], stop_times={}, calendar=[], calendar_dates=[], shapes=[shape])
    assert build_tables(feed).shapes == [("S1", 1, 41.5, 12.5)]
