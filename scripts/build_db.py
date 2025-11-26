"""
GTFS Database Builder for Roma Mobilità
Scarica i dati GTFS, crea un database SQLite ottimizzato e genera metadata per l'aggiornamento.
"""

import os
import sqlite3
import pandas as pd
import zipfile
import requests
import shutil
import hashlib
import json
from datetime import datetime

# Configurazione
GTFS_URL = "https://romamobilita.it/sites/default/files/rome_static_gtfs.zip"
DB_NAME = "rome_gtfs.db"
OUTPUT_DIR = "output"

def calculate_md5(filepath):
    """Calcola hash MD5 di un file per rilevare cambiamenti"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_and_extract():
    """Scarica e estrae il file GTFS da Roma Mobilità"""
    print("=" * 60)
    print("STEP 1: Download GTFS da Roma Mobilità")
    print("=" * 60)
    
    # Check se il file esiste già (per testing locale)
    if os.path.exists("gtfs.zip"):
        print(f"✓ Usando file GTFS locale esistente")
        print(f"  Dimensione: {os.path.getsize('gtfs.zip') / (1024*1024):.2f} MB")
    else:
        try:
            print(f"Scaricamento da: {GTFS_URL}")
            response = requests.get(GTFS_URL, timeout=180)
            response.raise_for_status()
            
            with open("gtfs.zip", "wb") as f:
                f.write(response.content)
            
            print(f"✓ Scaricati {len(response.content) / (1024*1024):.2f} MB")
        except Exception as e:
            print(f"✗ Errore download: {e}")
            exit(1)
    
    print("\nEstrazione file ZIP...")
    try:
        with zipfile.ZipFile("gtfs.zip", "r") as z:
            z.extractall("temp_gtfs")
        
        files = os.listdir("temp_gtfs")
        print(f"✓ Estratti {len(files)} file: {', '.join(files)}")
    except Exception as e:
        print(f"✗ Errore estrazione: {e}")
        exit(1)

def create_database():
    """Crea il database SQLite con tabelle e indici ottimizzati"""
    print("\n" + "=" * 60)
    print("STEP 2: Creazione Database SQLite")
    print("=" * 60)
    
    # Prepara directory output
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    db_path = os.path.join(OUTPUT_DIR, DB_NAME)
    
    # Rimuovi vecchio database se esiste
    if os.path.exists(db_path):
        os.remove(db_path)
        print("✓ Rimosso database precedente")
    
    print(f"\nCreazione database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crea tabella di metadata Room (richiesta da Room per validazione schema)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS room_master_table (
            id INTEGER PRIMARY KEY,
            identity_hash TEXT
        )
    """)
    # Hash dell'identità del database (versione 1, come definito in GtfsDatabase.kt)
    # Questo hash deve corrispondere a quello generato da Room dalle Entity
    # Per ora usiamo un valore placeholder che Room aggiornerà al primo accesso
    cursor.execute("""
        INSERT INTO room_master_table (id, identity_hash) 
        VALUES (42, 'GTFS_DATABASE_V1_PLACEHOLDER')
    """)
    print("✓ Tabella room_master_table creata")
    
    # CREATE TABLE esplicite con schema corrispondente alle Room entities dell'app
    print("\nCreazione schema tabelle...")
    
    # Tabella routes - Schema ESATTO da RouteEntity
    cursor.execute("""
        CREATE TABLE routes (
            route_id TEXT PRIMARY KEY NOT NULL,
            agency_id TEXT,
            route_short_name TEXT NOT NULL,
            route_long_name TEXT NOT NULL,
            route_type TEXT,
            route_color TEXT,
            route_text_color TEXT
        )
    """)
    print("✓ Tabella routes creata")
    
    # Tabella stops - Schema da StopEntity
    cursor.execute("""
        CREATE TABLE stops (
            stop_id TEXT PRIMARY KEY NOT NULL,
            stop_code TEXT,
            stop_name TEXT NOT NULL,
            stop_desc TEXT,
            stop_lat TEXT NOT NULL,
            stop_lon TEXT NOT NULL,
            zone_id TEXT,
            stop_url TEXT,
            location_type TEXT,
            parent_station TEXT,
            stop_timezone TEXT,
            wheelchair_boarding TEXT
        )
    """)
    print("✓ Tabella stops creata")
    
    # Tabella trips - Schema da TripEntity
    cursor.execute("""
        CREATE TABLE trips (
            trip_id TEXT PRIMARY KEY NOT NULL,
            route_id TEXT NOT NULL,
            service_id TEXT NOT NULL,
            trip_headsign TEXT,
            trip_short_name TEXT,
            direction_id TEXT,
            block_id TEXT,
            shape_id TEXT,
            wheelchair_accessible TEXT,
            bikes_allowed TEXT
        )
    """)
    print("✓ Tabella trips creata")
    
    # Tabella stop_times - Chiave composta (trip_id, stop_sequence)
    cursor.execute("""
        CREATE TABLE stop_times (
            trip_id TEXT NOT NULL,
            arrival_time TEXT NOT NULL,
            departure_time TEXT NOT NULL,
            stop_id TEXT NOT NULL,
            stop_sequence INTEGER NOT NULL,
            stop_headsign TEXT,
            pickup_type TEXT,
            drop_off_type TEXT,
            shape_dist_traveled TEXT,
            timepoint TEXT,
            PRIMARY KEY (trip_id, stop_sequence)
        )
    """)
    print("✓ Tabella stop_times creata")
    
    # Tabella calendar - Schema da CalendarEntity (usa TEXT per i giorni, non INTEGER)
    cursor.execute("""
        CREATE TABLE calendar (
            service_id TEXT PRIMARY KEY NOT NULL,
            monday TEXT NOT NULL,
            tuesday TEXT NOT NULL,
            wednesday TEXT NOT NULL,
            thursday TEXT NOT NULL,
            friday TEXT NOT NULL,
            saturday TEXT NOT NULL,
            sunday TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL
        )
    """)
    print("✓ Tabella calendar creata")
    
    # Tabella calendar_dates - Chiave composta (usa TEXT per exception_type)
    cursor.execute("""
        CREATE TABLE calendar_dates (
            service_id TEXT NOT NULL,
            date TEXT NOT NULL,
            exception_type TEXT NOT NULL,
            PRIMARY KEY (service_id, date)
        )
    """)
    print("✓ Tabella calendar_dates creata")
    
    # Tabella shapes - Chiave composta (usa TEXT per lat/lon, non REAL, per preservare precisione)
    cursor.execute("""
        CREATE TABLE shapes (
            shape_id TEXT NOT NULL,
            shape_pt_lat TEXT NOT NULL,
            shape_pt_lon TEXT NOT NULL,
            shape_pt_sequence INTEGER NOT NULL,
            shape_dist_traveled TEXT,
            PRIMARY KEY (shape_id, shape_pt_sequence)
        )
    """)
    print("✓ Tabella shapes creata")
    
    # Tabella agency (solo campi definiti in AgencyEntity, non agency_fare_url o agency_email)
    cursor.execute("""
        CREATE TABLE agency (
            agency_id TEXT PRIMARY KEY NOT NULL,
            agency_name TEXT NOT NULL,
            agency_url TEXT NOT NULL,
            agency_timezone TEXT NOT NULL,
            agency_lang TEXT,
            agency_phone TEXT
        )
    """)
    print("✓ Tabella agency creata")
    
    conn.commit()
    
    # Importa dati dai CSV nelle tabelle con schema definito
    print("\n" + "=" * 60)
    print("STEP 2b: Importazione Dati GTFS")
    print("=" * 60)
    
    # Mappa: nome_tabella -> (file_csv, colonne_da_importare)
    imports = {
        "routes": ("routes.txt", ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"]),
        "stops": ("stops.txt", ["stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding"]),
        "trips": ("trips.txt", ["trip_id", "route_id", "service_id", "trip_headsign", "trip_short_name", "direction_id", "block_id", "shape_id", "wheelchair_accessible", "bikes_allowed"]),
        "stop_times": ("stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled", "timepoint"]),
        "calendar": ("calendar.txt", ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"]),
        "calendar_dates": ("calendar_dates.txt", ["service_id", "date", "exception_type"]),
        "shapes": ("shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"]),
        "agency": ("agency.txt", ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone"])
    }
    
    for table_name, (file_name, columns) in imports.items():
        file_path = os.path.join("temp_gtfs", file_name)
        
        if not os.path.exists(file_path):
            print(f"⚠ File {file_name} non trovato, skip")
            continue
        
        print(f"\nImportazione {table_name}...", end=" ")
        
        try:
            row_count = 0
            # Leggi CSV e seleziona solo le colonne che ci servono
            for chunk in pd.read_csv(file_path, dtype=str, chunksize=50000):
                # Seleziona solo le colonne che esistono sia nel CSV che nello schema
                available_cols = [col for col in columns if col in chunk.columns]
                missing_cols = [col for col in columns if col not in chunk.columns]
                
                if missing_cols:
                    print(f"\n⚠ Colonne mancanti nel CSV: {missing_cols}")
                
                data = chunk[available_cols].copy()
                
                # Pulizia dati: gestisci valori NULL per colonne NOT NULL
                if table_name == "routes":
                    # Se route_long_name è vuoto, usa route_short_name come fallback
                    data['route_long_name'] = data['route_long_name'].fillna(data['route_short_name'])
                    # Se route_short_name è vuoto, usa route_id come fallback
                    data['route_short_name'] = data['route_short_name'].fillna(data['route_id'])
                
                elif table_name == "stops":
                    # stop_name, stop_lat, stop_lon sono obbligatori
                    data['stop_name'] = data['stop_name'].fillna('Unknown Stop')
                    data['stop_lat'] = data['stop_lat'].fillna('0.0')
                    data['stop_lon'] = data['stop_lon'].fillna('0.0')
                
                elif table_name == "trips":
                    # route_id e service_id sono obbligatori ma dovrebbero sempre esserci
                    pass
                
                elif table_name == "stop_times":
                    # arrival_time, departure_time, stop_id sono obbligatori
                    pass
                
                elif table_name == "calendar":
                    # Tutti i campi dovrebbero essere presenti
                    pass
                
                # Inserisci dati nella tabella con schema predefinito
                # IMPORTANTE: if_exists="append" usa lo schema esistente dalla CREATE TABLE
                data.to_sql(table_name, conn, if_exists="append", index=False, dtype='text')
                row_count += len(chunk)
            
            print(f"✓ {row_count:,} righe")
        except Exception as e:
            print(f"✗ Errore: {e}")
            import traceback
            traceback.print_exc()
            conn.close()
            exit(1)
    
    # Creazione indici per performance
    print("\n" + "=" * 60)
    print("STEP 3: Creazione Indici (Performance Boost)")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    indices = [
        # Indici per stop_times (query più frequenti)
        # trip_id e stop_sequence sono già in PRIMARY KEY, ma stop_id serve per lookup
        ("idx_stop_times_stop_id", "stop_times", "stop_id"),
        
        # Indici per trips (route_id e service_id per JOIN)
        ("idx_trips_route_id", "trips", "route_id"),
        ("idx_trips_service_id", "trips", "service_id"),
        
        # Indici per shapes (mappe) - shape_id e sequence sono in PRIMARY KEY, ma serve indice separato per shape_id
        ("idx_shapes_id", "shapes", "shape_id"),
        
        # Indici per calendar_dates (service_id e date sono in PRIMARY KEY, ma servono lookup separati)
        ("idx_calendar_dates_service", "calendar_dates", "service_id"),
        ("idx_calendar_dates_date", "calendar_dates", "date")
    ]
    
    for idx_name, table, column in indices:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
            print(f"✓ Indice {idx_name} creato")
        except Exception as e:
            print(f"⚠ Indice {idx_name}: {e}")
    
    # Ottimizzazione finale
    print("\n" + "=" * 60)
    print("STEP 4: Ottimizzazione Database")
    print("=" * 60)
    
    print("Esecuzione VACUUM (compattazione)...", end=" ")
    cursor.execute("VACUUM")
    print("✓")
    
    print("Esecuzione ANALYZE (statistiche query)...", end=" ")
    cursor.execute("ANALYZE")
    print("✓")
    
    # Verifica integrità
    print("Verifica integrità database...", end=" ")
    cursor.execute("PRAGMA integrity_check")
    result = cursor.fetchone()
    if result[0] == "ok":
        print("✓ Database integro")
    else:
        print(f"✗ Errore integrità: {result}")
        conn.close()
        exit(1)
    
    conn.commit()
    conn.close()
    
    return db_path

def generate_metadata(db_path):
    """Genera file metadata.json con hash, dimensione e URL"""
    print("\n" + "=" * 60)
    print("STEP 5: Generazione Metadata")
    print("=" * 60)
    
    print("Calcolo hash MD5...", end=" ")
    db_hash = calculate_md5(db_path)
    print(f"✓ {db_hash}")
    
    db_size = os.path.getsize(db_path)
    print(f"Dimensione database: {db_size / (1024*1024):.2f} MB")
    
    build_date = datetime.now()
    date_str = build_date.strftime("%Y-%m-%d")
    
    metadata = {
        "version": build_date.strftime("%Y%m%d%H%M"),
        "date": build_date.isoformat(),
        "date_human": date_str,
        "hash": db_hash,
        "size_bytes": db_size,
        "size_mb": round(db_size / (1024*1024), 2),
        "download_url": f"https://github.com/YOUR_USERNAME/roma-gtfs-data/releases/download/db-{date_str}/rome_gtfs.db.gz",
        "source": GTFS_URL
    }
    
    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Metadata salvato in {manifest_path}")
    
    return metadata

def cleanup():
    """Rimuove file temporanei"""
    print("\n" + "=" * 60)
    print("STEP 6: Cleanup")
    print("=" * 60)
    
    if os.path.exists("gtfs.zip"):
        os.remove("gtfs.zip")
        print("✓ Rimosso gtfs.zip")
    
    if os.path.exists("temp_gtfs"):
        shutil.rmtree("temp_gtfs")
        print("✓ Rimossa cartella temp_gtfs")

def main():
    """Entry point principale"""
    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 10 + "GTFS DATABASE BUILDER - ROMA MOBILITÀ" + " " * 11 + "║")
    print("╚" + "═" * 58 + "╝")
    print()
    
    try:
        # Pipeline completa
        download_and_extract()
        db_path = create_database()
        metadata = generate_metadata(db_path)
        cleanup()
        
        # Riepilogo finale
        print("\n" + "╔" + "═" * 58 + "╗")
        print("║" + " " * 20 + "BUILD COMPLETATA" + " " * 22 + "║")
        print("╚" + "═" * 58 + "╝")
        print()
        print(f"📦 Database: {db_path}")
        print(f"📊 Dimensione: {metadata['size_mb']} MB")
        print(f"🔐 Hash: {metadata['hash']}")
        print(f"📅 Data: {metadata['date_human']}")
        print()
        print("✓ Il database è pronto per essere usato nell'app Android!")
        print()
        
    except Exception as e:
        print(f"\n✗ ERRORE FATALE: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
