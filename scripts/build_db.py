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
    
    try:
        print(f"Scaricamento da: {GTFS_URL}")
        response = requests.get(GTFS_URL, timeout=3000)
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
    
    # Mappa file CSV -> nome tabella
    # IMPORTANTE: Le colonne nel DB avranno gli stessi nomi degli header CSV
    files_to_import = {
        "stops": "stops.txt",
        "routes": "routes.txt",
        "trips": "trips.txt",
        "stop_times": "stop_times.txt",
        "calendar": "calendar.txt",
        "calendar_dates": "calendar_dates.txt",
        "shapes": "shapes.txt",
        "agency": "agency.txt"
    }
    
    # Importa ogni file CSV come tabella
    for table_name, file_name in files_to_import.items():
        file_path = os.path.join("temp_gtfs", file_name)
        
        if not os.path.exists(file_path):
            print(f"⚠ File {file_name} non trovato, skip")
            continue
        
        print(f"\nImportazione {table_name}...", end=" ")
        
        try:
            # Leggi in chunk per gestire file grandi
            # dtype=str previene errori di conversione tipo
            row_count = 0
            for chunk in pd.read_csv(file_path, dtype=str, chunksize=50000):
                chunk.to_sql(table_name, conn, if_exists="append", index=False)
                row_count += len(chunk)
            
            print(f"✓ {row_count:,} righe")
        except Exception as e:
            print(f"✗ Errore: {e}")
            conn.close()
            exit(1)
    
    # Creazione indici per performance
    print("\n" + "=" * 60)
    print("STEP 3: Creazione Indici (Performance Boost)")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    indices = [
        # Indici per stop_times (query più frequenti)
        ("idx_stop_times_stop_id", "stop_times", "stop_id"),
        ("idx_stop_times_trip_id", "stop_times", "trip_id"),
        ("idx_stop_times_sequence", "stop_times", "stop_sequence"),
        
        # Indici per trips
        ("idx_trips_route_id", "trips", "route_id"),
        ("idx_trips_service_id", "trips", "service_id"),
        ("idx_trips_trip_id", "trips", "trip_id"),
        
        # Indici per routes
        ("idx_routes_id", "routes", "route_id"),
        
        # Indici per stops
        ("idx_stops_id", "stops", "stop_id"),
        
        # Indici per shapes (mappe)
        ("idx_shapes_id", "shapes", "shape_id"),
        ("idx_shapes_sequence", "shapes", "shape_pt_sequence"),
        
        # Indici per calendar
        ("idx_calendar_service", "calendar", "service_id"),
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
