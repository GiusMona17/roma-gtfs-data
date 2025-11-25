# Roma GTFS Data Repository

🚍 **Database GTFS ottimizzato per Roma Mobilità**

Questo repository genera automaticamente un database SQLite pre-indicizzato contenente tutti i dati GTFS di Roma Mobilità.

## 📦 Cosa contiene

- **Database SQLite** (`rome_gtfs.db`) con tabelle ottimizzate:
  - `stops` - Fermate
  - `routes` - Linee
  - `trips` - Percorsi
  - `stop_times` - Orari di passaggio
  - `calendar` - Calendari di servizio
  - `calendar_dates` - Eccezioni calendario
  - `shapes` - Tracciati geografici
  - `agency` - Informazioni azienda trasporti

- **Indici ottimizzati** per query veloci
- **Metadata JSON** con hash MD5 e informazioni versione

## 🔄 Aggiornamento Automatico

Il database viene aggiornato **automaticamente ogni giorno alle 04:00 UTC** tramite GitHub Actions.

Se i dati GTFS non sono cambiati, non viene creata una nuova release.

## 📥 Download

### Ultima versione (latest)
Usa questi URL nell'app per scaricare sempre l'ultima versione:

```
https://github.com/GiusMona17/roma-gtfs-data/releases/download/latest-db/rome_gtfs.db.gz
https://github.com/GiusMona17/roma-gtfs-data/releases/download/latest-db/manifest.json
```

### Versione specifica
Ogni build crea un release con tag data (es. `db-2025-11-24`):

```
https://github.com/GiusMona17/roma-gtfs-data/releases/download/db-2025-11-24/rome_gtfs.db.gz
```

## 🛠️ Build Locale

Per generare il database manualmente:

```bash
# Installa dipendenze
pip install pandas requests

# Esegui lo script
python scripts/build_db.py

# Il database sarà in output/rome_gtfs.db
```

## 📊 Esempio di Utilizzo

```bash
# Scarica e decomprimi
wget https://github.com/GiusMona17/roma-gtfs-data/releases/download/latest-db/rome_gtfs.db.gz
gunzip rome_gtfs.db.gz

# Query di esempio
sqlite3 rome_gtfs.db <<EOF
-- Fermate con più linee
SELECT stop_name, COUNT(DISTINCT route_id) as num_routes
FROM stops s
JOIN stop_times st ON s.stop_id = st.stop_id
JOIN trips t ON st.trip_id = t.trip_id
GROUP BY s.stop_id
ORDER BY num_routes DESC
LIMIT 10;
EOF
```

## 🔗 Integrazione Android

Questo repository è pensato per essere usato con l'app **Quando Passa**.

L'app scarica il database compresso, lo decomprime e lo usa con Room per query ultra-veloci senza parsing CSV.

## 📄 Licenza

I dati GTFS sono di proprietà di **Roma Mobilità** e distribuiti secondo i loro termini di servizio.

Questo repository fornisce solo un'elaborazione dei dati per facilitarne l'utilizzo.

## 🤝 Contributi

Questo è un repository automatizzato. Per segnalazioni o miglioramenti, apri una issue.

---

*Database generato automaticamente da GitHub Actions* 🤖
