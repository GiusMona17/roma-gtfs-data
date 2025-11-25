# Setup del Repository Satellite

## 🎯 Istruzioni Complete

### 1. Crea il repository su GitHub

1. Vai su https://github.com/new
2. Nome repository: `roma-gtfs-data`
3. Visibilità: **Pubblico** (importante per i download!)
4. ✅ Aggiungi README (poi lo sostituirai)
5. Click "Create repository"

### 2. Clona e configura

```bash
# Clona il repository appena creato
git clone https://github.com/YOUR_USERNAME/roma-gtfs-data.git
cd roma-gtfs-data

# Copia tutti i file da questa cartella template
# (copia il contenuto di satellite-repo-template/ dentro roma-gtfs-data/)
```

### 3. Aggiorna il README

Nel file `README.md`, sostituisci `YOUR_USERNAME` con il tuo username GitHub.

### 4. Prima build locale (test)

```bash
# Assicurati di avere Python 3.9+ installato
python --version

# Installa dipendenze
pip install pandas requests

# Esegui il builder
python scripts/build_db.py

# Dovresti vedere:
# - output/rome_gtfs.db (~30-40 MB)
# - output/manifest.json
```

### 5. Push su GitHub

```bash
git add .
git commit -m "Initial setup: GTFS database builder"
git push origin main
```

### 6. Test GitHub Actions

1. Vai su https://github.com/YOUR_USERNAME/roma-gtfs-data/actions
2. Click su "Build GTFS Database" nel menu laterale
3. Click su "Run workflow" → "Run workflow"
4. Attendi 2-5 minuti
5. Controlla la tab "Releases" → dovresti vedere:
   - `db-2025-11-24` (data odierna)
   - `latest-db`

### 7. Verifica i file scaricabili

Prova a scaricare:

```bash
# Scarica manifest
curl -L https://github.com/YOUR_USERNAME/roma-gtfs-data/releases/download/latest-db/manifest.json

# Dovresti vedere JSON con hash, dimensione, ecc.
```

## ✅ Checklist Completamento

- [ ] Repository creato su GitHub
- [ ] File copiati dal template
- [ ] README aggiornato con username corretto
- [ ] Build locale completata con successo
- [ ] Push su GitHub completato
- [ ] Workflow eseguito manualmente
- [ ] Release create (db-YYYY-MM-DD e latest-db)
- [ ] File scaricabili pubblicamente

## 🔧 Troubleshooting

### Errore "Permission denied" nella release

Vai su Settings → Actions → General → Workflow permissions → Seleziona "Read and write permissions"

### Errore download GTFS

Verifica che l'URL sia corretto: https://romamobilita.it/sites/default/files/rome_static_gtfs.zip

### Build locale lenta

Normale! Il processo può richiedere 2-5 minuti per scaricare ed elaborare tutti i dati.

## 📝 Note

- Il workflow si attiverà automaticamente ogni giorno alle 04:00 UTC
- Se i dati non cambiano, non viene creata una nuova release
- Il database compresso è ~10-15 MB (originale ~30-40 MB)
- Gli indici SQLite rendono le query 100x più veloci rispetto al parsing CSV

## 🎉 Prossimi Passi

Una volta completato questo setup, torna all'app Android per:
1. Scaricare il database generato
2. Integrarlo negli assets
3. Configurare Room per usarlo
4. Implementare il sistema di aggiornamento automatico
