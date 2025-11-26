import sqlite3

conn = sqlite3.connect('../output/rome_gtfs.db')
cur = conn.cursor()
cur.execute('SELECT name FROM sqlite_master WHERE type="index" AND sql IS NOT NULL ORDER BY name')
for row in cur.fetchall():
    print(row[0])
conn.close()
