#!/usr/bin/env python3
import csv
import sqlite3
from pathlib import Path

EXPORT_DIR = Path("exports")

def list_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cur.fetchall()]

def export_table_to_csv(conn: sqlite3.Connection, table: str, out_path: Path) -> None:
    cur = conn.execute(f'SELECT * FROM "{table}"')
    col_names = [desc[0] for desc in cur.description]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(cur.fetchall())

def main() -> int:
    here = Path.cwd()
    db_files = sorted(here.glob("*.db"))

    if not db_files:
        print("Keine .db Dateien im aktuellen Verzeichnis gefunden.")
        return 0

    # Export-Ordner erstellen (falls nicht vorhanden)
    EXPORT_DIR.mkdir(exist_ok=True)

    for db in db_files:
        print(f"Verarbeite Datenbank: {db.name}")
        dbname = db.stem

        try:
            conn = sqlite3.connect(str(db))
        except sqlite3.Error as e:
            print(f"  FEHLER: Konnte {db.name} nicht öffnen: {e}")
            continue

        try:
            tables = list_tables(conn)
            if not tables:
                print("  (Keine Tabellen gefunden)")
                continue

            for table in tables:
                out_file = EXPORT_DIR / f"{dbname}_{table}.csv"
                print(f"  Exportiere: {table} -> {out_file}")

                try:
                    export_table_to_csv(conn, table, out_file)
                except sqlite3.Error as e:
                    print(f"    FEHLER beim Export von {table}: {e}")
        finally:
            conn.close()

    print("Fertig.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())