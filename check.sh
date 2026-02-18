#!/usr/bin/env bash

# --- Konfiguration ---
datadir=${DATA_DIR:-/mnt/LeLuke/dDocker/dImmich}
immich_container=${IMMICH_CONTAINER:-immich_server}
postgres_container=${POSTGRES_CONTAINER:-immich_postgres}
postgres_user=${POSTGRES_USER:-postgres}
postgres_db=${POSTGRES_DB:-immich}

# Das interne Präfix, das Immich in der DB nutzt (haben wir per Test ermittelt)
immich_internal_path="/usr/src/app/upload/"

echo "--- Starte Immich Konsistenz-Check ---"

# --- 1. Pfad-Validierung (Der Auto-Check) ---
echo "Prüfe Pfad-Synchronität..."

# Test-Datei vom Dateisystem
sample_fs_file=$(find "$datadir/library" -type f -not -iname "*.xmp" | head -n 1)
sample_fs_normalized=$(echo "$sample_fs_file" | sed "s|$datadir/|$immich_internal_path|")

# Test-Pfad aus der DB (wir suchen nach dem Dateinamen der Test-Datei)
sample_filename=$(basename "$sample_fs_file")
sample_db_path=$(sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" -t -A -c "SELECT \"originalPath\" FROM asset WHERE \"originalPath\" LIKE '%$sample_filename%' LIMIT 1;")

if [[ "$sample_fs_normalized" != "$sample_db_path" ]]; then
    echo "❌ FEHLER: Pfade passen nicht zusammen!"
    echo "Dateisystem (normalisiert): $sample_fs_normalized"
    echo "Datenbank (Original):      $sample_db_path"
    echo "Abgleich abgebrochen. Bitte sed-Pfad im Skript prüfen."
    exit 1
else
    echo "✅ Pfad-Check erfolgreich: $sample_fs_normalized"
fi

# --- 2. Daten sammeln ---
echo "Sammle Daten aus DB und Dateisystem (das kann einen Moment dauern)..."

sql=$(cat <<'EOF'
select 'db' as source, 'asset' as type, "originalPath", "fileSizeInByte", "createdAt", "deletedAt", status, visibility, id
from asset
left join asset_exif on "assetId" = "id"
where not "isExternal"
EOF
)

# Dateisystem-Scan
find_cmd=$(cat <<EOF
{
find "$datadir/library"       -type f -not -iname ".immich" -not -iname "*.xmp" -exec stat -t -c "fs|library|%n|%s|%.19y" {} +
find "$datadir/upload"        -type f -not -iname ".immich" -not -iname "*.xmp" -exec stat -t -c "fs|upload|%n|%s|%.19y" {} +
find "$datadir/encoded-video" -type f -not -iname ".immich" -name "*-MP.mp4"    -exec stat -t -c "fs|encoded-video|%n|%s|%.19y" {} +
} | sed "s|$datadir/|$immich_internal_path|" | sort -k3,3 -t '|'
EOF
)

# SQL-Daten holen und sortieren
sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" --tuples-only --no-align -c "$sql" \
  | sort -k3,3 -t '|' > db_sorted.tmp

# FS-Daten holen (sortiert bereits im find_cmd)
bash -c "$find_cmd" > fs_sorted.tmp

# --- 3. Join und Auswertung ---
echo "Führe Abgleich durch..."
echo "source|type|path|size|ts|db_source|db_type|db_path|db_size|db_ts|del_ts|status|visibility|id" > library.tsv

join -j3 -t '|' -a1 -a2 -o 1.1,1.2,1.3,1.4,1.5,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9 \
  fs_sorted.tmp \
  db_sorted.tmp \
  | tr '|' $'\t' >> library.tsv

# Mismatches finden (Zeilen, die entweder nur in fs oder nur in db existieren)
# Ein normaler Asset hat in Spalte 1 'fs' stehen UND in Spalte 6 'db'
grep -vE "^fs.*db" library.tsv | grep -v "source|type|path" > library.mismatch.tsv

# Cleanup
rm fs_sorted.tmp db_sorted.tmp

echo "---------------------------------------"
echo "Fertig!"
echo "Gesamtliste:    library.tsv"
echo "Inkonsistenzen: library.mismatch.tsv ($(wc -l < library.mismatch.tsv) Einträge)"