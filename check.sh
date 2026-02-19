#!/usr/bin/env bash

# --- Configuration ---
datadir=${DATA_DIR:-/mnt/dImmich}
immich_container=${IMMICH_CONTAINER:-immich_server}
postgres_container=${POSTGRES_CONTAINER:-immich_postgres}
postgres_user=${POSTGRES_USER:-postgres}
postgres_db=${POSTGRES_DB:-immich}
immich_internal_path="/usr/src/app/upload/"

echo "--- Starting Immich Consistency Check ---"

# --- 1. Path Validation ---
echo "Validating path synchronization..."
sample_fs_file=$(sudo find "$datadir/library" -type f -not -iname "*.xmp" | head -n 1)

if [[ -z "$sample_fs_file" ]]; then
    echo "❌ ERROR: No files found in $datadir/library."
    exit 1
fi

sample_fs_normalized=$(echo "$sample_fs_file" | sed "s|$datadir/|$immich_internal_path|")
sample_filename=$(basename "$sample_fs_file")
sample_db_path=$(sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" -t -A -c "SELECT \"originalPath\" FROM asset WHERE \"originalPath\" LIKE '%$sample_filename%' LIMIT 1;")

if [[ "$sample_fs_normalized" != "$sample_db_path" ]]; then
    echo "❌ ERROR: Path Mismatch detected!"
    echo "Aborting. Please check your DATA_DIR or sed mapping."
    exit 1
else
    echo "✅ Path check successful: $sample_fs_normalized"
fi

# --- 2. Data Collection ---
echo "Collecting data (this may take a minute)..."

sql=$(cat <<'EOF'
select 'db' as source, 'asset' as type, "originalPath", "fileSizeInByte", "createdAt", "deletedAt", status, visibility, id
from asset left join asset_exif on "assetId" = "id" where not "isExternal"
EOF
)

find_cmd=$(cat <<EOF
{
find "$datadir/library"       -type f -not -iname ".immich" -not -iname "*.xmp" -exec stat -t -c "fs|library|%n|%s|%.19y" {} +
find "$datadir/upload"        -type f -not -iname ".immich" -not -iname "*.xmp" -exec stat -t -c "fs|upload|%n|%s|%.19y" {} +
find "$datadir/encoded-video" -type f -not -iname ".immich" -name "*-MP.mp4"    -exec stat -t -c "fs|encoded-video|%n|%s|%.19y" {} +
} | sed "s|$datadir/|$immich_internal_path|" | sort -k3,3 -t '|'
EOF
)

sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" --tuples-only --no-align -c "$sql" | sort -k3,3 -t '|' > db_sorted.tmp
bash -c "$find_cmd" > fs_sorted.tmp

# --- 3. Join and Analysis ---
echo "Performing comparison..."
HEADER="source|type|path|size|ts|db_source|db_type|db_path|db_size|db_ts|del_ts|status|visibility|id"

# Master Raw List
echo "$HEADER" > library.master.tsv
join -j3 -t '|' -a1 -a2 -o 1.1,1.2,1.3,1.4,1.5,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9 fs_sorted.tmp db_sorted.tmp >> library.master.tsv

# Export: Perfectly Synced
echo "$HEADER" > library.matched.tsv
grep -E "^fs.*\|db\|" library.master.tsv >> library.matched.tsv

# Export: On Disk but NOT in DB
echo "$HEADER" > onDiskNotInDB.tsv
grep -E "^fs.*\|\|\|\|\|\|\|\|\|" library.master.tsv >> onDiskNotInDB.tsv

# Export: In DB but NOT on Disk
echo "$HEADER" > inDBnotOnDisk.tsv
grep -E "^\|\|\|\|\|.*db" library.master.tsv >> inDBnotOnDisk.tsv

# Statistics for Summary
fuse_ghosts=$(grep -c "\.fuse_hidden" onDiskNotInDB.tsv)
orphan_size_bytes=$(awk -F'|' '{sum+=$4} END {print sum+0}' onDiskNotInDB.tsv)
orphan_size_gb=$(echo "scale=2; $orphan_size_bytes / 1024 / 1024 / 1024" | bc)
orphans_fs=$(grep -v "\.fuse_hidden" onDiskNotInDB.tsv | grep -cv "source|type|path")
missing_db=$(grep -cv "source|type|path" inDBnotOnDisk.tsv)

rm fs_sorted.tmp db_sorted.tmp

# --- 4. Final Summary ---
echo "-------------------------------------------------------"
echo "COMPLETED!"
echo "✅ SYNCED:  $(($(wc -l < library.matched.tsv)-1)) assets"
echo "❌ ORPHANS: $orphans_fs files (storage only) -> see onDiskNotInDB.tsv"
echo "⚠️  GHOSTS:  $fuse_ghosts .fuse_hidden files  -> see onDiskNotInDB.tsv"
echo "   TOTAL WASTED SPACE: $orphan_size_gb GB"
echo "❌ MISSING: $missing_db entries (DB only)    -> see inDBnotOnDisk.tsv"
echo "-------------------------------------------------------"
echo "GUIDE:"
echo "1. For onDiskNotInDB.tsv: These are files Immich doesn't know about."
echo "   Restart Docker to clear .fuse_hidden. Re-upload or delete the rest."
echo ""
echo "2. For inDBnotOnDisk.tsv: These entries point to missing files."
echo "   Use Immich UI: Administration -> Repair -> Remove Offline Assets."
echo "-------------------------------------------------------"
