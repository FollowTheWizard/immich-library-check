#!/usr/bin/env bash

# --- Configuration ---
# Adjust these variables if your setup differs
datadir=${DATA_DIR:-/mnt/Docker/Immich}
immich_container=${IMMICH_CONTAINER:-immich_server}
postgres_container=${POSTGRES_CONTAINER:-immich_postgres}
postgres_user=${POSTGRES_USER:-postgres}
postgres_db=${POSTGRES_DB:-immich}

# Internal path used by Immich inside the container
immich_internal_path="/usr/src/app/upload/"

echo "--- Starting Immich Consistency Check ---"

# --- 1. Path Validation (Auto-Check) ---
echo "Validating path synchronization..."

# Find a sample file to test the path mapping
sample_fs_file=$(sudo find "$datadir/library" -type f -not -iname "*.xmp" | head -n 1)

if [[ -z "$sample_fs_file" ]]; then
    echo "❌ ERROR: No files found in $datadir/library. Check your DATA_DIR variable."
    exit 1
fi

sample_fs_normalized=$(echo "$sample_fs_file" | sed "s|$datadir/|$immich_internal_path|")
sample_filename=$(basename "$sample_fs_file")

# Query the DB for the same file to see if the paths match
sample_db_path=$(sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" -t -A -c "SELECT \"originalPath\" FROM asset WHERE \"originalPath\" LIKE '%$sample_filename%' LIMIT 1;")

if [[ "$sample_fs_normalized" != "$sample_db_path" ]]; then
    echo "❌ ERROR: Path Mismatch detected!"
    echo "Filesystem (normalized): $sample_fs_normalized"
    echo "Database (actual):       $sample_db_path"
    echo "Aborting. Please check the 'sed' command in the script."
    exit 1
else
    echo "✅ Path check successful: $sample_fs_normalized"
fi

# --- 2. Data Collection ---
echo "Collecting data from DB and Filesystem (this may take a minute)..."

sql=$(cat <<'EOF'
select 'db' as source, 'asset' as type, "originalPath", "fileSizeInByte", "createdAt", "deletedAt", status, visibility, id
from asset 
left join asset_exif on "assetId" = "id" 
where not "isExternal"
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

# Export DB and FS data to temporary sorted files
sudo docker exec "$postgres_container" psql -U "$postgres_user" -d "$postgres_db" --tuples-only --no-align -c "$sql" | sort -k3,3 -t '|' > db_sorted.tmp
bash -c "$find_cmd" > fs_sorted.tmp

# --- 3. Join and Analysis ---
echo "Performing comparison..."
HEADER="source|type|path|size|ts|db_source|db_type|db_path|db_size|db_ts|del_ts|status|visibility|id"

# Master list
echo "$HEADER" > library.master.tsv
join -j3 -t '|' -a1 -a2 -o 1.1,1.2,1.3,1.4,1.5,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9 fs_sorted.tmp db_sorted.tmp >> library.master.tsv

# Success list
echo "$HEADER" > library.matched.tsv
grep -E "^fs.*\|db\|" library.master.tsv >> library.matched.tsv

# Mismatch list
echo "$HEADER" > library.mismatch.tsv
grep -vE "^fs.*\|db\|" library.master.tsv | grep -v "source|type|path" >> library.mismatch.tsv

# Statistics & Highlights
fuse_ghosts=$(grep -c "\.fuse_hidden" library.mismatch.tsv)
fuse_size_bytes=$(grep "\.fuse_hidden" library.mismatch.tsv | awk -F'|' '{sum+=$4} END {print sum+0}')
fuse_size_gb=$(echo "scale=2; $fuse_size_bytes / 1024 / 1024 / 1024" | bc)

orphans_fs=$(grep -E "^fs.*\|\|\|\|" library.mismatch.tsv | grep -v "\.fuse_hidden" | wc -l)
orphans_db=$(grep -E "^\|\|\|\|.*db" library.mismatch.tsv | wc -l)

# Cleanup
rm fs_sorted.tmp db_sorted.tmp

# --- 4. Final Output and Interpretation Guide ---
echo "-------------------------------------------------------"
echo "COMPLETED!"
echo "✅ Correctly Synchronized: $(($(wc -l < library.matched.tsv)-1))"
echo "❌ Inconsistencies Found:  $(($(wc -l < library.mismatch.tsv)-1))"
echo "-------------------------------------------------------"
echo "HOW TO INTERPRET THE RESULTS:"
echo ""
echo "⚠️  SYSTEM GHOSTS (FUSE-HIDDEN): $fuse_ghosts files (~$fuse_size_gb GB)"
echo "   -> Files starting with '.fuse_hidden...'. These consume space but are invisible to Immich."
echo "   -> CAUSE: Files deleted on the disk while still locked by a process (e.g., Docker)."
echo "   -> FIX: Restart Immich containers. If they persist, they can be manually deleted."
echo ""
echo "1. ORPHAN FILES ($orphans_fs files)"
echo "   -> Physical files on disk that are NOT in the Immich database."
echo "   -> CAUSE: Manual file movement or failed deletion jobs."
echo "   -> FIX: Use Immich CLI to re-upload or manually delete if not needed."
echo ""
echo "2. MISSING ASSETS ($orphans_db entries)"
echo "   -> Database entries where the physical file is GONE."
echo "   -> CAUSE: Manual deletion via Terminal or filesystem corruption."
echo "   -> FIX: In Immich UI: 'Administration -> Repair -> Remove Offline Assets'."
echo "-------------------------------------------------------"
echo "Details exported to: library.mismatch.tsv"
