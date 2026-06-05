#!/bin/bash

# Target Directories
DEST_DIR="raw_data/gdelt"
mkdir -p ${DEST_DIR}

# Array of target dates (3 events * 21 days = 63 days)
# Event 1: Russia-Ukraine War (2022-02-24) -> 2022-02-17 to 2022-03-09
# Event 2: Israel-Hamas War (2023-10-07) -> 2023-09-30 to 2023-10-21
# Event 3: Iran-Israel Attack (2024-04-13) -> 2024-04-06 to 2024-04-27

# Function to download and unzip for a specific date range
download_range() {
    START_DATE=$1
    END_DATE=$2
    
    CURRENT_DATE=$(date -d "$START_DATE" +%Y%m%d)
    FINAL_DATE=$(date -d "$END_DATE" +%Y%m%d)
    
    while [ "$CURRENT_DATE" -le "$FINAL_DATE" ]; do
        URL="http://data.gdeltproject.org/events/${CURRENT_DATE}.export.CSV.zip"
        
        echo "Downloading GDELT: ${CURRENT_DATE}"
        wget -q -O "${DEST_DIR}/${CURRENT_DATE}.export.CSV.zip" "${URL}"
        
        if [ -f "${DEST_DIR}/${CURRENT_DATE}.export.CSV.zip" ]; then
            unzip -q -o "${DEST_DIR}/${CURRENT_DATE}.export.CSV.zip" -d "${DEST_DIR}"
            # Remove zip to save space after extraction
            rm -f "${DEST_DIR}/${CURRENT_DATE}.export.CSV.zip"
        fi
        
        # Move to next day
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
    done
}

echo "=== STARTING BULK DATA INGESTION ==="

echo "Processing Event 1: Russia-Ukraine War"
download_range "2022-02-17" "2022-03-09"

echo "Processing Event 2: Israel-Hamas War"
download_range "2023-09-30" "2023-10-21"

echo "Processing Event 3: Iran-Israel Conflict"
download_range "2024-04-06" "2024-04-27"

echo "Processing Daily Automation: Recent 5 Days GDELT Ingestion"
for i in {1..5}; do
    TARGET_DATE=$(date -d "$i days ago" +%Y-%m-%d)
    echo "Downloading GDELT for ${TARGET_DATE}..."
    download_range "${TARGET_DATE}" "${TARGET_DATE}"
done


echo "=== BULK DATA INGESTION COMPLETED ==="
