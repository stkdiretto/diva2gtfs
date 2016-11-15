#!/bin/bash

# Check preconditions

if [[ $# -ne 1 ]] ; then
    printf "%s\n" "Invalid params set!"
    printf "%s\n" "Usage: ./$(basename "${0}") AGENCY_NAME"
    exit 0
fi

AGENCY_NAME="$1"
DB_NAME="build/data/diva2gtfs.db"
OUTPUT_BASE="build/gtfs"
OUTPUT_DIR="${OUTPUT_BASE}/${AGENCY_NAME}"

## Main

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# Create GTFS file contents from database (into .txt files)

sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM stops AS s WHERE s.stop_id IN (SELECT DISTINCT parent_station FROM stops AS st WHERE location_type = 0 AND st.stop_id in (SELECT DISTINCT stop_id FROM stop_times)) UNION SELECT * FROM stops WHERE stop_id in (SELECT DISTINCT stop_id FROM stop_times);" > "${OUTPUT_DIR}/stops.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM calendar WHERE service_id in (SELECT DISTINCT service_id FROM trips);" > "${OUTPUT_DIR}/calendar.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM calendar_dates WHERE service_id in (SELECT DISTINCT service_id FROM trips);" > "${OUTPUT_DIR}/calendar_dates.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT route_id, service_id, trip_id, trip_headsign, direction_id, block_id FROM trips;" > "${OUTPUT_DIR}/trips.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM routes WHERE route_type IS NOT NULL;" > "${OUTPUT_DIR}/routes.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM stop_times;" > "${OUTPUT_DIR}/stop_times.txt"
sqlite3 -header -csv "${DB_NAME}" "SELECT * FROM agency;" > "${OUTPUT_DIR}/agency.txt"

# Create single GTFS archive file from .txt files

rm -rf "${OUTPUT_BASE}/${AGENCY_NAME}.zip"
zip -r -j "${OUTPUT_BASE}/${AGENCY_NAME}.zip" "${OUTPUT_BASE}/${AGENCY_NAME}"
