#!/bin/bash

# Check preconditions

if [[ $# -eq 0 ]] ; then
    echo 'No output agency specified!'
    exit 0
fi

AGENCY_NAME="$1"
DB_NAME="build/data/diva2gtfs.db"
OUTPUT_BASE="build/gtfs"
OUTPUT_DIR="${OUTPUT_BASE}/${AGENCY_NAME}"

## Main

mkdir -p "${OUTPUT_DIR}"

# Create GTFS file contents from database (into .txt files)

sqlite3 -header -csv "${DB_NAME}" "select * from stops AS s where s.stop_id in (select distinct parent_station from stops AS st where location_type = 0 and st.stop_id in (select distinct stop_id from stop_times)) UNION select * from stops where stop_id in (select distinct stop_id from stop_times);" > "${OUTPUT_DIR}/stops.txt"
sqlite3 -header -csv "${DB_NAME}" "select * from calendar where service_id in (select distinct service_id from trips);" > "${OUTPUT_DIR}/calendar.txt"
sqlite3 -header -csv "${DB_NAME}" "select * from calendar_dates where service_id in (select distinct service_id from trips);" > "${OUTPUT_DIR}/calendar_dates.txt"
sqlite3 -header -csv "${DB_NAME}" "select route_id, service_id, trip_id, trip_headsign, direction_id, block_id from trips;" > "${OUTPUT_DIR}/trips.txt"
sqlite3 -header -csv "${DB_NAME}" "select * from routes;" > "${OUTPUT_DIR}/routes.txt"
sqlite3 -header -csv "${DB_NAME}" "select * from stop_times;" > "${OUTPUT_DIR}/stop_times.txt"
sqlite3 -header -csv "${DB_NAME}" "select * from agency;" > "${OUTPUT_DIR}/agency.txt"

# Create single GTFS archive file from .txt files

rm -rf "${OUTPUT_BASE}/${AGENCY_NAME}.zip"
zip -r -j "${OUTPUT_BASE}/${AGENCY_NAME}.zip" "${OUTPUT_BASE}/${AGENCY_NAME}"
