#!/bin/bash

OUTPUT_DIR="build/gtfs"

mkdir -p "${OUTPUT_DIR}"

sqlite3 -header -csv diva2gtfs.db "select * from stops AS s where s.stop_id in (select distinct parent_station from stops AS st where location_type = 0 and st.stop_id in (select distinct stop_id from stop_times)) UNION select * from stops where stop_id in (select distinct stop_id from stop_times);" > "${OUTPUT_DIR}/stops.txt"
sqlite3 -header -csv diva2gtfs.db "select * from calendar where service_id in (select distinct service_id from trips);" > "${OUTPUT_DIR}/calendar.txt"
sqlite3 -header -csv diva2gtfs.db "select * from calendar_dates where service_id in (select distinct service_id from trips);" > "${OUTPUT_DIR}/calendar_dates.txt"
sqlite3 -header -csv diva2gtfs.db "select route_id, service_id, trip_id, trip_headsign, direction_id, block_id from trips;" > "${OUTPUT_DIR}/trips.txt"
sqlite3 -header -csv diva2gtfs.db "select * from routes;" > "${OUTPUT_DIR}/routes.txt"
sqlite3 -header -csv diva2gtfs.db "select * from stop_times;" > "${OUTPUT_DIR}/stop_times.txt"
sqlite3 -header -csv diva2gtfs.db "select * from agency;" > "${OUTPUT_DIR}/agency.txt"
