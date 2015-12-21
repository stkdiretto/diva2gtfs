#!/bin/bash

sqlite3 -header -csv diva2gtfs.db "select * from stops AS s where s.stop_id in (select distinct parent_station from stops AS st where location_type = 0 and st.stop_id in (select distinct stop_id from stop_times)) UNION select * from stops where stop_id in (select distinct stop_id from stop_times);" > autoexport/stops.txt
sqlite3 -header -csv diva2gtfs.db "select * from calendar where service_id in (select distinct service_id from trips);" > autoexport/calendar.txt
sqlite3 -header -csv diva2gtfs.db "select * from calendar_dates where service_id in (select distinct service_id from trips);" > autoexport/calendar_dates.txt
sqlite3 -header -csv diva2gtfs.db "select route_id, service_id, trip_id, trip_headsign, direction_id, block_id from trips;" > autoexport/trips.txt
sqlite3 -header -csv diva2gtfs.db "select * from routes;" > autoexport/routes.txt
sqlite3 -header -csv diva2gtfs.db "select * from stop_times;" > autoexport/stop_times.txt
sqlite3 -header -csv diva2gtfs.db "select * from agency;" > autoexport/agency.txt
