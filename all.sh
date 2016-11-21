#!/bin/bash

# Check preconditions
#####################

if [[ $# -eq 1 ]]; then
    AGENCY_NAME=$(basename "${1}")
elif [[ $# -ne 2 ]] ; then
    printf "%s\n" "Invalid params set!"
    printf "%s\n" "Usage: ./$(basename "${0}") PATH AGENCY_NAME"
    exit 0
fi

# Variable definition
#####################

DIVA_PATH="${1}"
if [[ $# -eq 2 ]]; then
    AGENCY_NAME="${2}"
fi

FILE_BZW="${DIVA_PATH}/bzw"
FILE_LNRLIT="${DIVA_PATH}/lin/lnrlit"
FILE_TGTYP="${DIVA_PATH}/tgtyp"
FILES_ANSCHLB="${DIVA_PATH}/anschlb.*"
FILES_HALTESTELLEN="${DIVA_PATH}/haltestellen*"
FILES_VBESCH="${DIVA_PATH}/vbesch.*"
# Perform ~ expansion
DIVA_PATH="`eval echo ${DIVA_PATH}`"
# Perform * expansion
FILES_HALTESTELLEN=(${FILES_HALTESTELLEN[@]})

# Main method
#####################

printf "%s\n" "Building GTFS for ${AGENCY_NAME} from \"${DIVA_PATH}\""
printf "\n%s\n\n" "Initializing new databases"

./initdb.pl --clear all

printf "\n%s\n\n" "Loading data into DIVA database"

./loaddiva.pl ${FILES_HALTESTELLEN[@]/*.format32/}
./loaddiva.pl ${FILES_VBESCH}
./loaddiva.pl ${FILE_BZW}
./loaddiva.pl ${FILES_ANSCHLB}
#./loaddiva.pl ${FILE_TGTYP}
./loaddiva.pl ${FILE_LNRLIT}

printf "\n%s\n\n" "Transforming DIVA to GTFS"

./agencies2gtfs.pl
./stops2gtfs.pl
./service2gtfs.pl
./diva2gtfs.pl --path "${DIVA_PATH}/"
./transfers2gtfs.pl

printf "\n%s\n\n" "Exporting GTFS files"

./export.sh "${AGENCY_NAME}"
