#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use File::Path qw(make_path);
use Getopt::Long;

dbconnect();

GetOptions	(
	"drop=s"	=>	\&drophandler,
	"create=s"	=>	\&createhandler,
	"clear=s"	=>	\&clearhandler,
) or die("Error in command line arguments\n");

my $dbh;
my $divadbh;

disconnect();

sub drophandler {
	my ($opt_name, $opt_value) = @_;
	if ($opt_value eq "divastops") {
		dropdiva_stops();
	} elsif ($opt_value eq "diva") {
		dropdiva();
	} elsif ($opt_value eq "divaservice") {
		dropdiva_servicerestrictions();
	} elsif ($opt_value eq "divalnrlit") {
		dropdiva_lnrlit();
	} elsif ($opt_value eq "gtfsfares") {
		dropgtfs_fares();
	} elsif ($opt_value eq "gtfsstops") {
		dropgtfs_stops();
	} elsif ($opt_value eq "gtfsruns") {
		dropgtfs_runs();
	} elsif ($opt_value eq "gtfscalendar") {
		dropgtfs_calendar();
	} elsif ($opt_value eq "gtfs") {
		dropgtfs();
	} elsif ($opt_value eq "all") {
		dropgtfs();
		dropdiva();
	} else {
		print "Invalid argument for $opt_name\n";
	}
}

sub createhandler {
	my ($opt_name, $opt_value) = @_;
	if ($opt_value eq "divastops") {
		creatediva_stops();
	} elsif ($opt_value eq "diva") {
		creatediva();
	} elsif ($opt_value eq "divaservice") {
		creatediva_servicerestrictions();
	} elsif ($opt_value eq "divalnrlit") {
		creatediva_lnrlit();
	} elsif ($opt_value eq "gtfsfares") {
		creategtfs_fares();
	} elsif ($opt_value eq "gtfsstops") {
		creategtfs_stops();
	} elsif ($opt_value eq "gtfsruns") {
		creategtfs_runs();
	} elsif ($opt_value eq "gtfscalendar") {
		creategtfs_calendar();
	} elsif ($opt_value eq "gtfs") {
		creategtfs();
	} elsif ($opt_value eq "all") {
		creategtfs();
		creatediva();
	} else {
		print "Invalid argument for $opt_name\n";
	}
}

sub clearhandler {
	my ($opt_name, $opt_value) = @_;
	if ($opt_value eq "divastops") {
		cleardiva_stops();
	} elsif ($opt_value eq "divalnrlit") {
		cleardiva_lnrlit();
	} elsif ($opt_value eq "divaservice") {
		cleardiva_servicerestrictions();
	} elsif ($opt_value eq "diva") {
		cleardiva();
	} elsif ($opt_value eq "gtfsstops") {
		cleargtfs_stops();
	} elsif ($opt_value eq "gtfsfares") {
		cleargtfs_fares();
	} elsif ($opt_value eq "gtfscalendar") {
		cleargtfs_calendar();
	} elsif ($opt_value eq "gtfsruns") {
		cleargtfs_runs();
	} elsif ($opt_value eq "gtfs") {
		cleargtfs();
	} elsif ($opt_value eq "all") {
		cleargtfs();
		cleardiva();
	} else {
		print "Invalid argument for $opt_name\n";
	}
}

# --------------------
# CONNECT TO DATABASE
# --------------------

sub dbconnect {
	my $db_folder = "build/data";
	make_path($db_folder);

	my $driver   = "SQLite";
	my $database = "$db_folder/diva2gtfs.db";
	my $dsn = "DBI:$driver:dbname=$database";
	my $userid = "";
	my $password = "";
	$dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
		                    or die $DBI::errstr;
	print "Opened database successfully\n";

	my $divadatabase = "$db_folder/divadata.db";
	my $divadsn = "DBI:$driver:dbname=$divadatabase";
	$divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 })
		                    or die $DBI::errstr;
}

# --------------------
# GTFS TABLES
# --------------------


sub dropgtfs_stops {
	$dbh->do("DROP TABLE IF EXISTS stops");
}

sub dropgtfs_runs {
	$dbh->do("DROP TABLE IF EXISTS routes");
	$dbh->do("DROP TABLE IF EXISTS trips");
	$dbh->do("DROP TABLE IF EXISTS stop_times");
}

sub dropgtfs_calendar {
	$dbh->do("DROP TABLE IF EXISTS calendar_dates");
	$dbh->do("DROP TABLE IF EXISTS calendar");
}

sub dropgtfs_fares {
	$dbh->do("DROP TABLE IF EXISTS fare_attributes");
	$dbh->do("DROP TABLE IF EXISTS fare_rules");
}

sub dropgtfs {
	dropgtfs_stops();
	dropgtfs_runs();
	dropgtfs_calendar();
	dropgtfs_fares();
	$dbh->do("DROP TABLE IF EXISTS agency");
	$dbh->do("DROP TABLE IF EXISTS shapes");
	$dbh->do("DROP TABLE IF EXISTS transfers");
	$dbh->do("DROP TABLE IF EXISTS feed_info");
}


sub creategtfs_stops {
	$dbh->do("CREATE TABLE IF NOT EXISTS stops (stop_id TEXT, stop_code TEXT, stop_name TEXT, stop_lat REAL, stop_lon REAL, zone_id TEXT, location_type INTEGER, parent_station INTEGER, wheelchair_boarding TEXT)");
}

sub creategtfs_runs {
	$dbh->do("CREATE TABLE IF NOT EXISTS routes (route_id TEXT PRIMARY KEY,agency_id TEXT,route_short_name TEXT,route_long_name TEXT,route_type TEXT,route_color TEXT,route_text_color TEXT)");
	$dbh->do("CREATE TABLE IF NOT EXISTS trips (route_id TEXT,service_id TEXT,trip_id TEXT PRIMARY KEY,trip_headsign TEXT, trip_short_name TEXT, direction_id INTEGER,block_id INTEGER,shape_id TEXT)");
	$dbh->do("CREATE INDEX IF NOT EXISTS tr_rid ON trips(route_id)");
	$dbh->do("CREATE TABLE IF NOT EXISTS stop_times (trip_id TEXT, arrival_time TEXT, departure_time TEXT, stop_id TEXT, stop_sequence INTEGER, stop_headsign TEXT, pickup_type INTEGER, drop_off_type INTEGER, shape_dist_traveled REAL)");
	$dbh->do("CREATE INDEX IF NOT EXISTS st_trid ON stop_times(trip_id)");
	$dbh->do("CREATE INDEX IF NOT EXISTS st_stid ON stop_times(stop_id)");
	$dbh->do("CREATE INDEX IF NOT EXISTS st_starrtime ON stop_times(arrival_time)");
	$dbh->do("CREATE INDEX IF NOT EXISTS st_stdeptime ON stop_times(departure_time)");
}

sub creategtfs_calendar {
	$dbh->do("CREATE TABLE IF NOT EXISTS calendar_dates (service_id Text, date TEXT, exception_type INTEGER, PRIMARY KEY (service_id, date))");
	$dbh->do("CREATE INDEX IF NOT EXISTS cd_service ON calendar_dates(service_id)");
	$dbh->do("CREATE INDEX IF NOT EXISTS cd_date ON calendar_dates(date)");
	$dbh->do("CREATE TABLE IF NOT EXISTS calendar (service_id Text PRIMARY KEY, monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, start_date TEXT, end_date TEXT)");
}

sub creategtfs_fares {
	$dbh->do("CREATE TABLE IF NOT EXISTS fare_attributes (fare_id TEXT PRIMARY KEY, price REAL, currency_type TEXT, payment_method INTEGER, transfers INTEGER, transfer_duration INTEGER)");
	$dbh->do("CREATE TABLE IF NOT EXISTS fare_rules (fare_id TEXT, route_id TEXT, origin_id TEXT, destination_id TEXT, contains_id TEXT)");
}

sub creategtfs {
	print "Trying to newly create GTFS tables\n";
	creategtfs_stops();
	creategtfs_runs();
	creategtfs_calendar();
	creategtfs_fares();
	$dbh->do("CREATE TABLE IF NOT EXISTS agency (agency_id TEXT PRIMARY KEY, agency_name TEXT, agency_url TEXT, agency_timezone TEXT, agency_lang TEXT, agency_phone TEXT, agency_fare_url TEXT)");
	$dbh->do("CREATE TABLE IF NOT EXISTS shapes (shape_id TEXT, shape_pt_lat REAL, shape_pt_lon REAL, shape_pt_sequence INTEGER, shape_dist_traveled REAL)");
	$dbh->do("CREATE TABLE IF NOT EXISTS transfers (from_stop_id TEXT, to_stop_id TEXT, transfer_type INTEGER, min_transfer_time INTEGER, from_route_id TEXT, to_route_id TEXT, from_trip_id TEXT, to_trip_id TEXT)");
	$dbh->do("CREATE TABLE IF NOT EXISTS feed_info (feed_publisher_name TEXT, feed_publisher_url TEXT, feed_lang TEXT, feed_start_date INTEGER, feed_end_date INTEGER, feed_version TEXT)");
}

sub cleargtfs {
	dropgtfs();
	creategtfs();
}

sub cleargtfs_calendar {
	dropgtfs_calendar();
	creategtfs_calendar();
}

sub cleargtfs_runs {
	dropgtfs_runs();
	creategtfs_runs();
}

sub cleargtfs_stops {
	dropgtfs_stops();
	creategtfs_stops();
}

# -----------------------
# DIVA TABLES
# -----------------------

sub creatediva_agencies {
	$divadbh->do("CREATE TABLE IF NOT EXISTS OpBranch ( IDX_Version INTEGER, bzw VARCHAR(2), bzwtext VARCHAR(40), bzwkb VARCHAR(6), vm CHAR,lkrnr VARCHAR(5), fettdruck CHAR, hst_schreibweise CHAR, stundensatz double, rbl_nummer INTEGER, tstr_laenge_auswahl INTEGER, GisLaengenVerwenden BOOL, sOperatorCode VARCHAR(7), input TEXT)");
}

sub creatediva_stops{
	print ("Trying to newly create DIVA stop tables\n");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop( _AutoKey_ INTEGER, IDX_Version INTEGER, hstnr INTEGER, plan VARCHAR(2), dat_v INTEGER, dat_b INTEGER, mdat INTEGER, mzeit INTEGER, a_bzn INTEGER, dbkenn VARCHAR(6), tpunkt CHAR, hstart CHAR, tcode VARCHAR(3), tarifnr INTEGER, zielcode INTEGER, gkz VARCHAR(8), ortsname VARCHAR(20), glklein VARCHAR(6), a_attr INTEGER, ber CHAR, XYT_x_koord VARCHAR(8), XYT_y_koord VARCHAR(8), a_hst_mast INTEGER, a_hst_steig INTEGER, a_hst_koord INTEGER, a_umgeb_plan INTEGER, transformnr INTEGER, zwangsumsteigen CHAR, umsteigequalitaet INTEGER, a_georef INTEGER, flughafenkennung VARCHAR(3), a_hst_ahf_bdat INTEGER, hstnr_gdm INTEGER, hstkbez VARCHAR(8), zurPausezeit INTEGER, m_lGISVerkehrsmittel INTEGER, m_cAstHst CHAR, m_pPostleitzahl VARCHAR(1024), hstname VARCHAR(30), anschlusssicherung BOOL, ansagetextnummer INTEGER, hstnr_fahrscheindrucker INTEGER, a_Zhst INTEGER, transformnr2 INTEGER, m_pBenutzerName VARCHAR(1024), m_pKommentar VARCHAR(1024), a_hst_gebiet INTEGER, a_hst_alias_orte INTEGER, m_lTuNummer INTEGER, m_TuSchluessel VARCHAR(3), m_pHaltestellenLage VARCHAR(1024), m_bZentraleHaltestelle BOOL, SpaVlpUsage TINYINT, m_lHstAllgAttribute INTEGER, GisFootPathRegionLayer VARCHAR(99), GisFootPathRegionID INTEGER, GisBuildingRegionLayer VARCHAR(99), GisBuildingRegionID INTEGER, strInnenanzeiger_1 VARCHAR(256), strInnenanzeiger_2 VARCHAR(256), strInnenanzeiger_3 VARCHAR(256), GlobalID VARCHAR(1024), ModInfo__Time INTEGER, ModInfo__Flag INTEGER, input TEXT,
 PRIMARY KEY (_AutoKey_, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS ArrayHaltezoneGeoref ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, ID INTEGER, RICHTUNG CHAR, GISMAP VARCHAR(4), STATUS CHAR, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS HalteZoneGeoRefKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, COORD1_plan VARCHAR(4), COORD1_status CHAR, COORD1_x INTEGER, COORD1_y INTEGER, COORD1_disp_x INTEGER, COORD1_disp_y INTEGER, COORD1_text_x INTEGER, COORD1_text_y INTEGER, COORD2_plan VARCHAR(4), COORD2_status CHAR, COORD2_x INTEGER, COORD2_y INTEGER, COORD2_disp_x INTEGER, COORD2_disp_y INTEGER, COORD2_text_x INTEGER, COORD2_text_y INTEGER, GeoRef1_id INTEGER, GeoRef1_abst_von INTEGER, GeoRef1_abst_bis INTEGER, GeoRef1_seite CHAR, GeoRef1_status CHAR, GeoRef1_quell_name VARCHAR(4), GeoRef1_strassenname VARCHAR(1024), GeoRef1_GisVm INTEGER, GeoRef2_id INTEGER, GeoRef2_abst_von INTEGER, GeoRef2_abst_bis INTEGER, GeoRef2_seite CHAR, GeoRef2_status CHAR, GeoRef2_quell_name VARCHAR(4), GeoRef2_strassenname VARCHAR(1024), GeoRef2_GisVm INTEGER, input TEXT	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopAreaGIS ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, CharArr VARCHAR(4), input TEXT	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopAreaGeoRefKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, GisName VARCHAR(4), GisVm INTEGER, Koord_x INTEGER, Koord_y INTEGER, input TEXT	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopAreaGeoref ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, id INTEGER, abst_von INTEGER, abst_bis INTEGER, seite CHAR, status CHAR, quell_name VARCHAR(4), strassenname VARCHAR(1024), GisVm INTEGER, input TEXT	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopAreaKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, plan VARCHAR(4), status CHAR, x INTEGER, y INTEGER, z INTEGER, disp_x INTEGER, disp_y INTEGER, text_x INTEGER, text_y INTEGER, input TEXT,
 PRIMARY KEY(_FK__AutoKey_,_FK_ARR_IDX,input)	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopAreaMetanumber ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, typ VARCHAR(3), Nummer INTEGER, input TEXT	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopKeyInterval ( betrieb VARCHAR(10), hstnr_von INTEGER, hstnr_bis INTEGER, input TEXT
	)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopPlatformGIS ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, CharArr VARCHAR(4), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopPlatformGeoRefKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, GisName VARCHAR(4), GisVm INTEGER, Koord_x INTEGER, Koord_y INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopPlatformGeoref ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, id INTEGER, abst_von INTEGER, abst_bis INTEGER, seite CHAR, status CHAR, quell_name VARCHAR(4), strassenname VARCHAR(1024), GisVm INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS StopPlatformKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, plan VARCHAR(4), status CHAR, x INTEGER, y INTEGER, z INTEGER, disp_x INTEGER, disp_y INTEGER, text_x INTEGER, text_y INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_GeoRefKoord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, GisName VARCHAR(4), GisVm INTEGER, Koord_x INTEGER, Koord_y INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_attr ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, attr VARCHAR(3), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_bzn ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, bz VARCHAR(2), hstmit VARCHAR(30), hstohne VARCHAR(30), logo VARCHAR(40), hstgemeinde VARCHAR(30), hstgemeindemitOrt VARCHAR(30), pszhstmit VARCHAR(1024), pszhstohne VARCHAR(1024), pszhstgemeinde VARCHAR(1024), pszhstgemeindemitOrt VARCHAR(1024), input TEXT,
	PRIMARY KEY (_FK__AutoKey_,_FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_georef ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, id INTEGER, abst_von INTEGER, abst_bis INTEGER, seite CHAR, status CHAR, quell_name VARCHAR(4), strassenname VARCHAR(1024), GisVm INTEGER, input TEXT,
	PRIMARY KEY (_FK__AutoKey_,_FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_Metanumber ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, typ VARCHAR(3), Nummer INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_SpaTable ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, nKey INTEGER, strDescription VARCHAR(1024), strPlan VARCHAR(4), bGISbased BOOL, nMapFormat INTEGER, nPrintCount INTEGER, SPARectLeft INTEGER, SPARectTop INTEGER, SPARectRight INTEGER, SPARectBottom INTEGER, NeedlePosition INTEGER, NeedleCoordinateX INTEGER, NeedleCoordinateY INTEGER, NeedleObject INTEGER, NeedleObjectArea INTEGER, strNeedleObjectStoppingpoint VARCHAR(5), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_Unternehmer ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, Value VARCHAR(100), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_Verkaufsstellen ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, Value INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_alias_orte ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, szAliasgkz VARCHAR(8),
 szAliasortsname VARCHAR(1024), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_gebiet ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, gebiet INTEGER, gebietsgruppierung INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_koord ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, plan VARCHAR(4), status CHAR, x INTEGER, y INTEGER, z INTEGER, disp_x INTEGER, disp_y INTEGER, text_x INTEGER, text_y INTEGER, input TEXT,
	PRIMARY KEY (_FK__AutoKey_, _FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_mast ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, mast INTEGER, mastbez VARCHAR(17), mastkbez VARCHAR(5), eigenschaften TINYINT, art CHAR, niveau INTEGER, uebergangspunkt INTEGER, uebergangsnummer INTEGER, a_koord INTEGER, a_georef INTEGER, a_GisNetze INTEGER, m_lGISVerkehrsmittel INTEGER, m_nSortierschluessel INTEGER, m_pFremdschluessel VARCHAR(1024), SpaVlpUsage TINYINT, GisRegionLayer VARCHAR(99), GisRegionID INTEGER, ModInfo__Time INTEGER, ModInfo__Flag INTEGER, input TEXT,
	 PRIMARY KEY (_FK__AutoKey_,_FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_hst_steig ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, mast INTEGER, steig VARCHAR(5), ahftyp CHAR, ahfformat CHAR, nummer INTEGER, vwzsatz CHAR, vwzahf CHAR, vwzzob CHAR, vwzefa CHAR, a_koord INTEGER, a_georef INTEGER, langbezeichner VARCHAR(40), a_GisNetze INTEGER, m_lGISVerkehrsmittel INTEGER, buchtgroesse INTEGER, bucht_vor_mast INTEGER, kopfhaltestelle BOOL, ansagetextnummer INTEGER, kurzbezeichner VARCHAR(1024), hstSteigArt CHAR, StoppingPointDirection INTEGER, StoppingPointType1 INTEGER, StoppingPointType2 INTEGER, SteigAttribute INTEGER, kurzbezeichner2 VARCHAR(1024), StoppingPointExtType INTEGER, SpaVlpUsage TINYINT, DfiZieltextNummer INTEGER, Anzeigeprioritaet INTEGER, GisRegionLayer VARCHAR(99), GisRegionID INTEGER, OeffentSteigName VARCHAR(1024), ModInfo__Time INTEGER, ModInfo__Flag INTEGER, input TEXT,
 PRIMARY KEY (_FK__AutoKey_,_FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_lZhst ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, Zhst INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_lokalNete ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, lokalNetz VARCHAR(3), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_pq ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, pq_b VARCHAR(2), pq_z VARCHAR(2), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_tzonen ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, tzonen VARCHAR(4), input TEXT,
	PRIMARY KEY (_FK__AutoKey_,_FK_ARR_IDX, input))");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Stop_umgeb_plan ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, art INTEGER, dname VARCHAR(256), hst_x INTEGER, hst_y INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS Zus_Stop_Info ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, StoppingPointType1 INTEGER, strHstBerSteigNummer VARCHAR(1024), strZusInfoName VARCHAR(1024), strWert VARCHAR(1024), lVerwendungszweck INTEGER, input TEXT)");
}

sub creatediva_lnrlit {
	print "Creating DIVA route table\n";
	$divadbh->do("CREATE TABLE IF NOT EXISTS TabelleAhfPraesentationen (_FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, datv INTEGER, datb INTEGER, beschreibung VARCHAR(100), sv_gruppe VARCHAR(3), flags INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS TabelleBuchPraesentationen ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, datv INTEGER, datb INTEGER, beschreibung VARCHAR(100), sv_gruppe VARCHAR(3), flags INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS TabelleLinienPraesentationen ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, datv INTEGER, datb INTEGER, dateiname VARCHAR(1024), beschreibung VARCHAR(100), flags INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS TabelleLnrlit ( _AutoKey_ INTEGER, uvz VARCHAR(20), lierg VARCHAR(6), kbez VARCHAR(3), vm INTEGER, textpfp VARCHAR(5), tar_bes CHAR, aktiv CHAR, ahf CHAR, bedverb CHAR, db CHAR, puffer_zeit INTEGER, ia_tn VARCHAR(3), fv CHAR, linieGeoreferenzieren_yn CHAR, buch_yn CHAR, linieOhneEFAFahrten CHAR, ind_fahrrad_regel VARCHAR(2), cZugNrStattFSchl CHAR, bLVP BOOL, SpaVlpUsage TINYINT, TextBAlang VARCHAR(256), LNRLITAttribute INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS TabelleReferenzLinien ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, _AutoKey_ INTEGER, eLinRefTyp INTEGER, m_strRefLinie VARCHAR(6), input TEXT)");
}

sub creatediva_servicerestrictions {
	print "Creating DIVA service restrictions\n";
	$divadbh->do("CREATE TABLE IF NOT EXISTS SatzAnw ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, Str VARCHAR(60), input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS ServiceRestriction ( _AutoKey_ INTEGER, anfjahr INTEGER, code VARCHAR(5), text VARCHAR(80), def VARCHAR(60), dat_von VARCHAR(6), dat_bis VARCHAR(6), kenn CHAR, allg_dar VARCHAR(6), textbaustein VARCHAR(3), datvz CHAR, kurztext VARCHAR(5), vbt_von INTEGER, vbt_bis INTEGER, vt VARCHAR(215), optionen INTEGER, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS ServiceRestrictionFlags ( StartYear INTEGER, Heiligabend BOOL, Silvester BOOL, input TEXT)");
	$divadbh->do("CREATE TABLE IF NOT EXISTS ZusatzTxt ( _FK__AutoKey_ INTEGER, _FK_ARR_IDX INTEGER, Str VARCHAR(80), input TEXT)");
}

sub creatediva_transfers {
	print "Creating DIVA transfers\n";
	$divadbh->do("CREATE TABLE IF NOT EXISTS TransferProtection (project VARCHAR(3), hst_nr_an INTEGER, mast_nr_an INTEGER, linie_erg_an VARCHAR(6), richt_an CHAR, wttyp_an CHAR, zeit_von_an INTEGER, zeit_bis_an INTEGER, hst_nr_ab INTEGER, mast_nr_ab INTEGER, linie_erg_ab VARCHAR(6), richt_ab CHAR, wttyp_ab CHAR, zeit_von_ab INTEGER, zeit_bis_ab INTEGER, umst_zeit INTEGER, sitz_blb CHAR, bGDMExport BOOL, nMaxVerzoegerungManuell INTEGER, nMaxVerzoegerungAutom INTEGER, nAnschlussgewicht INTEGER, nFlags INTEGER, input TEXT);")
}

sub creatediva {
	creatediva_stops();
	creatediva_lnrlit();
	creatediva_servicerestrictions();
	creatediva_transfers();
	creatediva_agencies();
}

sub dropdiva_agencies {
	$divadbh->do("DROP TABLE IF EXISTS OpBranch");
}

sub dropdiva_stops{
	$divadbh->do("DROP TABLE IF EXISTS Stop");
	$divadbh->do("DROP TABLE IF EXISTS ArrayHaltezoneGeoref");
	$divadbh->do("DROP TABLE IF EXISTS HalteZoneGeoRefKoord");
	$divadbh->do("DROP TABLE IF EXISTS StopAreaGIS");
	$divadbh->do("DROP TABLE IF EXISTS StopAreaGeoRefKoord");
	$divadbh->do("DROP TABLE IF EXISTS StopAreaGeoref");
	$divadbh->do("DROP TABLE IF EXISTS StopAreaKoord");
	$divadbh->do("DROP TABLE IF EXISTS StopAreaMetanumber");
	$divadbh->do("DROP TABLE IF EXISTS StopKeyInterval");
	$divadbh->do("DROP TABLE IF EXISTS StopPlatformGIS");
	$divadbh->do("DROP TABLE IF EXISTS StopPlatformGeoRefKoord");
	$divadbh->do("DROP TABLE IF EXISTS StopPlatformGeoref");
	$divadbh->do("DROP TABLE IF EXISTS StopPlatformKoord");
	$divadbh->do("DROP TABLE IF EXISTS Stop_GeoRefKoord");
	$divadbh->do("DROP TABLE IF EXISTS Stop_attr");
	$divadbh->do("DROP TABLE IF EXISTS Stop_bzn");
	$divadbh->do("DROP TABLE IF EXISTS Stop_georef");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_Metanumber");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_SpaTable");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_Unternehmer");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_Verkaufsstellen");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_alias_orte");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_gebiet");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_koord");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_mast");
	$divadbh->do("DROP TABLE IF EXISTS Stop_hst_steig");
	$divadbh->do("DROP TABLE IF EXISTS Stop_lZhst");
	$divadbh->do("DROP TABLE IF EXISTS Stop_lokalNete");
	$divadbh->do("DROP TABLE IF EXISTS Stop_pq");
	$divadbh->do("DROP TABLE IF EXISTS Stop_tzonen");
	$divadbh->do("DROP TABLE IF EXISTS Stop_umgeb_plan");
	$divadbh->do("DROP TABLE IF EXISTS Zus_Stop_Info");
}

sub dropdiva_lnrlit {
	$divadbh->do("DROP TABLE IF EXISTS TabelleAhfPraesentationen");
	$divadbh->do("DROP TABLE IF EXISTS TabelleBuchPraesentationen");
	$divadbh->do("DROP TABLE IF EXISTS TabelleLinienPraesentationen");
	$divadbh->do("DROP TABLE IF EXISTS TabelleLnrlit");
	$divadbh->do("DROP TABLE IF EXISTS TabelleReferenzLinien");
}

sub dropdiva_servicerestrictions {
	$divadbh->do("DROP TABLE IF EXISTS SatzAnw");
	$divadbh->do("DROP TABLE IF EXISTS ServiceRestriction");
	$divadbh->do("DROP TABLE IF EXISTS ServiceRestrictionFlags");
	$divadbh->do("DROP TABLE IF EXISTS ZusatzTxt");
}

sub dropdiva_transfers {
	$divadbh->do("DROP TABLE IF EXISTS TransferProtection");
}

sub dropdiva {
	dropdiva_stops();
	dropdiva_lnrlit();
	dropdiva_servicerestrictions();
	dropdiva_transfers();
	dropdiva_agencies();
}

sub cleardiva_servicerestrictions() {
	dropdiva_servicerestrictions();
	creatediva_servicerestrictions();
}

sub cleardiva_lnrlit {
	dropdiva_lnrlit();
	creatediva_lnrlit();
}

sub cleardiva_stops {
	dropdiva_stops();
	creatediva_stops();
}

sub cleardiva {
	dropdiva();
	creatediva();
}

sub disconnect {
	$dbh->disconnect();
	print "GTFS-Database closed.\n";

	$divadbh->disconnect();
	print "Diva-Database closed.\n";

	print "Everything done.\n";
	print "Bye!\n";
}
