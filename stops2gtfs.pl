#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use Text::ParseWords;

# take care of windows newlines
#$/ = "\n";


my $line;

# --------------------
# CONNECT TO DATABASE
# --------------------

my $driver   = "SQLite"; 
my $divadatabase = "divadata.db";
my $divadsn = "DBI:$driver:dbname=$divadatabase";
my $userid = "";
my $password = "";
my $divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 }) 
                      or die $DBI::errstr;

# sacrificing security for speed
$divadbh->{AutoCommit} = 0;
$divadbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

	print "Opened diva-database successfully\n";

my $database = "diva2gtfs.db";
my $dsn = "DBI:$driver:dbname=$database";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
                      or die $DBI::errstr;

# sacrificing security for speed
$dbh->{AutoCommit} = 0;
$dbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

	print "Opened gtfs-database successfully\n";

# --------------------------
# END OF DATABASE GEDOENS
# --------------------------


# -------------------------------------------
# OUTPUT TEH STUFF
#--------------------------------------------

my %CS2CS_params = (
		GIP1 => '+init=epsg:31259 +to +init:epsg:4326', #should be the same as STVH but offset is 6000000 and not 1000000
		MVTT => '+init=epsg:31466 +to +init=epsg:4326',
		NAV2 => '+init=epsg:31466 +to +init=epsg:4326',
		NAV3 => '+init=epsg:31467 +to +init=epsg:4326',
		NAV4 => '+init=epsg:31468 +to +init=epsg:4326',
		NAV5 => '+init=epsg:31469 +to +init=epsg:4326',
		NBWT => '+init=epsg:31467 +to +init=epsg:4326',
		STVH => '+init=epsg:31259 +to +init:epsg:4326',
		VVTT => '+init=epsg:31257 +to +init=epsg:4326'
		);
	my %offsets = (
                GIP1 => 6000000,
                MVTT => 6160100,
                NAV2 => 6160100,
                NAV3 => 6160100,
                NAV4 => 6160100,
                NAV5 => 6160100,
                NBWT => 6160100,
                STVH => 1000000
                VVTT => 1000000
                );

	my $stop_id ="";
	my $stop_name = "";
	my $stop_lat = "";
	my $stop_lon = "";
	my $zone_id = "";

	print "Parent stations ";

	my $sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, group_concat(tz.tzonen,"") AS zone_id, HK.x AS stop_lat, HK.y AS stop_lon, HK.plan AS plan
FROM Stop AS S LEFT OUTER JOIN Stop_hst_koord as HK ON S._AutoKey_=HK._FK__AutoKey_ AND HK.plan="NBWT" AND S.input=HK.input LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_=tz._FK__AutoKey_ AND S.input=tz.input 
WHERE S._AutoKey_ IN (SELECT SHS._FK__AutoKey_ FROM Stop_hst_steig AS SHS WHERE S.input = SHS.input)
GROUP BY stop_id, HK.x');
	$sth->execute();

	print "queried...";

	while (my $row = $sth->fetchrow_hashref()) {

			$stop_id = $row->{stop_id};
			if (defined $row->{stop_name}) { $stop_name = $row->{stop_name}; }
			if (defined $row->{zone_id}) { $zone_id = $row->{zone_id}; }

			if (defined $row->{stop_lat} and defined $row->{stop_lon}) { 
				$stop_lat = $row->{stop_lat}; 
				$stop_lon = -1 * ($row->{stop_lon} - $offsets{$row->{plan}});

				my @coords1=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $CS2CS_params{$row->{plan}}`);

				$stop_lon = $coords1[0];
				$stop_lat = $coords1[1];

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($stop_id,undef,$stop_name,$stop_lat,$stop_lon,$zone_id,"1",undef,undef);

			} else { 
				$stop_name = $stop_name . "KOORDFIX!"; 

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($stop_id,undef,$stop_name,undef,undef,$zone_id,"1",undef,undef);
			}

	}

	print "...and written to GTFS database\n";


	print "Child stations";

	$sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, group_concat(tz.tzonen,"") as zone_id, SHS.steig AS steig, SPK.x AS stop_lat, SPK.y AS stop_lon, SPK.plan AS plan
FROM Stop AS S LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_=tz._FK__AutoKey_ AND S.input=tz.input LEFT OUTER JOIN Stop_hst_steig AS SHS on S._AutoKey_ = SHS._FK__AutoKey_ AND S.input=SHS.input LEFT OUTER JOIN StopPlatformKoord AS SPK ON SHS._AutoKey_ = SPK._FK__AutoKey_ AND SHS.input=SPK.input AND SPK.plan = "NBWT" 
WHERE SHS.steig NOT LIKE "Eing%"
GROUP BY stop_id, steig, SPK.x');
	$sth->execute();

	print(" queried...");

	while (my $row = $sth->fetchrow_hashref()) {

			$stop_id = $row->{stop_id} . $row->{steig};
			if (defined $row->{stop_name}) { $stop_name = $row->{stop_name}; }
			if (defined $row->{zone_id}) { $zone_id = $row->{zone_id}; }

			if (defined $row->{stop_lat} and defined $row->{stop_lon}) { 
				$stop_lat = $row->{stop_lat};
				$stop_lon = -1 * ($row->{stop_lon} - $offsets{$row->{plan}});

				my @coords2=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $CS2CS_params{$row->{plan}}`);

				$stop_lon = $coords2[0];
				$stop_lat = $coords2[1];

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($stop_id,undef,$stop_name,$stop_lat,$stop_lon,$zone_id,"0",$row->{stop_id},undef);

			} else { 
				$stop_name = $stop_name . "KOORDFIX!"; 

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($stop_id,undef,$stop_name,undef,undef,$zone_id,"0",$row->{stop_id},undef);

			}

	}

	print("...and written to GTFS database\n");

	print "Stations without child stations";

$sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, group_concat(tz.tzonen,"") as zone_id, HK.x AS stop_lat, HK.y AS stop_lon, HK.plan AS plan
FROM Stop AS S LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_=tz._FK__AutoKey_ AND S.input=tz.input LEFT OUTER JOIN Stop_hst_koord as HK ON S._AutoKey_=HK._FK__AutoKey_ AND HK.plan="NBWT" AND S.input=HK.input
WHERE S._AutoKey_ NOT IN (SELECT SHS._FK__AutoKey_ FROM Stop_hst_steig AS SHS WHERE S.input = SHS.input)
GROUP BY stop_id, HK.x');
	$sth->execute();

	print " queried...";

	while (my $row = $sth->fetchrow_hashref()) {

			if (defined $row->{stop_lat} and defined $row->{stop_lon}) { 
				$stop_lat = $row->{stop_lat}; 
				$stop_lon = -1 * ($row->{stop_lon} - $offsets{$row->{plan}});

				my @coords1=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $CS2CS_params{$row->{plan}}`);

				$stop_lon = $coords1[0];
				$stop_lat = $coords1[1];

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($row->{stop_id},undef,$row->{stop_name},$stop_lat,$stop_lon,$row->{zone_id},"0",undef,undef);

			} else { 
				$stop_name = $row->{stop_id} . "KOORDFIX!"; 

				my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
				$insertsth->execute($row->{stop_id},undef,$stop_name,undef,undef,$row->{zone_id},"0",undef,undef);

			}
	}

	print "...and written to GTFS database\n";

#	print "Querying for stations without coordinates...";

#	$sth = $divadbh->prepare('SELECT hstnr AS stop_id, hstname AS stop_name, Stop.input, Stop_hst_koord.plan, Stop_hst_koord.x FROM Stop LEFT OUTER JOIN Stop_hst_koord on Stop._AutoKey_ = Stop_hst_koord._FK__AutoKey_ and Stop.input = Stop_hst_koord.input WHERE Stop_hst_koord.x IS NULL');
#	$sth->execute();

#	while (my $row = $sth->fetchrow_hashref()) {

#			$stop_id = $row->{stop_id};
#			if (defined $row->{stop_name}) { $stop_name = $row->{stop_name} . "KOORDFIX"; } else {$stop_name = "KOORDFIX!"; }

#			my $insertsth = $dbh->prepare('INSERT INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
#			$insertsth->execute($stop_id,undef,$stop_name,undef,undef,undef,"0",undef,undef);
#	}


	$dbh->commit;
	$divadbh->commit;





# ---------------------------------
# CLEANING UP!
# ---------------------------------

$divadbh->disconnect();
print "Diva-Database closed. ";

$dbh->disconnect();
print "GTFS-Database closed. ";

print "Everything done. Bye!\n";
