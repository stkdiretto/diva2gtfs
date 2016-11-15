#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use File::Path qw(make_path);
use Text::ParseWords;

# take care of windows newlines
#$/ = "\n";


my $line;

# --------------------
# CONNECT TO DATABASE
# --------------------

my $db_folder = "build/data";
make_path($db_folder);

my $driver = "SQLite";
my $divadatabase = "$db_folder/divadata.db";
my $divadsn = "DBI:$driver:dbname=$divadatabase";
my $userid = "";
my $password = "";
my $divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 })
                      or die $DBI::errstr;

# sacrificing security for speed
$divadbh->{AutoCommit} = 0;
$divadbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

print "Opened diva-database successfully\n";

my $database = "$db_folder/diva2gtfs.db";
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
# MAIN METHOD
#--------------------------------------------

# TODO: Add all CRS used by DIVA here
my %crs = (
# GIP1 causes issues when calling cs2cs (exception cause is "projection not named")
#	GIP1 => {
#		cs2cs_params => '+init=epsg:31259 +to +init:epsg:4326',
##		offset => 6000000
#		offset => 987402
#	},
	MVTT => {
		cs2cs_params => '+init=epsg:31466 +to +init=epsg:4326',
		offset => 6160100
	},
	NAV2 => {
		cs2cs_params => '+init=epsg:31466 +to +init=epsg:4326',
		offset => 6160100
	},
	NAV3 => {
		cs2cs_params => '+init=epsg:31467 +to +init=epsg:4326',
		offset => 6160100
	},
	NAV4 => {
		cs2cs_params => '+init=epsg:31468 +to +init=epsg:4326',
		offset => 6160100
	},
	NAV5 => {
		cs2cs_params => '+init=epsg:31469 +to +init=epsg:4326',
		offset => 6160100
	},
	NBWT => {
		cs2cs_params => '+init=epsg:31467 +to +init=epsg:4326',
		offset => 6160100
	},
	STVH => {
		cs2cs_params => '+init=epsg:31259 +to +init:epsg:4326',
		offset => 1000000
	},
	VVTT => {
		cs2cs_params => '+init=epsg:31257 +to +init=epsg:4326',
		offset => 1000000
	}
);
my @supported_crs = keys %crs;
my $stop_id = "";
my $stop_name = "";
my $stop_lat = "";
my $stop_lon = "";
my $zone_id = "";

# Parent station handling

print "Parent stations ";

my $insertsth = $dbh->prepare('INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
my $sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, bzn.hstohne AS stop_name_bzn, group_concat(tz.tzonen, "") AS zone_id, HK.x AS stop_lat, HK.y AS stop_lon, HK.plan AS plan FROM Stop AS S
LEFT OUTER JOIN Stop_bzn AS bzn ON S._AutoKey_ = bzn._FK__AutoKey_
LEFT OUTER JOIN Stop_hst_koord as HK ON S._AutoKey_ = HK._FK__AutoKey_
LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_ = tz._FK__AutoKey_
WHERE S._AutoKey_ IN (SELECT SHS._FK__AutoKey_ FROM Stop_hst_steig AS SHS) AND HK.plan IN (\'' . join('\', \'',  @supported_crs) . '\')
GROUP BY stop_id');
$sth->execute();
print "queried...";

while (my $row = $sth->fetchrow_hashref()) {
	$stop_id = $row->{stop_id};
	if (defined $row->{stop_name} and $row->{stop_name} ne "") { $stop_name = $row->{stop_name}; }
	elsif (defined $row->{stop_name_bzn}) { $stop_name = $row->{stop_name_bzn}; }
	if (defined $row->{zone_id}) { $zone_id = $row->{zone_id}; }
	if (defined $row->{plan} and exists $crs{$row->{plan}} and defined $row->{stop_lat} and defined $row->{stop_lon}) {
		$stop_lat = $row->{stop_lat};
		$stop_lon = -1 * ($row->{stop_lon} - $crs{$row->{plan}}{offset});

		my @coords1=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $crs{$row->{plan}}{cs2cs_params}`);

		$stop_lon = $coords1[0];
		$stop_lat = $coords1[1];
	} else {
		$stop_name = $stop_name . "_KOORDFIX!";
		$stop_lon = undef;
		$stop_lat = undef;
	}

	$insertsth->execute($stop_id, undef, $stop_name, $stop_lat, $stop_lon, $zone_id, "1", undef, undef);
}
$dbh->commit();
print " and written to GTFS database\n";

# Child station handling

print "Child stations";

$sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, bzn.hstohne AS stop_name_bzn, group_concat(tz.tzonen, "") as zone_id, SHS.steig AS steig, SPK.x AS stop_lat, SPK.y AS stop_lon, SPK.plan AS plan FROM Stop AS S
LEFT OUTER JOIN Stop_bzn AS bzn ON S._AutoKey_ = bzn._FK__AutoKey_
LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_ = tz._FK__AutoKey_
LEFT OUTER JOIN Stop_hst_steig AS SHS on S._AutoKey_ = SHS._FK__AutoKey_
LEFT OUTER JOIN StopPlatformKoord AS SPK ON SHS._AutoKey_ = SPK._FK__AutoKey_
WHERE SHS.steig NOT LIKE "Eing%" AND SPK.plan IN (\'' . join('\', \'',  @supported_crs) . '\')
GROUP BY stop_id, steig');
$sth->execute();
print(" queried...");

while (my $row = $sth->fetchrow_hashref()) {
	$stop_id = $row->{stop_id} . $row->{steig};
	if (defined $row->{stop_name} and $row->{stop_name} ne "") { $stop_name = $row->{stop_name}; }
	elsif (defined $row->{stop_name_bzn}) { $stop_name = $row->{stop_name_bzn}; }
	if (defined $row->{zone_id}) { $zone_id = $row->{zone_id}; }
	if (defined $row->{plan} and exists $crs{$row->{plan}} and defined $row->{stop_lat} and defined $row->{stop_lon}) {
		$stop_lat = $row->{stop_lat};
		$stop_lon = -1 * ($row->{stop_lon} - $crs{$row->{plan}}{offset});

		my @coords2=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $crs{$row->{plan}}{cs2cs_params}`);

		$stop_lon = $coords2[0];
		$stop_lat = $coords2[1];
	} else {
		$stop_name = $stop_name . "_KOORDFIX!";
		$stop_lon = undef;
		$stop_lat = undef;
	}

	$insertsth->execute($stop_id, undef, $stop_name, $stop_lat, $stop_lon, $zone_id, "0", $row->{stop_id}, undef);
}
$dbh->commit();
print(" and written to GTFS database\n");

# Station handling without child stations

print "Stations without child stations";

$sth = $divadbh->prepare('SELECT S.hstnr AS stop_id, S.hstname AS stop_name, bzn.hstohne AS stop_name_bzn, group_concat(tz.tzonen, "") as zone_id, HK.x AS stop_lat, HK.y AS stop_lon, HK.plan AS plan FROM Stop AS S
LEFT OUTER JOIN Stop_bzn AS bzn ON S._AutoKey_ = bzn._FK__AutoKey_
LEFT OUTER JOIN Stop_hst_koord as HK ON S._AutoKey_=HK._FK__AutoKey_
LEFT OUTER JOIN Stop_tzonen as tz ON S._AutoKey_=tz._FK__AutoKey_
WHERE S._AutoKey_ NOT IN (SELECT SHS._FK__AutoKey_ FROM Stop_hst_steig AS SHS) AND HK.plan IN (\'' . join('\', \'',  @supported_crs) . '\')
GROUP BY stop_id');
$sth->execute();
print " queried...";

while (my $row = $sth->fetchrow_hashref()) {
	$stop_id = $row->{stop_id};
	if (defined $row->{stop_name} and $row->{stop_name} ne "") { $stop_name = $row->{stop_name}; }
	elsif (defined $row->{stop_name_bzn}) { $stop_name = $row->{stop_name_bzn}; }
	if (defined $row->{zone_id}) { $zone_id = $row->{zone_id}; }
	if (defined $row->{plan} and exists $crs{$row->{plan}} and defined $row->{stop_lat} and defined $row->{stop_lon}) {
		$stop_lat = $row->{stop_lat};
		$stop_lon = -1 * ($row->{stop_lon} - $crs{$row->{plan}}{offset});

		my @coords1=split(/\s+/, `echo $stop_lat $stop_lon | cs2cs -f "%.8f" $crs{$row->{plan}}{cs2cs_params}`);

		$stop_lon = $coords1[0];
		$stop_lat = $coords1[1];
	} else {
		$stop_name = $row->{stop_id} . "_KOORDFIX!";
		$stop_lon = undef;
		$stop_lat = undef;
	}

	$insertsth->execute($stop_id, undef, $stop_name, $stop_lat, $stop_lon, $zone_id, "0", undef, undef);
}
$dbh->commit();
print " and written to GTFS database\n";

# Station handling without coordinates

#print "Querying for stations without coordinates...";

#$sth = $divadbh->prepare('SELECT hstnr AS stop_id, hstname AS stop_name, Stop.input, Stop_hst_koord.plan, Stop_hst_koord.x FROM Stop LEFT OUTER JOIN Stop_hst_koord on Stop._AutoKey_ = Stop_hst_koord._FK__AutoKey_ and Stop.input = Stop_hst_koord.input WHERE Stop_hst_koord.x IS NULL');
#$sth->execute();

#while (my $row = $sth->fetchrow_hashref()) {
#	$stop_id = $row->{stop_id};
#	if (defined $row->{stop_name}) {
#		$stop_name = $row->{stop_name} . "_KOORDFIX";
#	} else {
#		$stop_name = "_KOORDFIX!";
#	}

#	my $insertsth = $dbh->prepare('INSERT INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
#	$insertsth->execute($stop_id, undef, $stop_name, undef, undef, undef, "0", undef, undef);
#}

$dbh->commit();
$divadbh->commit();

# ---------------------------------
# CLEANING UP!
# ---------------------------------

$divadbh->disconnect();
print "Diva-Database closed.\n";

$dbh->disconnect();
print "GTFS-Database closed.\n";
print "Everything done.\n";
print "Bye!\n";
