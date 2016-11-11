#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use File::Path qw(make_path);
use Getopt::Long;
use Text::ParseWords;

# take care of windows newlines
#$/ = "\n";

# Hard-coded values
my %defaults = (
	agency_fare_url => undef,
	agency_lang => "de",
	agency_phone => undef,
	agency_timezone => "Europe/Berlin",
	agency_url => "http://www.example.com/"
);

GetOptions ("set=s" => \%defaults) or die("Error in command line arguments\n");

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

my $agency_id;
my $agency_name;
my $agency_url;
my $agency_timezone;
my $agency_lang;
my $agency_phone;
my $agency_fare_url;

print "Agencies ";

my $sth = $divadbh->prepare('SELECT bzw AS agency_id, bzwtext AS agency_name FROM OpBranch');
$sth->execute();

print "queried...";

while (my $row = $sth->fetchrow_hashref()) {
	$agency_id = $row->{agency_id};
	$agency_name = $row->{agency_name};
	$agency_url = $defaults{agency_url};
	$agency_timezone = $defaults{agency_timezone};
	$agency_lang = $defaults{agency_lang};
	$agency_phone = $defaults{agency_phone};
	$agency_fare_url = $defaults{agency_fare_url};

	my $sthDelete = $dbh->prepare('DELETE FROM agency WHERE agency_id = ?');
	$sthDelete->execute($agency_id);

	my $sthInsert = $dbh->prepare('INSERT INTO agency VALUES (?, ?, ?, NULLIF(?, \'\'), NULLIF(?, \'\'), NULLIF(?, \'\'), NULLIF(?, \'\'))');
	$sthInsert->execute($agency_id, $agency_name, $agency_url, $agency_timezone, $agency_lang, $agency_phone, $agency_fare_url);
}

print " and written to GTFS database\n";

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
