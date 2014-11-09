#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use Text::ParseWords;

 use open ':encoding(windows-1252)';

# take care of windows newlines
#$/ = "\n";

my $agency_timezone = "Europe/Berlin";
my $agency_lang = "de";
my $agency_url = "http://www.ding.eu";

my $line;

# DEFINE I/O FILES
my $log;


# --------------------
# CONNECT TO DATABASE
# --------------------

my $driver   = "SQLite"; 
my $database = "diva2gtfs.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
                      or die $DBI::errstr;

# sacrificing security for speed
$dbh->{AutoCommit} = 0;
$dbh->do( "PRAGMA synchronous=OFF" );

	print "Opened database successfully\n";

# --------------------------
# END OF DATABASE GEDOENS
# --------------------------

	open ($log, ">","log_agency.txt") or die "Something terrible happened while opening my log file: $!";
#	open ($log, ">","/dev/null") or die "Something terrible happened while opening my log file: $!";

# ------------------------------------------
# ITERATE OVER ALL FILES PASSED AS ARGUMENTS

foreach my $file (@ARGV) {
	process($file);
}

# -------------------------------------------


# ------------------------------------------
# PROCESS EACH FILE
# -----------------------------------------

sub process {
	open ($log, ">>","log_agency.txt") or die "Something even more terrible happened while opening my log file: $!";
	my $arg = shift;
	open (FILE, "<", "$arg") or die("Could not open inputfile: $!");
	print $log "Now working on $arg\n";

	foreach $line (<FILE>) {
		chomp $line;
		if ($line =~ /rec;.*?;\"(?<agency_id>.*?)\";\"(?<agency_name>.*?)\";/) {
			print "parsing: $+{agency_id},$+{agency_name}\n";
			my $sth = $dbh->prepare('INSERT INTO agency values (?, ?, ?, ?, ?, ?, ?)');
			$sth->execute($+{agency_id},$+{agency_name},$agency_url,$agency_timezone,$agency_lang, undef, undef);

		}

	}

	$dbh->commit;

	close FILE;
}

# ---------------------------------
# END OF FILE PROCESSING SUBROUTINE
# ---------------------------------


# ---------------------------------
# CLEANING UP!
# ---------------------------------

close $log;
$dbh->disconnect();
print "Database closed. ";

print "Everything done. Bye!\n";
