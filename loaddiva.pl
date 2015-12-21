#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use Text::ParseWords;

 use open ':encoding(windows-1252)';

# take care of windows newlines
#$/ = "\n";


my $line;

# --------------------
# CONNECT TO DATABASE
# --------------------

my $driver   = "SQLite"; 
my $database = "divadata.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $divadbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
                      or die $DBI::errstr;

# sacrificing security for speed
$divadbh->{AutoCommit} = 0;
$divadbh->do( "PRAGMA synchronous=OFF" );

	print "Opened database successfully\n";


# --------------------------
# END OF DATABASE GEDOENS
# --------------------------

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

	my $currentTable;

	my $arg = shift;
	open (FILE, "<", "$arg") or die("Could not open inputfile: $!");
	$arg =~ /.*\.(.*)/;
	my $opbranch = $1;
	print ("Working on project $opbranch\n");

	foreach $line (<FILE>) {
		chomp $line;

		if ($line =~ s/tbl;//) {	
			$currentTable = $line;
			print "Filling table: $currentTable\n";
		}	
		elsif ($line =~ s/rec;//) {

			my @currentRecord = quotewords(";", 0, $line);
			my $insertion = "INSERT INTO $currentTable VALUES (";
				for my $i (0 .. $#currentRecord) {
					$insertion = $insertion . "?, ";
				}
			$insertion = $insertion . "? )";

			push (@currentRecord, $opbranch);

			my $sth = $divadbh->prepare($insertion);
			$sth->execute(@currentRecord);

		}

	}

	$divadbh->commit;

	close FILE;
}

# ---------------------------------
# END OF FILE PROCESSING SUBROUTINE
# ---------------------------------


# ---------------------------------
# CLEANING UP!
# ---------------------------------

$divadbh->disconnect();
print "Database closed. ";

print "Everything done. Bye!\n";
