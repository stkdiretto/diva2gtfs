#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use DBI;
use File::Basename;
use File::Path qw(make_path);
use Text::ParseWords;
use open ':encoding(windows-1252)';

# take care of windows newlines
#$/ = "\n";

my $line;

# --------------------
# CONNECT TO DATABASE
# --------------------

my $db_folder = "build/data";
make_path($db_folder);

my $driver   = "SQLite";
my $database = "$db_folder/divadata.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $divadbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
                      or die $DBI::errstr;

# sacrificing security for speed
$divadbh->{AutoCommit} = 0;
$divadbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

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
	my $arg = shift;
	my $currentTable;
	my $filename = basename($arg);
	my $opbranch;

	my $exists_table = 0;
	my $exist_check = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";

	open (FILE, "<", "$arg") or die("Could not open inputfile: $!");
	if ($filename =~ /.*\.(.*)/) {
		$opbranch = $1;
	} else {
		$opbranch = $filename;
	}
	print ("Working on project \"$opbranch\"\n");

	foreach $line (<FILE>) {
		chomp $line;

		if ($line =~ s/tbl;//) {
			$currentTable = $line;
			$exists_table = 0;

			my $sth = $divadbh->prepare($exist_check);
			$sth->execute($currentTable);

			while (my $row = $sth->fetchrow_hashref()) {
				print "Filling table: $currentTable\n";
				$exists_table = 1;
			}

			if (! $exists_table) {
				print "Skipping table: $currentTable\n";
			}
		} elsif ($exists_table and $line =~ s/rec;//) {
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

	$divadbh->commit();

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
print "Everything done.\n";
print "Bye!\n";
