#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use Switch;
use DBI;
use File::Path qw(make_path);
use Text::ParseWords;
use Getopt::Long;
use Digest::MD5 qw(md5);
use open ':encoding(cp850)';

# take care of windows newlines
$/ = "\r\n";

# DEFINE I/O FILES
my $log;
my $log_folder = "build/log";
make_path($log_folder);

open ($log, ">","$log_folder/diva2gtfs.log") or die "Something terrible happened while opening my log file: $!";
#open ($log, ">","/dev/null") or die "Something terrible happened while opening my log file: $!";

#unbuffering log (useful for debugging)
#$| = 1;

my $divadbh;
my $dbh;
my $basename;
my $tripname;
my $operator;
my $textpfp;
my $textbalang;
my $basepath = '';

dbconnect();

GetOptions	( "path=s" => \$basepath) or die("Error in command line arguments\n");

my $sth = $divadbh->prepare('SELECT uvz,lierg,kbez,textpfp,TextBAlang FROM TabelleLnrlit');
$sth->execute();

while (my $row = $sth->fetchrow_hashref()) {

	# naming confusion galore.
	# tripname: Everything, e.g. 11310a or 11310_
	# basename: Just the operator and route, e.g. 11310
	$tripname = $row->{lierg};
	$tripname =~ /(?<basename>(?<operator>.{2}).{2}[^_]?).+/;
	$operator = $+{operator};
	$basename = $+{basename};

	# trim trailing spaces
	$tripname =~ s/\s/_/;
	$basename =~ s/\s+$//;

	$textpfp = $row->{textpfp};
	$textpfp =~ s/\s+$//;

	$textbalang = $row->{TextBAlang};
	$textbalang =~ s/\s+$//;

	# build the path to each file. Pattern is uvz/lierg.kbez with trimmed spaces
	my $path = $basepath . $row->{uvz} . "/" . $tripname . "." . $row->{kbez};
	print "Route: $basename, tripname $tripname, Path: $path\n";

	if($textpfp eq $textbalang) {
		undef $textbalang;
	}

	my $newroute = $dbh->prepare('INSERT OR REPLACE INTO routes (route_id, agency_id, route_short_name, route_long_name) VALUES (?, ?, ?, ?)');
	$newroute->execute($operator."-".$textpfp,$operator,$textpfp,$textbalang);

	$dbh->commit;

	my %job = (
		'path' => $path,
		'tripname' => $tripname,
		'operator' => $operator ,
		'textpfp' => $textpfp,
		'textbalang' => $textbalang,
		'route' => $operator . "-" . $textpfp
	);

	process(%job);
}


# ------------------------------------------
# ITERATE OVER ALL FILES PASSED AS ARGUMENTS

#foreach my $file (@ARGV) {
#	process($file);
#}

# -------------------------------------------


# ----------------------------------------
# SUBROUTINE TO EXPAND TIMING PATTERNS

sub expandtimes {
	my @timearray;
	# push minutes, -, |, $ to array
	foreach (@_) {

		# expand * sequences. First capture is the amount of occurrences, second the content.
		if ($_ =~ /\*([0-9]{2})(\-|\||\$|[0-9]{2})/) {
			for (my $i = 0; $i < $1; $i++) {
				push @timearray, $2;
			}
		}
		# deal with single occurrences
		else {
			push @timearray, $_;
		}
	}
	# done pushing the timing pattern to the array
	return @timearray;
}

# -----------------------------------------


# ------------------------------------------
# PROCESS FILE
# -----------------------------------------

sub process {
	my %process = @_;
	my $file = $process{path};

	print $log "Now working on $file\n";
	if (open FILE, "<", "$file") {
		my $current_line;
		my $line;
		my @stops;
		my %platforms;
		my $route_type;
		my $direction;
		my $route_long_name;
		my %FT;

		foreach $current_line (<FILE>) {
			chomp $current_line;
			# $line is the current line that is modified by applying patterns (unmodified line is still available in $current_line)
			$line = $current_line;

			# ----------------------------------------------------------
			# HEADERS FOR EACH DIRECTION TO BE TAKEN CARE OF
			# These are: Stop Patterns, Stop Platforms, Timing Patterns
			# ----------------------------------------------------------

			# Recognize Fahrwege (Stop patterns)
			if ($line =~ s/FW(?<cnt>[0-9]*)[H,R]//) {
				my $stopCnt = $+{cnt} + 0; # + 0 converts to number
				if ($stopCnt <= 0) {
					print " Skipping stop pattern (invalid stop count): $current_line\n";
					next;
				}

				my $stopLen = (length $line) / $stopCnt;
				@stops = ();

				push @stops, substr $line, 0, $stopLen, '' while $line;
				print $log " FW recognized: ";
				print $log "$_ " for @stops;
				print $log "\n";
			}
			# Recognize Stop Platforms
			elsif ($line =~ s/ST[H,R](?<cnt>[0-9]{3})//) {
				# TODO: will do this later :'(
				my $stopCnt = $+{cnt} + 0; # + 0 converts to number
				if ($stopCnt <= 0) {
					print " Skipping stop platform (invalid stop count): $current_line\n";
					next;
				}

				print $log " Platform line recognized: ";
				while ($line =~ /([0-9]{3})(.{5})/g) {
					my $stid = $1;
					my $plat = $2;

					$plat =~ s/\s+$//; # trim trailing spaces
#					$plat =~ s/\+.*//;
					print $log "$plat ";

					if ($plat ne '-' and $plat ne '0') {
						$stops[$stid] = $stops[$stid] . $plat;
					}
				}

				print $log "\n";
			}
			# Recognize Timing Patterns
			elsif ($line =~ /FT(?<ftid>[HR][0-9]{5}).{2}(?<pattern>.*)[ ,N].*/) {
				# create identifier for current pattern
				my $ftid = $+{ftid};
				print $log " Timing Pattern: $ftid ; ";
				my $pattern = $+{pattern};

				print $log "$pattern \n   ";
				# match: *00-, *0000, *00|, *00$ or 00 or - or | or $
				# write everything in temporary tmparray for later expansion of * sequences
	 			my @tmparray = $pattern =~ /(\*[0-9]{2}\-|\*[0-9]{4}|\*[0-9]{2}\||\*[0-9]{2}\$|[0-9]{2}|\-|\||[\$])/g;
				# expand * sequences
				@{ $FT{$ftid} } = expandtimes(@tmparray);

				# output written timing pattern to debug log for debugging purposes
				foreach (@{ $FT{$ftid} }) {
					print $log "$_ ";
					if ($_ eq '-' || $_ eq '|' || $_ eq '$') {
						print $log " ";
					}
				}
				print $log "\n";
			}
			# Done with timing pattern

			# -------------------------------
			# HEADERS BEEN TAKEN CARE OF HERE
			# -------------------------------


			# -----------------------
			# HERE COME ACTUAL TRIPS
			# -----------------------

			elsif ($line =~ s/^FA//) {
				# example:
				# H005580000000000548100002Z      57635  N  RB       0000000000907017700000000000000264  0             000
				# R01042000000017104210001110         0  N           000000000       000005011900003170000000264  0             000
				if ($line =~ /(?<tripid>(?<direction>[H,R])(?<serviceid>.{1})(?<tripkey>[0-9]{4}))0{5}(.{4})?(?<starttime>[0-9]{4}).(?<timingpattern>[0-9]{5})\s?(?<vehicletype>[A-Z0-9]{1,2})?\s*(?<servicerestriction>[A-Za-z][A-Za-z0-9]{1,2})?\s+((?<trainid>[A-Z]?[1-9][0-9]{0,5})\s*[A-Z]?\s*(?<traintype>[A-Z]+))?.*[0-9]{3}(?<notice>\".*\")*/) {
					my $tripid;
					my $trip_short_name;
					if (defined $+{servicerestriction}) {
						$tripid = $+{direction}.$+{serviceid}.$+{servicerestriction}.$+{tripkey};
					} else {
						$tripid = $+{tripid};
					}
					print $log " Tripid $process{tripname}.$tripid at $+{starttime}, Pattern $+{timingpattern}";
					if (defined $+{vehicletype}) {	print $log "\t$+{vehicletype}"; } else {print $log "\t";}
					if (defined $+{servicerestriction}) { print $log "\t$+{servicerestriction}"; } else { print $log "\t"; }
					if (defined $+{traintype}) { print $log "\t$+{traintype}"; } else { print $log "\t"; }
					if (defined $+{trainid}) { print $log " $+{trainid}"; } else { print $log "\t" };
					if (defined $+{notice}) { print $log "\t$+{notice}"; }
					print $log "\n";

					my $timingpattern = $+{direction} . $+{timingpattern};

					# Taking care of directions
					if ($+{direction} eq "H") {
						$direction = 0;
					} else {
						$direction = 1;
					}

					# if train, use train number as trip id
					if (defined $+{trainid}) {
						$trip_short_name = $+{traintype} . $+{trainid};
					}

					# take care of service restriction. If a restriction is defined,
					# the previous service id is replaced
					my $service_id = $+{serviceid};
					if (defined $+{servicerestriction}) {
						$service_id = $+{servicerestriction};
					}

					my $sth = $dbh->prepare('INSERT OR REPLACE INTO trips (route_id, service_id, trip_id, trip_short_name, direction_id, shape_id) values (?, ?, ?, ?, ?, ?)');
					$sth->execute($process{route},$service_id,$process{tripname}.$tripid,$trip_short_name,$direction,$process{route}.$timingpattern);

					# Analyze timing pattern for trip and save stop times

					my $hours = substr($+{starttime},0,2);
					my $minutes = substr($+{starttime},2,2);
					my $arrival_time;
					my $departure_time;

					for my $i (0 .. $#stops) {
						if (not exists $FT{$timingpattern}) {
							print "Trip handling failed: timingpattern of trip was not defined in header ($timingpattern)!\n";
							last;
						}
						if (not exists $FT{$timingpattern}[$i]) {
							print "Trip handling failed: timingpattern for stop #$i ($stops[$i]) not found!\n";
							last;
						}

						my $ft_timing_pattern = $FT{$timingpattern}[$i];
						if ($ft_timing_pattern ne '|' and $ft_timing_pattern ne '$' and $ft_timing_pattern ne '-') {
							my $sth = $dbh->prepare('INSERT OR REPLACE INTO stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence) values (?, ?, ?, ?, ?)');

							$minutes = $minutes + $ft_timing_pattern;
							if ($minutes > 59) {
								$minutes -= 60;
								$hours++;
							}
							$minutes = sprintf("%02d", $minutes);
							$arrival_time = "$hours:$minutes:00";

							# handle departures/arrivals at same stop: take time of next stop and use it as departure
							if ($i < $#stops and $stops[$i] eq $stops[$i+1] and $FT{$timingpattern}[$i + 1] ne '-' and $FT{$timingpattern}[$i + 1] ne '$' and $FT{$timingpattern}[$i + 1] ne '|') {
								my $dep_hours = $hours;
								my $dep_minutes = $minutes + $FT{$timingpattern}[$i+1];
								if ($dep_minutes > 59) {
									$dep_minutes -= 60;
									$dep_hours++;
								}

								$dep_minutes = sprintf("%02d", $dep_minutes);
#								print "$process{tripname}\t$hours:$minutes\t$dep_hours:$dep_minutes\t$stops[$i]\t$i\n";
		 						print $log ("\t$i\t$ft_timing_pattern\tFW LayO\t $stops[$i] at $hours:$minutes leave $dep_hours:$dep_minutes\n");
								$departure_time = "$dep_hours:$dep_minutes:00";
							# if the above procedure has been performed, the next iteration is skipped
							} elsif ($i > 1 and $stops[$i] eq $stops[$i-1] and $FT{$timingpattern}[$i - 1] ne '-' and $FT{$timingpattern}[$i - 1] ne '$' and $FT{$timingpattern}[$i - 1] ne '|') {
								print $log ("\t$i\t$ft_timing_pattern\tFW next\t $stops[$i]\n");
								next;
							# regular arrival/departure handling
							} else {
								$departure_time = $arrival_time;
#								print $log ("$process{tripname}\t$hours:$minutes\t$hours:$minutes\t$stops[$i]\t$i\n");
								print $log ("\t$i\t$ft_timing_pattern\tFW Stop\t $stops[$i] at $hours:$minutes\n");
							}

							$sth->execute($process{tripname}.$tripid,$arrival_time,$departure_time,$stops[$i],$i);
						} else {
							print $log ("\t$i\t$ft_timing_pattern\tFW Skip\t $stops[$i]\n");
						}
					}
				} else {
					print "Trip handling failed: $current_line\n";
				}
			}

			# -------------------------
			# END OF TRIPS
			# -------------------------

			# -------------------------
			# HEADSIGN HANDLING
			# -------------------------

			elsif ($line =~ s/^EE//) {

				# example:
				# EER "Wiley" 3230100000-00000001_00000000000000000000

				if ($line =~ /(?<direction>[HR])\s\"(?<headsign>.*)\"\s+(?<serviceid>.{1})(?<tid>[0-9]{4}).*(?<startingstop>[0-9]{3})_/) {
				my $tripid;
					if ($+{tid} == 0) {
						print $log "Headsign for all trips of $process{tripname}$+{direction}$+{serviceid}: $+{headsign} (starting at $+{startingstop})\n";
						$tripid = "$process{tripname}$+{direction}$+{serviceid}%";

						# discriminate: if startingstop is 1 (first stop), set headsign for routeuid
						# within TRIPS table. Otherwise, update STOP_TIMES table
						if ($+{startingstop} == 1) {
							my $sth = $dbh->prepare('UPDATE trips set trip_headsign = ? where trip_id LIKE ?');
							$sth->execute($+{headsign},$tripid);
						} else {
							my $sth = $dbh->prepare('UPDATE stop_times set stop_headsign = ? where trip_id LIKE ? and stop_sequence >= ?');
							$sth->execute($+{headsign},$tripid, $+{startingstop}-1);
						}
					} else {
						print $log "Headsign for trip $process{tripname}$+{direction}$+{serviceid}%$+{tid}: $+{headsign} (starting at $+{startingstop})\n";
						$tripid = $process{tripname} . $+{direction} . $+{serviceid} . "%" . $+{tid};
						# discriminate: if startingstop is 1 (first stop), set headsign for routeuid
						# within TRIPS table. Otherwise, update STOP_TIMES table
						if ($+{startingstop} == 0) {
							my $sth = $dbh->prepare('UPDATE trips set trip_headsign = ? where trip_id LIKE ?');
							$sth->execute($+{headsign},$tripid);
						} else {
							my $sth = $dbh->prepare('UPDATE stop_times set stop_headsign = ? where trip_id LIKE ? and stop_sequence >= ?');
							$sth->execute($+{headsign},$tripid, $+{startingstop}-1);
						}
					}
				} else {
					print "Headsign handling failed! $current_line\n";
				}
			}

			# -------------------------
			# END OF HEADSIGN HANDLING
			# -------------------------


			# ---------------------------------
			# BUS NAME AND DESCRIPTION PARSING
			# ---------------------------------

			elsif ($line =~ s/^BU//) {
				if ($line =~ /(?<direction>[HR])\s\"(?<shortid>.*)\"\s\"(?<routetype>.*)\"\s(\".*\")\s\"(?<longid1>.*)\"\s\"(?<longid2>.*)\"\s(\".*\")\s(\".*\")\s[0-9]*[NY]/) {
					if (not defined $process{textbalang}) {
						$route_long_name = $+{longid1} . $+{longid2};
					} else {
						$route_long_name = $process{textbalang};
					}

					# take care of route types
					# bus 						3
					# bahn (rail)			2
					# strab (tram)		0
					# SAM (taxi)			3

					switch ($+{routetype}) {
						case "bus"		{ $route_type = 3 }
						case "bahn"		{ $route_type = 2 }
						case "strab"	{ $route_type = 0 }
						case "SAM"		{ $route_type = 3 } #FIXME
						case "AST"		{ $route_type = 3 } #FIXME
						case "Fahrradbus"		{ $route_type = 3 } #FIXME
						else					{ $route_type = 99}
					}
				} else {
					print "Bus description handling failed! $current_line\n";
				}
			}

			# -------------------------------------
			# END OF BUS NAME/DESCRIPTION PARSING
			# -------------------------------------
		}

		my $sth = $dbh->prepare('UPDATE routes SET route_type = ?, route_long_name= ? where route_id IS ?');
		$sth->execute($route_type,$route_long_name,$process{route});

		$dbh->commit;

		close FILE;
	} else {
		warn "Could not open file $file: $!";
	}
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

	my $divadatabase = "$db_folder/divadata.db";
	my $divadsn = "DBI:$driver:dbname=$divadatabase";
	$divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 })
		                    or die $DBI::errstr;

	# sacrificing security for speed
	$dbh->{AutoCommit} = 0;
	$dbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

	print "Opened database successfully\n";
}

