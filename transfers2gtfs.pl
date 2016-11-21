#!/usr/bin/perl

use warnings;
use strict;
#use DateTime;
use DBI;
use File::Path qw(make_path);

my $dbh;
my $divadbh;
my $Strf = "%Y%m%d";

dbconnect();
findtransfers();
disconnect();

# We will look for transfers between trips now.
# Transfers can have two types in both DIVA and GTFS:
#   a) transfers where you can stay in the same vehicle (sitz_blb=Y or y in DIVA, which is „Sitzenbleiben“, or „just stay seated for transferring“), and
#   b) transfers where you have to change your vehicle at a certain stop (sitz_blb=N in DIVA).

sub findtransfers {
	my $sth = $divadbh->prepare('SELECT hst_nr_an, linie_erg_an, richt_an, wttyp_an, zeit_von_an, zeit_bis_an, hst_nr_ab, linie_erg_ab, richt_ab, wttyp_ab, zeit_von_ab, zeit_bis_ab, sitz_blb
	FROM TransferProtection');

	$sth->execute();

	while (my $row = $sth->fetchrow_hashref()) {
		# In DIVA, the time frames are calculated in seconds from midnight. I introduced a little
		# subroutine to convert those seconds into a hh:mm:ss string
		my $from_starttime = secondstogtfstime($row->{zeit_von_an});
		my $from_endtime = secondstogtfstime($row->{zeit_bis_an});

		my $to_starttime = secondstogtfstime($row->{zeit_von_ab});
		my $to_endtime = secondstogtfstime($row->{zeit_bis_ab});

		# Startroutes look like "87005" or "87004e", or "219E_e" in DIVA
		# They translate to 87005_ or 87004e or 219E_e in the GTFS trips table
		my $startroute = $row->{linie_erg_an};
		$startroute =~ /(?<basename>.{2}.{2}[^_]?)(?<suffix>.*)/;
		if ($+{suffix} eq '') {
			$startroute = $startroute . '\_';
		}
		my $endroute = $row->{linie_erg_ab};
		$endroute =~ /(?<basename>.{2}.{2}[^_]?)(?<suffix>.*)/;
		if ($+{suffix} eq '') {
			$endroute = $endroute . '\_';
		}
		my $from_stop = $row->{hst_nr_an};
		my $to_stop = $row->{hst_nr_ab};

		# If wttyp_an or wttyp_ab is A, this transfer is valid for _all_ day types starting/ending with this
		# route/stop combination within the given time frame.
		if ($row->{wttyp_an} eq "A") {
			# ALL day types! First, from day type 0.

			my %job = (
				starttrip => $startroute.$row->{richt_an}."0%",
				from_stop => $from_stop,
				from_starttime => $from_starttime,
				from_endtime => $from_endtime,
				to_stop => $to_stop,
				to_starttime => $to_starttime,
				to_endtime => $to_endtime,
				block => $row->{sitz_blb}
			);

			if ($row->{wttyp_ab} eq "A") {
				$job{endtrip} = $endroute.$row->{richt_ab}."0%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."2%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."3%";
				messyblockhandler(%job);
			} else {
				$job{endtrip} = $endroute.$row->{richt_ab}.$row->{wttyp_ab};
				messyblockhandler(%job);
			}

			# NOW, from day type 2
			$job{starttrip} = $startroute.$row->{richt_an}."2%";
			if ($row->{wttyp_ab} eq "A") {
				$job{endtrip} = $endroute.$row->{richt_ab}."0%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."2%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."3%";
				messyblockhandler(%job);
			} else {
				$job{endtrip} = $endroute.$row->{richt_ab}.$row->{wttyp_ab};
				messyblockhandler(%job);
			}

			#FINALLY, from day type 3
			$job{starttrip} = $startroute.$row->{richt_an}."3%";
			if ($row->{wttyp_ab} eq "A") {
				$job{endtrip} = $endroute.$row->{richt_ab}."0%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."2%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."3%";
				messyblockhandler(%job);
			} else {
				$job{endtrip} = $endroute.$row->{richt_ab}.$row->{wttyp_ab};
				messyblockhandler(%job);
			}
		}
		elsif ($row->{wttyp_an} eq "1" or $row->{wttyp_ab} eq "1") {
			print "ERROR: THIS NEEDS FIXME\n"; #FIXME
		}
		else { #
			my $starttrip = $startroute.$row->{richt_an}.$row->{wttyp_an}."%";
			my %job = (
				starttrip => $starttrip,
				from_stop => $from_stop,
				from_starttime => $from_starttime,
				from_endtime => $from_endtime,
				to_stop => $to_stop,
				to_starttime => $to_starttime,
				to_endtime => $to_endtime,
				block => $row->{sitz_blb}
			);

			# Again, handling of day type A for the departing trips
			if ($row->{wttyp_ab} eq "A") {
				$job{endtrip} = $endroute.$row->{richt_ab}."0%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."2%";
				messyblockhandler(%job);
				$job{endtrip} = $endroute.$row->{richt_ab}."3%";
				messyblockhandler(%job);
			} else {
				$job{endtrip} = $endroute.$row->{richt_ab}.$row->{wttyp_ab}."%";
				messyblockhandler(%job);
			}
		}
	}
}

#  / \ WARNING WARNING WARNING WARNING WARNING
# / ! \ The following code is a complete and utter mess. It is slow as hell and in dire need of better SQL requests
# -----  WARNING WARNING WARNING WARNING WARNING
# TODO FIXME

sub messyblockhandler {
	my %messyparams = @_;

	print ("Transfer from $messyparams{starttrip} to $messyparams{endtrip} at $messyparams{from_stop} to $messyparams{to_stop} from $messyparams{from_starttime}, $messyparams{from_endtime} to $messyparams{to_starttime}, $messyparams{to_endtime}, Block: ", $messyparams{block},"\n");

	my $sth = $dbh->prepare('SELECT trips.trip_id AS trip_id, arrival_time, stop_id, block_id, service_id
	FROM trips
	JOIN stop_times on trips.trip_id = stop_times.trip_id
	WHERE trips.trip_id like ? ESCAPE "\" AND arrival_time >= ? AND arrival_time <= ? AND stop_id LIKE ?');

	$sth->execute($messyparams{starttrip}, $messyparams{from_starttime}, $messyparams{from_endtime}, $messyparams{from_stop}."%");

	my %block_identifier;
	my %triptransfer;

	while (my $arrival_triprow = $sth->fetchrow_hashref()) {
		my $current_arrival_time = $arrival_triprow->{arrival_time};
		my $current_arrival_trip = $arrival_triprow->{trip_id};
		my $current_arrival_stop = $arrival_triprow->{stop_id};

		# Transfer by staying on the vehicle
		if ($messyparams{block} eq "Y" or $messyparams{block} eq "y") {
			# Does the inbound trip already have a block ID? If yes, we'll use that later on!
			if (defined $arrival_triprow->{block_id}) {
				$block_identifier{$current_arrival_trip} = $arrival_triprow->{block_id};
			}
			# If not, we will just use the current trip as a block identifier
			else {
				$block_identifier{$current_arrival_trip} = $current_arrival_trip;
			}
			print ("trip1: " , $current_arrival_trip, " $current_arrival_time gets " . $block_identifier{$current_arrival_trip}, "\n");
		}

		# Let's find matching departure trips for this arrival trip! Look at all
		# departures between the inbound trip's arrival time and the end of the transfer time frame.
		# TODO is this better?
		# TODO select trip_id, min(arrival_time) from stop_times where trip_id like ? ESCAPE "\" and arrival_time >= ? and arrival_time <= ? and stop_id LIKE ?;

		my $sth = $dbh->prepare('SELECT trips.trip_id, arrival_time, stop_id
		FROM trips
		JOIN stop_times ON trips.trip_id = stop_times.trip_id
		WHERE trips.trip_id LIKE ? ESCAPE "\" AND arrival_time >= ? AND arrival_time <= ? AND stop_id LIKE ? AND service_id = ?
		ORDER BY arrival_time ASC
		LIMIT 1;');

		$sth->execute($messyparams{endtrip}, $current_arrival_time,$messyparams{to_endtime}, $messyparams{from_stop}."%", $arrival_triprow->{service_id});

		my $transfersth = $dbh->prepare('INSERT INTO transfers (from_stop_id, to_stop_id, transfer_type, from_trip_id, to_trip_id) VALUES (?, ?, ?, ?, ?)');
		while (my $departure_triprow = $sth->fetchrow_hashref()) {
			my $current_departure_trip = $departure_triprow->{trip_id};
			my $current_departure_stop = $departure_triprow->{stop_id};

			# Transfer by staying on the vehicle
			if (($messyparams{block} eq "Y") or ($messyparams{block} eq "y")) {
				$block_identifier{$current_departure_trip} = $block_identifier{$current_arrival_trip};
				print ("trip2: " , $departure_triprow->{trip_id} , ", ", $departure_triprow->{arrival_time} ," gets " . $block_identifier{$current_departure_trip} . "\n");
			}
			# Else: Write a transfer
			elsif ($messyparams{block} eq "N") {
				$transfersth->execute($current_arrival_stop, $current_departure_stop, 1, $current_departure_trip, $current_arrival_trip);
			}
		}

		$dbh->commit();
	}

	# Finally, if the current request was for block transfers, use the temporary hash
	# to write everything to the GTFS database!
	if ($messyparams{block} eq "Y") {
		my $updatesth = $dbh->prepare('UPDATE trips SET block_id = ? WHERE trip_id = ?');
		for (keys %block_identifier) {
#			print "$_: $block_identifier{$_}\n"
			$updatesth->execute($block_identifier{$_}, $_);
		}

		$dbh->commit();
	}
}

#--------------------------------------------------------------------
# Time conversion subroutine, calculate hh:mm:ss string from seconds
# -------------------------------------------------------------------

sub secondstogtfstime {
	foreach (@_) {
		return (sprintf ("%02d", int($_/60)) . ":" . sprintf ("%02d", $_%60) . ":00");
	}
}

# --------------------
# CONNECT TO DATABASE
# --------------------

sub dbconnect {
	my $db_folder = "build/data";
	make_path($db_folder);

	my $driver = "SQLite";
	my $database = "$db_folder/diva2gtfs.db";
	my $dsn = "DBI:$driver:dbname=$database";
	my $userid = "";
	my $password = "";
	$dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
		                    or die $DBI::errstr;
	$dbh->{AutoCommit} = 0;
	$dbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

	print "Opened database successfully\n";

	my $divadatabase = "$db_folder/divadata.db";
	my $divadsn = "DBI:$driver:dbname=$divadatabase";
	$divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 })
		                    or die $DBI::errstr;
}

sub disconnect {
	$dbh->disconnect();
	print "GTFS-Database closed.\n";

	$divadbh->disconnect();
	print "Diva-Database closed.\n";

	print "Everything done.\n";
	print "Bye!\n";
}
