#!/usr/bin/perl

use warnings;
use strict;
use Getopt::Long;
use DBI;
use XML::LibXML;
use DateTime;
use DateTime::Format::Strptime;

my $XMLparser = XML::LibXML->new();

my $EFAendpoint = "http://www.ding.eu/ding3/XML_TRIP_REQUEST2";
my $StopIDregex = "(.*)";
my $StopIDprefix = "";
my $StartDate = "20120101";
my $Database;
my $dbh;
my %Trips;
my %Shapes;
my %Service;
my $Strf = "%Y%m%d";
my $Strp = DateTime::Format::Strptime->new(
		pattern   => $Strf,
    locale    => 'de_DE',
    time_zone => 'Europe/Berlin',
);

my $Monday = DateTime->today->add(days => (8 - DateTime->today->day_of_week) % 7);
my $Tuesday = DateTime->today->add(days => (9 - DateTime->today->day_of_week) % 7);
my $Wednesday = DateTime->today->add(days => (10 - DateTime->today->day_of_week) % 7);
my $Thursday = DateTime->today->add(days => (11 - DateTime->today->day_of_week) % 7);
my $Friday = DateTime->today->add(days => (12 - DateTime->today->day_of_week) % 7);
my $Saturday = DateTime->today->add(days => (13 - DateTime->today->day_of_week) % 7);
my $Sunday = DateTime->today->add(days => (14 - DateTime->today->day_of_week) % 7);

my $Debug = "1";

GetOptions	(	"database=s"	=>	\$Database,
							"efa=s"	=>	\$EFAendpoint,
							"stopprefix=s" => \$StopIDprefix,
							"stopregex=s" => \$StopIDregex,
							"startdate=s" => \$StartDate
						)
						or die("Error in command line arguments\n");

dbconnect();

	my $daysth = $dbh->prepare('SELECT calendar.service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, dmonday, dtuesday, dwednesday, dthursday, dfriday, dsaturday, dsunday 
FROM calendar 
		LEFT JOIN (SELECT service_id, date AS dmonday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 1")) AND exception_type = "2") AS cmonday ON cmonday.service_id = calendar.service_id 
		LEFT JOIN (SELECT service_id, date AS dtuesday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 2")) AND exception_type = "2") AS ctuesday ON ctuesday.service_id = calendar.service_id
		LEFT JOIN (SELECT service_id, date AS dwednesday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 3")) AND exception_type = "2") AS cwednesday ON cwednesday.service_id = calendar.service_id
		LEFT JOIN (SELECT service_id, date AS dthursday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 4")) AND exception_type = "2") AS cthursday ON cthursday.service_id = calendar.service_id
		LEFT JOIN (SELECT service_id, date AS dfriday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 5")) AND exception_type = "2") AS cfriday ON cfriday.service_id = calendar.service_id
		LEFT JOIN (SELECT service_id, date AS dsaturday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 6")) AND exception_type = "2") AS csaturday ON csaturday.service_id = calendar.service_id
		LEFT JOIN (SELECT service_id, date AS dsunday FROM calendar_dates WHERE date=strftime("%Y%m%d",date("now", "weekday 0")) AND exception_type = "2") AS csunday ON csunday.service_id = calendar.service_id');
	$daysth->execute();
		
	while (my $servicerow = $daysth->fetchrow_hashref()) {
		if ($servicerow->{monday} eq "1" and not defined $servicerow->{cmonday}) {
			$Service{$servicerow->{service_id}} = $Monday->strftime($Strf);
		}
		elsif ($servicerow->{tuesday} eq "1" and not defined $servicerow->{ctuesday}) {
			$Service{$servicerow->{service_id}} = $Tuesday->strftime($Strf);
		}
		elsif ($servicerow->{wednesday} eq "1" and not defined $servicerow->{cwednesday}) {
			$Service{$servicerow->{service_id}} = $Wednesday->strftime($Strf);
		}
		elsif ($servicerow->{thursday} eq "1" and not defined $servicerow->{cthursday}) {
			$Service{$servicerow->{service_id}} = $Thursday->strftime($Strf);
		}
		elsif ($servicerow->{friday} eq "1" and not defined $servicerow->{cfriday}) {
			$Service{$servicerow->{service_id}} = $Friday->strftime($Strf);
		}
		elsif ($servicerow->{saturday} eq "1" and not defined $servicerow->{csaturday}) {
			$Service{$servicerow->{service_id}} = $Saturday->strftime($Strf);
		}
		elsif ($servicerow->{sunday} eq "1" and not defined $servicerow->{csunday}) {
			$Service{$servicerow->{service_id}} = $Sunday->strftime($Strf);
		}
	}


my $sth = $dbh->prepare('SELECT route_id,trip_id,shape_id, trips.service_id AS service_id, nextdate, prevdate FROM trips LEFT JOIN (SELECT service_id, MIN(date) as nextdate from calendar_dates WHERE date > strftime(?) AND exception_type <> "2" GROUP BY service_id) AS nextcaldate ON trips.service_id = nextcaldate.service_id LEFT JOIN (SELECT service_id, MAX(date) as prevdate from calendar_dates WHERE date > strftime(?) AND date < strftime(?) AND exception_type <> "2" GROUP BY service_id) AS prevcaldate ON trips.service_id = prevcaldate.service_id');

$sth->execute($Strf,$StartDate,$Strf);

while (my $row = $sth->fetchrow_hashref()) {
	my $trip = $row->{trip_id};
	if (defined $row->{shape_id}){
		$Trips{$trip}{shape_id} = $row->{shape_id};
	}
	$Trips{$trip}{route_id} = $row->{route_id};
	$Trips{$trip}{service_id} = $row->{service_id};
	if (defined $row->{nextdate}) {
		$Trips{$trip}{date} = $row->{nextdate};
	}
	elsif (defined $row->{prevdate}) {
		$Trips{$trip}{date} = $row->{prevdate};
	} 
	elsif (defined $Service{$Trips{$trip}{service_id}}) {
		$Trips{$trip}{date} = $Service{$Trips{$trip}{service_id}};
	}
	else {
		print "ERROR! NO DATE FOUND FOR $trip! Use another startdate, maybe?\n";
	}
	
	my $stopsth = $dbh->prepare('SELECT stop_id, departure_time FROM stop_times WHERE trip_id = ?');
	$stopsth->execute($trip);

	$Trips{$trip}{stopcount} = 0;
	while (my $stoprow = $stopsth->fetchrow_hashref()) {
		push (@{$Trips{$trip}{stops}}, $stoprow->{stop_id});
		push (@{$Trips{$trip}{departure}}, $stoprow->{departure_time});
		$Trips{$trip}{stopcount}++;
	}
	
	
	my $deststh = $dbh->prepare('SELECT f.stop_id AS start, MIN(f.departure_time) AS triptime, MAX (d.departure_time), d.stop_id AS destination, trips.service_id AS service_id FROM stop_times AS f JOIN stop_times as d ON f.trip_id = d.trip_id JOIN trips ON d.trip_id = trips.trip_id WHERE f.trip_id = ? AND d.trip_id = ?');
	$deststh->execute($trip,$trip);
	
	while (my $destrow = $deststh->fetchrow_hashref()) {
		$Trips{$trip}{start} = $destrow->{start};
		$Trips{$trip}{triptime} = $destrow->{triptime};
		$Trips{$trip}{destination} = $destrow->{destination};
		
	}
}

print "\nAll trips loaded\n";

for my $currentTrip (keys %Trips) {
	if (not defined $Trips{$currentTrip}{shape_id}) {
		if ($Debug) { print "New unique trip found, let's call it ", $currentTrip, "!\n"; }
		$Trips{$currentTrip}{shape_id} = $currentTrip;
		shape_request($currentTrip);
		for my $comparisonTrip (keys %Trips) {
			if ($Trips{$currentTrip}{stops} ~~ $Trips{$comparisonTrip}{stops} and $currentTrip ne $comparisonTrip) {
				if ($Debug) { print "Trip $comparisonTrip matches current stop pattern.\n"; }
				$Trips{$comparisonTrip}{shape_id} = $Trips{$currentTrip}{shape_id};
			}
		}
	}
	# Else: Current trip already has a shape assigned to it.
	# Check whether the shape_id is already registered.
	else {
		if ($Debug) { print "$Trips{$currentTrip}{shape_id} found in Trip $currentTrip on " . $Trips{$currentTrip}{date}; }
		if (defined $Shapes{$Trips{$currentTrip}{shape_id}}) {
			if ($Debug) { print ", already defined in Shapes hash. Don't do anything.\n"; }
		}
		else {
			if ($Debug) { print ", adding it to the Shapes hash.\n"; }
			shape_request($currentTrip);
			$Shapes{$Trips{$currentTrip}{shape_id}} = 1;
		}
	}
}

# Update Trips in Database

for my $updateTrip (keys %Trips) {
	if ($Trips{$updateTrip}{shape_id} ne '') {
	my $sth = $dbh->prepare('UPDATE trips set shape_id = ? where trip_id LIKE ?');
	$sth->execute($Trips{$updateTrip}{shape_id},$updateTrip);
	}
}
print "Trips updated in database\n";

$dbh->commit();


# REQUEST SHAPE FROM EFA

sub shape_request {

	my $shapetrip = shift;
	my $start = $Trips{$shapetrip}{start};
	$start =~ s/$StopIDregex/$1/;
	if (defined $StopIDprefix) {
		$start = $StopIDprefix . $start;
	}
	my $destination = $Trips{$shapetrip}{destination};
	$destination =~ s/$StopIDregex/$1/;
	if (defined $StopIDprefix) {
		$destination = $StopIDprefix . $destination;
	}
	
	my $via = $Trips{$shapetrip}{stops}[$Trips{$shapetrip}{stopcount}/2];
	$via =~s/$StopIDregex/$1/;
	if (defined $StopIDprefix) {
		$via = $StopIDprefix . $via;
	}
	
	my $triptime = $Trips{$shapetrip}{triptime};
	$triptime =~ /([0-9]{2})(.*)/;
	my $hour = $1;
	my $restoftime = $2;
	
	if ($hour > 23) {
		$hour = $hour - 24;
		$triptime = $hour. $restoftime;

		my $tempdate = $Strp->parse_datetime($Trips{$shapetrip}{date}, localtime);
		$tempdate->add ( days => 1 );
		$Trips{$shapetrip}{date} = $tempdate->strftime($Strf);
	}
	
	
	my $requesturl = $EFAendpoint .
		"?itdDate=" . $Trips{$shapetrip}{date} .
		"&itdTime=" . $triptime .
		"&itdTripDateTimeDepArr=dep" .
		"&locationServerActive=1" .
		"&type_origin=stop" .
		"&name_origin=" . $start .
		"&type_destination=stop" .
		"&name_destination=" . $destination .
		"&type_via=stop" .
		"&name_via=" . $via .
		"&coordOutputFormat=WGS84" .
		"&coordListOutputFormat=STRING";
		if ($Debug) { print "$requesturl\n"; }
		
	my $efaresult = XML::LibXML->load_xml( location => $requesturl );


	my $coordString = @{ ($efaresult->findnodes('//itdRoute[@changes="0"]/itdPartialRouteList/itdPartialRoute/itdPathCoordinates/itdCoordinateString')) }[0];
	if (defined $coordString) {
		$coordString =~ s/.00000//g;
		$coordString =~ s/ /,/g;
		$coordString =~ s/^<.*>(.*)<.*>$/$1/;

		if ($Debug) { print "$coordString \n"; }

		my @coordArray = split(/,/, $coordString);
		my $sequence = 0;

		for (my $ca = 0; $ca < $#coordArray; $ca = $ca+2) {
			if (length($coordArray[$ca])>4 and length($coordArray[$ca+1])>4) {
				my $longitude = substr($coordArray[$ca],0, length($coordArray[$ca])-6) . "." . substr($coordArray[$ca],-6);
				my $latitude = substr($coordArray[$ca+1],0, length($coordArray[$ca+1])-6) . "." . substr($coordArray[$ca+1],-6);
			
				my $shapesth = $dbh->prepare('INSERT INTO shapes (shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) VALUES (?,?,?,?)');
				$shapesth->execute($Trips{$shapetrip}{shape_id},$latitude,$longitude,$sequence);
			}
		$sequence++;
		}
		$dbh->commit();
	}
}


# HANDLE DATABASE CONNECTION TO GTFS FEED

sub dbconnect {
	my $driver   = "SQLite"; 
	my $dsn = "DBI:$driver:dbname=$Database";
	my $userid = "";
	my $password = "";
	$dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
		                    or die $DBI::errstr;
	$dbh->{AutoCommit} = 0;
	$dbh->do( "PRAGMA synchronous=OFF" );

		print "Opened database successfully\n";
}

sub dbdisconnect {
	$dbh->disconnect();
	print "Disconnected. Bye.\n";
}
