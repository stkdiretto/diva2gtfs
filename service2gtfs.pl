#!/usr/bin/perl

use warnings;
use strict;
use DateTime;
use DBI;
#use Date::Holidays;
#use Geo::GeoNames;
#use Date::Holidays::AT;
use Date::Holidays::DE;
use File::Path qw(make_path);

my $dbh;
my $divadbh;
my @holiday_region = ['common', 'bw']; # Holidays for germany and baden-wuerttemberg
#my @holiday_region = ['common', 'T'];
my $tf_dt = "%Y%m%d";
my $tf_sprintf = "%04d%02d%02d";
my $tz = DateTime::TimeZone->new( name => 'floating' );
my $tz_holidays = DateTime::TimeZone->new( name => 'Europe/Berlin' );
my $calendar_date_remove = 2;
my $calendar_date_add = 1;

## Main method

dbconnect();

#my $country_code = get_country_code();
my $cnt = get_restriction_count();

my $sth = $divadbh->prepare('SELECT anfjahr, code, dat_von, dat_bis, kenn, vbt_von, vbt_bis, vt FROM ServiceRestriction ORDER BY code ASC');
$sth->execute();

my $sthCalendarInsert = $dbh->prepare('INSERT OR REPLACE INTO calendar (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
my $sthCalendarDateInsert = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');

my $date_from = undef;
my $date_to = undef;
my $i = 0;
while (my $row = $sth->fetchrow_hashref()) {
	my $code = $row->{code};
	if (defined $row->{kenn} and $row->{kenn} eq 'I') {
		$code = substr $code, 1; # Skip first character
	}

	$i += 1;
	my $referencedate = DateTime->new(
		year       => $row->{anfjahr},
		month      => 1,
		day        => 1,
		time_zone  => $tz,
	);
	$referencedate->truncate(to => 'day');

	# clone starting date and add start and end months to get start and end date
	my $startdate = $referencedate->clone();
	$startdate->add(months => $row->{vbt_von});
	my $enddate = $referencedate->clone();
	$enddate->add(months => $row->{vbt_bis});

	if (! defined $date_from  || $date_from > $startdate) {
		$date_from = $startdate->clone();
	}
	if (! defined $date_to || $date_to < $enddate) {
		$date_to = $enddate->clone();
	}

	print ("$i/$cnt: $code, $referencedate, von: ", $startdate->strftime($tf_dt) , ", bis: ", $enddate->strftime($tf_dt), "\n");
	my %mi_args = (
		code => $code,
		date => $startdate,
		vt => $row->{vt}
	);
	my @months = get_month_information(%mi_args);
	my @week = get_calendar(@months);
	$sthCalendarInsert->execute($code, $week[0], $week[1], $week[2], $week[3], $week[4], $week[5], $week[6], $startdate->strftime($tf_dt), $enddate->strftime($tf_dt));

	foreach my $m (@months) {
		insert_calendardates4month(\@week, $m);
	}

	$dbh->commit();
}

# insert most common calendar dates (if not already done above)
$sth = $dbh->prepare('INSERT OR IGNORE INTO calendar VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
$sth->execute(0, 1, 1, 1, 1, 1, 0, 0, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # monday till friday (without holidays)
$sth->execute(1, 1, 1, 1, 1, 1, 1, 0, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # monday till saturday (without holidays))
$sth->execute(2, 0, 0, 0, 0, 0, 1, 0, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # saturday
$sth->execute(3, 0, 0, 0, 0, 0, 0, 1, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # sunday and holiday
$sth->execute(4, 0, 0, 0, 0, 0, 1, 1, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # saturday, sunday and holiday
$sth->execute('A', 1, 1, 1, 1, 1, 1, 1, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt)); # every day
$dbh->commit();

insert_holidays($date_from, $date_to);

disconnect();

##########################
## Subroutine definition #
##########################

sub get_calendar {
	my @process = @_;

	my @week_cnt = (0, 0, 0, 0, 0, 0, 0);
	my @week = (0, 0, 0, 0, 0, 0, 0);
	foreach my $m (@process) {
		my $day_offset = $m->{first_day_of_month} - 1;
		for (my $day = 0; $day < $m->{days_in_month}; ++$day) {
			my $i = ($day % 7 + $day_offset) % 7;
			++$week_cnt[$i];

			if ($m->{validity}[$day]) {
				++$week[$i];
			}
		}
	}

	# result calendar is not calculated in a very clever way
	#  -> day is marked as active if service is used more then the half of days
	# however, it seems to work if their is enough data :-)
	my @result = (0, 0, 0, 0, 0, 0, 0);
	for my $i (0 .. $#week) {
		if ((2 * $week[$i]) > $week_cnt[$i]) {
			$result[$i] = 1;
		}
	}

	return @result;
}

#sub get_country_code {
#	my $sth = $dbh->prepare('SELECT AVG(stop_lat) AS lat, AVG(stop_lon) AS lng FROM stops');
#	$sth->execute();
#
#	my $lat;
#	my $lng;
#	while (my $row = $sth->fetchrow_hashref()) {
#		$lat = $row->{lat};
#		$lng = $row->{lng};
#	}
#
#	my $geo = new Geo::GeoNames(username => "diva2gtfs");
#	my $country_code = $geo->country_code(
#		lat => $lat,
#		lng => $lng
#	);
#
#	return $country_code;
#}

sub get_month_information {
	my %process = @_;

	my @job_arr = ();
	my @vt = $process{vt} =~ /[0-9A-Fa-f]{8}/g;
	my $workingdate = $process{date}->clone();
	foreach my $vtmonth (@vt) {
		my $first_day = $workingdate->day_of_week();
		my $month = $workingdate->month();
		my $year = $workingdate->year();
		# create list of dates for current month
		my $enddate = DateTime->last_day_of_month(
			month => $month,
			year => $year
		);
		my $last_day = $enddate->day();

		# convert hexadeximal month pattern into binary and put it into a sorted array (starting with the first day of the month).
		# each day becomes one array cell with 0 or 1 here...
		my @validity = (map { unpack ('B*', pack ('H*', $_)) } $vtmonth)[0] =~ /[01]/g;
		@validity = reverse (@validity);

		my %job = (
			code => $process{code},
			days_in_month => $last_day,
			month => $month,
			first_day_of_month => $first_day,
			validity => \@validity,
			year => $year
		);

		push @job_arr, \%job;
		$workingdate->add(months => 1);
	}

	return @job_arr;
}

sub get_restriction_count {
	my $sth = $divadbh->prepare('SELECT COUNT(*) AS cnt FROM ServiceRestriction');
	$sth->execute();

	my $cnt = 0;
	while (my $row = $sth->fetchrow_hashref()) {
		$cnt = $row->{cnt};
	}

	return $cnt;
}

sub insert_calendardates4month {
	my @week = @{$_[0]};
	my %process = %{$_[1]};
	my $day_offset = $process{first_day_of_month} - 1;

	# iterate over all dates of the current month
	for (my $day = 0; $day < $process{days_in_month}; ++$day) {
		my $i = ($day % 7 + $day_offset) % 7;
		my $is_valid = $process{validity}[$day];

		if (($is_valid and !$week[$i]) or (!$is_valid and $week[$i])) {
			# consider days where this service_id is valid and has not been defined using the service calendar table (or vice-versa)
			my $time = sprintf $tf_sprintf, $process{year}, $process{month}, ($day + 1);
#			print ("$process{code}, $time, $is_valid\n");
			$sthCalendarDateInsert->execute($process{code}, $time, ($is_valid) ? $calendar_date_add : $calendar_date_remove);
		}
	}

#   The following loop would do the same as the loop above, but with strict DateTime usage (which is more flexibel when it comes to date formatting, but far slower)
#	for (my $date = $process{date}->clone; $date <= $enddate; $date->add( days => 1 )) {
#		my $day = $date->day();
#		my $i = ($day % 7 + $day_offset) % 7;
#		my $is_valid = $process{validity}[$day - 1];
#		if (($is_valid and !$week[$i]) or (!$is_valid and $week[$i])) {
#			# consider days where this service_id is valid and has not been defined using the service calendar table (or vice-versa)
#			my time = $date->strftime($tf_dt);
#			print ("$process{code}, $time, $is_valid\n");
#			$sthCalendarDateInsert->execute($process{code}, time, $is_valid);
#		}
#	}
}

# Takes care of the holiday
sub insert_holidays {
	my $from = $_[0];
	my $to = $_[1];
	my $year_from = $from->year();
	my $year_to = $to->year();
	my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');
	my $week_day_saturday = 6;

	print "Handling holidays:\n";
	for (my $year = $year_from; $year <= $year_to; ++$year) {
		my $holidays_ref = holidays(
			WHERE => @holiday_region,
			YEAR => $year
		);
		my @holidays = @$holidays_ref;
#		my @holidays = holidays(
#			countrycode => $country_code,
#			year => $year
#		);

		print "  - year $year loaded\n";
		foreach my $holiday (@holidays) {
			my $holiday_date = DateTime->from_epoch(
				epoch => $holiday,
				time_zone => $tz_holidays
			);

			$holiday_date->truncate( to => 'day' );
			my $day_of_week = $holiday_date->day_of_week();

			if ($day_of_week < $week_day_saturday) {
				# monday through friday: Disable Mo-Fr service, enable sunday service
				print ("    holiday ", $holiday_date->strftime($tf_dt), ": removing service 0, adding services 3 and 4", "\n");
				$sth->execute(0, $holiday_date->strftime($tf_dt), $calendar_date_remove);
				$sth->execute(3, $holiday_date->strftime($tf_dt), $calendar_date_add);
				$sth->execute(4, $holiday_date->strftime($tf_dt), $calendar_date_add);
			} elsif ($day_of_week == $week_day_saturday) {
				# saturday: disable sa service, enable sunday service
				print ("    holiday ", $holiday_date->strftime($tf_dt), ": removing service 1, adding services 3 and 4", "\n");
				$sth->execute(1, $holiday_date->strftime($tf_dt), $calendar_date_remove);
				$sth->execute(3, $holiday_date->strftime($tf_dt), $calendar_date_add);
				$sth->execute(4, $holiday_date->strftime($tf_dt), $calendar_date_add);
			}
		}
	}

	$dbh->commit();
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
