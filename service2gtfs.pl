#!/usr/bin/perl

use warnings;
use strict;
use DateTime;
use DBI;
use Date::Holidays::DE qw(holidays);
use File::Path qw(make_path);

my $dbh;
my $divadbh;
my $tf_dt = "%Y%m%d";
my $tf_sprintf = "%04d%02d%02d";
my $tz = DateTime::TimeZone->new( name => 'floating' );

dbconnect();

insert_holidays();

my $sth = $divadbh->prepare('SELECT COUNT(*) AS cnt FROM ServiceRestriction');
$sth->execute();

my $cnt = 0;
while (my $rowCnt = $sth->fetchrow_hashref()) {
	$cnt = $rowCnt->{cnt};
}

$sth = $divadbh->prepare('SELECT anfjahr, code, dat_von, dat_bis, vbt_von, vbt_bis, vt FROM ServiceRestriction');
$sth->execute();

my $sthCalendarInsert = $dbh->prepare('INSERT OR REPLACE INTO calendar (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
my $sthCalendarDateInsert = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');

my $date_from = undef;
my $date_to = undef;
my $i = 0;
while (my $row = $sth->fetchrow_hashref()) {
	$i += 1;
	my $referencedate = DateTime->new(
		year       => $row->{anfjahr},
		month      => 1,
		day        => 1,
		time_zone  => $tz,
	);
	$referencedate->truncate(to => 'day');

	# clone starting date and add start and end months to get start and end date
	my $workingdate = $referencedate->clone();
	$workingdate->add(months => $row->{vbt_von});
	my $enddate = $referencedate->clone();
	$enddate->add(months => $row->{vbt_bis});

	if (! defined $date_from  || $date_from > $workingdate) {
		$date_from = $workingdate->clone();
	}
	if (! defined $date_to || $date_to < $enddate) {
		$date_to = $enddate->clone();
	}

	print ("$i/$cnt: $row->{code}, $referencedate, von: ", $workingdate->strftime($tf_dt) , ", bis: ", $enddate->strftime($tf_dt), "\n");
	$sthCalendarInsert->execute($row->{code}, 0, 0, 0, 0, 0, 0, 0, $workingdate->strftime($tf_dt), $enddate->strftime($tf_dt));

	my @vt = $row->{vt} =~ /[0-9A-Fa-f]{8}/g;
	foreach my $vtmonth (@vt) {
		my %job = (
			code => $row->{code},
			month => $workingdate,
			pattern => $vtmonth
		);

		insert_calendardates4month(%job);

		$workingdate->add(months => 1);
	}

	$dbh->commit();
}

$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
$sth->execute(0, 1, 1, 1, 1, 1, 0, 0, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt));
$sth->execute(2, 0, 0, 0, 0, 0, 1, 0, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt));
$sth->execute(3, 0, 0, 0, 0, 0, 0, 1, $date_from->strftime($tf_dt), $date_to->strftime($tf_dt));
$dbh->commit();

disconnect();

sub insert_calendardates4month {
	my %process = @_;

	# convert hexadeximal month pattern into binary and put it into an array. Each day becomes one array cell with 0 or 1
	my @is_valid = (map { unpack ('B*', pack ('H*', $_)) } $process{pattern})[0] =~ /[01]/g;

	my $month = $process{month}->month();
	my $year = $process{month}->year();
	# create list of dates for current month
	my $enddate = DateTime->last_day_of_month(
		month => $month,
		year => $year
	);

	my $last_day = $enddate->day();
	# iterate over all dates of the current month
	for (my $day = 1; $day <= $last_day; ++$day) {
		# consider only days where this service_id is valid
		if ($is_valid[32 - $day]) {
			my $time = sprintf $tf_sprintf, $year, $month, $day;
#			print ("$process{code}, $time, $is_valid[32 - $day]\n");
			$sthCalendarDateInsert->execute($process{code}, $time, $is_valid[32 - $day]);
		}
	}

#   This would do the same as the loop above, but with strict DateTime usage (which is more flexibel when it comes to date formatting, but far slower)
#	for (my $date = $process{month}->clone; $date <= $enddate; $date->add( days => 1 )) {
#		my $day = $date->day();
#		# consider only days where this service_id is valid
#		if ($is_valid[32-$day]) {
#			my time = $date->strftime($tf_dt);
#			print ("$process{code}, $time, $is_valid[32 - $day]\n");
#			$sthCalendarDateInsert->execute($process{code}, time, $is_valid[32 - $day]);
#		}
#	}
}

# Takes care of the holiday
sub insert_holidays {
	# get hold of all holidays in Germany and special holidays in Baden-Wuerttemberg
	my $holidays_ref = holidays(
		WHERE=>['common', 'bw']
	);

	my @holidays = @$holidays_ref;
	my $tz = DateTime::TimeZone->new( name => 'Europe/Berlin' );
	print "Holidays for Germany and BaWue loaded!\n";
	foreach my $holiday (@holidays) {
		my $holiday_date = DateTime->from_epoch(
			epoch => $holiday,
			time_zone => $tz
		);
		$holiday_date->truncate( to => 'day' );
		my $day_of_week = $holiday_date->day_of_week();

		# monday through friday: Disable Mo-Fr service, enable sunday service
		if ($day_of_week < 6) {
			print ("Disabling 0, enabling 3 for ", $holiday_date->strftime($tf_dt), "\n");
			my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');
			$sth->execute(0,$holiday_date->strftime($tf_dt),2);
			$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');
			$sth->execute(3,$holiday_date->strftime($tf_dt),1);

		}
		# saturday: disable sa service, enable sunday service
		elsif ($day_of_week == 6) {
			print ("Disabling 2, enabling 3 for ", $holiday_date->strftime($tf_dt), "\n");
			my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');
			$sth->execute(2,$holiday_date->strftime($tf_dt),2);
			$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)');
			$sth->execute(3,$holiday_date->strftime($tf_dt),1);
		}
	}
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
