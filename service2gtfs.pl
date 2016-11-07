#!/usr/bin/perl

use warnings;
use strict;
use DateTime;
use DBI;
use Date::Holidays::DE qw(holidays);

my $dbh;
my $divadbh;
my $Strf = "%Y%m%d";

dbconnect();

insert_holidays();

	my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth->execute(0,1,1,1,1,1,0,0,undef,undef);
	$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth->execute(2,0,0,0,0,0,1,0,undef,undef);
	$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth->execute(3,0,0,0,0,0,0,1,undef,undef);


	my $sth = $divadbh->prepare('SELECT anfjahr, code, dat_von, dat_bis, vbt_von, vbt_bis, vt from ServiceRestriction');
	$sth->execute();
	
	while (my $row = $sth->fetchrow_hashref()) {
		my $referencedate = DateTime->new(
			year       => $row->{anfjahr},
			month      => 1,
			day        => 1,
			time_zone  => "floating",
		);
		$referencedate->truncate(to => 'day');
		
		# clone starting date and add start and end months to get start and end date
		my $workingdate = $referencedate->clone();
		$workingdate->add( months => $row->{vbt_von} );
		my $enddate = $referencedate->clone();
		$enddate->add ( months => $row->{vbt_bis} );
		
		
		print ("$row->{code}, $referencedate, von: ", $workingdate->strftime($Strf) , ", bis: ",$enddate->strftime($Strf),"\n");

		my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
		$sth->execute($row->{code},0,0,0,0,0,0,0,$workingdate->strftime($Strf),$enddate->strftime($Strf));

		my @vt = $row->{vt} =~ /[0-9A-Fa-f]{8}/g;
		
		foreach my $vtmonth (@vt) {
			my %job = ('code' => $row->{code}, 'month' => $workingdate, 'daypattern' => $vtmonth);
			eval_month(%job);
			$workingdate->add( months => '1' );
		}
	$dbh->commit();
	}
	

disconnect();

sub eval_month{
	my %process = @_;
	
	# Create list of dates for current month
	my $enddate = DateTime->last_day_of_month( year => $process{month}->year(), month => $process{month}->month() );

	#convert hexadeximal month pattern into binary and put it into an array. Each day becomes one array cell with 0 or 1
	my @is_valid = (map { unpack ('B*', pack ('H*',$_)) } $process{daypattern})[0] =~ /[01]/g;
	
	#iterate over all dates of the current month
	for (my $date = $process{month}->clone; $date <= $enddate; $date->add( days => 1 )) {
	my $day = $date->day();
		# consider only days where this service_id is valid
		if ($is_valid[32-$day]) {
#			print ("$process{code},$year$month$day,$is_valid[32-$day]\n");
			my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) values (?, ?, ?)');
			$sth->execute($process{code},$date->strftime($Strf),$is_valid[32-$day]);
		}
	}
	$dbh->commit();

}


# _----------------------
# TAKE CARE OF HOLIDAYS
# -----------------------

sub insert_holidays {
	# get hold of all holidays in Germany and special holidays in Baden-Wuerttemberg
	my $holidays_ref = holidays(
						WHERE=>['common', 'bw']
						);
	my @holidays     = @$holidays_ref;
	print "Holidays for Germany and BaWue loaded!\n";
	foreach my $holiday (@holidays) {
		my $holiday_date = DateTime->from_epoch( epoch => $holiday, time_zone => 'Europe/Berlin' );
		$holiday_date->truncate( to => 'day' );
		my $day_of_week = $holiday_date->day_of_week();

		# monday through friday: Disable Mo-Fr service, enable sunday service
		if ($day_of_week < 6) {
			print ("Disabling 0, enabling 3 for ", $holiday_date->strftime($Strf), "\n");
			my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) values (?, ?, ?)');
			$sth->execute(0,$holiday_date->strftime($Strf),2);
			$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) values (?, ?, ?)');
			$sth->execute(3,$holiday_date->strftime($Strf),1);

		}
		# saturday: disable sa service, enable sunday service
		elsif ($day_of_week == 6) {
			print ("Disabling 2, enabling 3 for ", $holiday_date->strftime($Strf), "\n");
			my $sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) values (?, ?, ?)');
			$sth->execute(2,$holiday_date->strftime($Strf),2);
			$sth = $dbh->prepare('INSERT OR REPLACE INTO calendar_dates (service_id, date, exception_type) values (?, ?, ?)');
			$sth->execute(3,$holiday_date->strftime($Strf),1);		
		}
	}
}

# --------------------
# CONNECT TO DATABASE
# --------------------

sub dbconnect {
	my $driver   = "SQLite"; 
	my $database = "diva2gtfs.db";
	my $dsn = "DBI:$driver:dbname=$database";
	my $userid = "";
	my $password = "";
	$dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
		                    or die $DBI::errstr;
	$dbh->{AutoCommit} = 0;
	$dbh->do( "COMMIT; PRAGMA synchronous=OFF; BEGIN TRANSACTION" );

		print "Opened database successfully\n";

	my $divadatabase = "divadata.db";
	my $divadsn = "DBI:$driver:dbname=$divadatabase";
	$divadbh = DBI->connect($divadsn, $userid, $password, { RaiseError => 1 }) 
		                    or die $DBI::errstr;
}


sub disconnect {
	$dbh->disconnect();
	$divadbh->disconnect();
	print "Disconnected. Bye.\n";
}
