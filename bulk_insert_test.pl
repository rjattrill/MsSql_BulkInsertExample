use strict;
use warnings;
use v5.16;
use DBI;
use Data::Dump qw(dump);

use FindBin qw($Bin);


my $dbh = &get_dbh;
say $dbh;

&create_table($dbh);

&bulk_insert($dbh);

&drop_table($dbh);

sub create_table {
	my $dbh = shift;

	say "Creating table AA";

	my $sql = "
		CREATE TABLE AA
		(
		   AA_ID     	int identity    NOT NULL,
		   B_ID    		int             NOT NULL,
		   PERIOD_START		datetime	NOT NULL,
		   QUALITY_CODE      nvarchar(4),
		   VALUE_NUM         numeric(18,4)
		);";
	

	my $rc = $dbh->do($sql);
}

sub bulk_insert {
	my $dbh = shift;

	my $data_file = $Bin . '/aa.dat';
	die "No data file" unless -e $data_file;

	my $format_file = $Bin . '/aa.fmt';
	die "No format file" unless -e $format_file;


	my $sql_date_format = "SET DATEFORMAT ymd";
	$dbh->do($sql_date_format);

	#	The approach of selecing from OPENROWSET and then INSERTING seems to be far more robust than BULK INSERT.
	#		* The built in string to type conversion of the bulk utilities appear to be very rough
	#		* The string conversion facilities of ordinary INSERT DML are well documented and understood
	#		* This approach can be broken down into the SELECT and INSERT parts separately
	my $sql = "
		INSERT INTO dbo.AA (B_ID, PERIOD_START, QUALITY_CODE, VALUE_NUM)
		SELECT a.* FROM OPENROWSET(
		BULK '$data_file',
		FORMATFILE = '$format_file',
		FIRSTROW = 1
		) as a
	";

	#	See above.
	# my $sql = "
	# 	SET DATEFORMAT ymd;

	# 	BULK INSERT dbo.AA
	# 	FROM '$data_file'
	# 	WITH (	FORMATFILE = '$format_file')
	# ";

	say "bulk insert sql:\n$sql";

	my $rc = $dbh->do($sql);
	say "Inserted $rc rows" if $rc;

	#	Check
	$sql = "SELECT * FROM AA";
	my $results = $dbh->selectall_arrayref($sql);
	say "Results: " . dump $results;
}

sub drop_table {
	my $dbh = shift;
	say "Dropping table AA";

	my $sql = "DROP TABLE AA";
	my $rc = $dbh->do($sql);
}

sub get_dbh {
	# my $server = 'localhost';
	my $server='MY_SERVER\SQLEXPRESS';
	my $driver = '{SQL Server Native Client 10.0}';
    my $database='db';
    my $username='my';
    my $password='secret';

    my $dbi_dsn = "dbi:ODBC:Driver=$driver;Server=$server;Database=$database;";
    my $dbh = DBI->connect($dbi_dsn, $username, $password) or die $DBI::errstr;
    return $dbh;
}

