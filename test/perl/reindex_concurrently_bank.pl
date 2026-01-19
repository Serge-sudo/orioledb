use strict;
use warnings FATAL => 'all';

use File::Temp qw(tempfile);
use FindBin;
use Time::HiRes qw(usleep);
use lib "$FindBin::RealBin/../../../../src/test/perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('reindex_concurrently_bank');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'orioledb'\n");
$node->start;

$node->safe_psql(
'postgres',
q{
CREATE EXTENSION orioledb;
CREATE TABLE o_bank (
id int PRIMARY KEY,
balance int
) USING orioledb;
INSERT INTO o_bank VALUES (1, 0);
});

my $updates = 20;
my $lock_key = 4242;
my ($fh, $filename) = tempfile('bank_updates_XXXX', SUFFIX => '.sql', UNLINK => 1);
print $fh "SELECT pg_advisory_lock($lock_key);\n";
for (1 .. $updates)
{
print $fh <<'SQL';
BEGIN;
UPDATE o_bank SET balance = balance + 1 WHERE id = 1;
COMMIT;
SELECT pg_sleep(0.02);
SQL
}
print $fh "SELECT pg_advisory_unlock($lock_key);\n";
close $fh;

my $pid = fork();
die "fork failed: $!" unless defined $pid;
if ($pid == 0)
{
	exec 'psql', '-X', '-q', '-d', $node->connstr('postgres'), '-f',
		$filename
		or die "exec psql failed: $!";
}

my $started = 0;
for (1 .. 100)
{
	my $locked = $node->safe_psql('postgres',
		"SELECT pg_try_advisory_lock($lock_key);");
	chomp $locked;
	if ($locked eq 't')
	{
		$node->safe_psql('postgres',
			"SELECT pg_advisory_unlock($lock_key);");
		usleep(20_000);
		next;
	}
	$started = 1;
	last;
}
ok($started, 'update workload started');
$node->safe_psql('postgres', 'REINDEX INDEX CONCURRENTLY o_bank_pkey;');
waitpid($pid, 0);
is($?, 0, 'update workload finished');

my $balance = $node->safe_psql('postgres', 'SELECT sum(balance) FROM o_bank;');
chomp $balance;
is($balance, $updates, 'balance reflects concurrent updates');

$node->stop;

done_testing();
