use strict;
use warnings FATAL => 'all';

use File::Temp qw(tempfile);
use FindBin;
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
my ($fh, $filename) = tempfile('bank_updates_XXXX', SUFFIX => '.sql', UNLINK => 1);
for (1 .. $updates)
{
print $fh <<'SQL';
BEGIN;
UPDATE o_bank SET balance = balance + 1 WHERE id = 1;
COMMIT;
SELECT pg_sleep(0.02);
SQL
}
close $fh;

my $pid = fork();
die "fork failed: $!" unless defined $pid;
if ($pid == 0)
{
	exec 'psql', '-X', '-q', '-d', $node->connstr('postgres'), '-f',
		$filename;
	exit 1;
}

sleep 0.1;
$node->safe_psql('postgres', 'REINDEX INDEX CONCURRENTLY o_bank_pkey;');
waitpid($pid, 0);
is($?, 0, 'update workload finished');

my $balance = $node->safe_psql('postgres', 'SELECT sum(balance) FROM o_bank;');
chomp $balance;
is($balance, $updates, 'balance reflects concurrent updates');

$node->stop;

done_testing();
