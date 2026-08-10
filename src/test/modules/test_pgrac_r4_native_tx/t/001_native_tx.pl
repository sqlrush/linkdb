# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_tx');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_r4_native_tx');

my $unrelated_xid = $node->safe_psql('postgres', q{
	BEGIN;
	SELECT test_pgrac_r4_current_xid();
	COMMIT;
});
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$unrelated_xid'::xid)"),
	'f', 'native 2PC owner rejects an unrelated normal xid');

my $commit_xid = $node->safe_psql('postgres', q{
	BEGIN;
	SELECT test_pgrac_r4_current_xid();
	PREPARE TRANSACTION 'pgrac_r4_native_commit';
});
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$commit_xid'::xid)"),
	't', 'native 2PC owner contains the exact prepared xid');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$commit_xid'::xid)"),
	't', 'native 2PC owner restores the exact prepared xid after crash restart');

$node->safe_psql('postgres',
	"COMMIT PREPARED 'pgrac_r4_native_commit'");
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$commit_xid'::xid)"),
	'f', 'native 2PC owner drops the xid after COMMIT PREPARED');

my $rollback_xid = $node->safe_psql('postgres', q{
	BEGIN;
	SELECT test_pgrac_r4_current_xid();
	PREPARE TRANSACTION 'pgrac_r4_native_rollback';
});
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$rollback_xid'::xid)"),
	't', 'native 2PC owner contains the xid before ROLLBACK PREPARED');

$node->safe_psql('postgres',
	"ROLLBACK PREPARED 'pgrac_r4_native_rollback'");
is($node->safe_psql('postgres',
		"SELECT test_pgrac_r4_two_phase_is_prepared('$rollback_xid'::xid)"),
	'f', 'native 2PC owner drops the xid after ROLLBACK PREPARED');

my $subtrans_edge = $node->safe_psql('postgres', q{
	BEGIN;
	CREATE TEMP TABLE pgrac_r4_native_xids(role text PRIMARY KEY, xid xid)
		ON COMMIT DROP;
	INSERT INTO pgrac_r4_native_xids VALUES
		('top', test_pgrac_r4_current_xid());
	SAVEPOINT pgrac_r4_native_child;
	INSERT INTO pgrac_r4_native_xids VALUES
		('child', test_pgrac_r4_current_xid());
	SELECT child.xid <> top.xid,
		test_pgrac_r4_subtrans_parent(child.xid) = top.xid
	FROM pgrac_r4_native_xids AS child
	CROSS JOIN pgrac_r4_native_xids AS top
	WHERE child.role = 'child' AND top.role = 'top';
	ROLLBACK;
});
is($subtrans_edge, 't|t',
	'native SubTrans records a distinct savepoint child with the exact top parent');

my $future_xid = $node->safe_psql('postgres', q{
	SELECT ((test_pgrac_r4_current_xid()::text::bigint + 1048576)::text::xid)::text;
});
my ($future_rc, $future_stdout, $future_stderr) =
	$node->psql('postgres',
		"SELECT test_pgrac_r4_subtrans_parent('$future_xid'::xid)");
is($future_rc, 3,
	'native SubTrans raises ERROR for a future xid in an absent SLRU segment');
like($future_stderr,
	qr/could not access status of transaction \Q$future_xid\E/,
	'future-xid error identifies the exact native transaction lookup');
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'future-xid SubTrans ERROR leaves the server healthy');

$node->stop('fast');
done_testing();
