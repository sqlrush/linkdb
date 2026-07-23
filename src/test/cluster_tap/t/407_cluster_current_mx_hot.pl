#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 407_cluster_current_mx_hot.pl
#	  Updater-bearing MultiXact and HOT-chain identity gate.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/407_cluster_current_mx_hot.pl
#
# NOTES
#	  pgrac-original file.
#	  Spec: spec-3.6b-multixact-current-dml.md
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);


sub wait_for
{
	my ($predicate, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		return 1 if $predicate->();
		usleep(200_000);
	}
	return $predicate->() ? 1 : 0;
}


sub state_int
{
	my ($node, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		   FROM pg_cluster_state
		  WHERE category = 'multixact_current' AND key = '$key'});
	return int($value // 0);
}


sub write_retry
{
	my ($node, $sql) = @_;
	for (1 .. 10)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	return 0;
}


sub mirrored_coincident_create
{
	my ($node0, $node1, $name, $ddl) = @_;

	for (1 .. 8)
	{
		return 0 unless write_retry($node0, $ddl);
		return 0 unless write_retry($node1, $ddl);
		my $p0 = $node0->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		my $p1 = $node1->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		return 1 if $p0 eq $p1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		return 0
		  unless write_retry($lag,
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'current_mx_hot',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 5000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.xid_striping = on',
	]);
$pair->start_pair;
usleep(2_000_000);
my ($node0, $node1) = ($pair->node0, $pair->node1);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 peers connected');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxh_t',
		'CREATE TABLE cmxh_t (id int PRIMARY KEY, v int, pad text)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');
$node0->safe_psql('postgres', q{INSERT INTO cmxh_t VALUES (1, 0, repeat('x', 32))});
$node0->safe_psql('postgres', 'CHECKPOINT');

# A key-share locker plus a non-key UPDATE leaves an updater-bearing old
# version.  Updating only v keeps the successor on the HOT chain.
my $locker = $node0->background_psql('postgres', on_error_die => 1);
$locker->query_safe('BEGIN');
$locker->query_safe('SELECT v FROM cmxh_t WHERE id = 1 FOR KEY SHARE');
$node0->safe_psql('postgres', 'UPDATE cmxh_t SET v = v + 10 WHERE id = 1');
$locker->query_safe('COMMIT');
$locker->quit;

my $before = state_int($node1, 'hot_proof_hit_count');
my ($rc, $out, $err) =
  $node1->psql('postgres', 'UPDATE cmxh_t SET v = v + 1 WHERE id = 1 RETURNING v',
	timeout => 30);
is($rc, 0, 'RED-HOT current DML follows the foreign updater chain');
is($out, '11', 'HOT successor is updated exactly once');
ok(wait_for(
		sub { state_int($node1, 'hot_proof_hit_count') > $before },
		10),
	'authoritative full-key HOT MATCH counter advanced');
is(state_int($node1, 'hot_proof_failclosed_count'), 0,
	'positive HOT path did not accept raw-only or unknown evidence');
is($node1->safe_psql('postgres', 'SELECT v FROM cmxh_t WHERE id = 1'), '11',
	'committed value is visible through the primary-key HOT search');

$pair->stop_pair;
done_testing();
