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
		my $matched = eval { $predicate->() };
		return 1 if $matched;
		usleep(200_000);
	}
	my $matched = eval { $predicate->() };
	return $matched ? 1 : 0;
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


sub state_key_exists
{
	my ($node, $key) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT EXISTS (
		     SELECT 1
		       FROM pg_cluster_state
		      WHERE category = 'multixact_current' AND key = '$key')}) eq 't';
}


sub tuple_has_hot_updater_multixact
{
	my ($node, $relation) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(bool_or(
		     'HEAP_XMAX_IS_MULTI' = ANY(f.raw_flags)
		     AND 'HEAP_HOT_UPDATED' = ANY(f.raw_flags)
		     AND h.t_ctid <> format('(0,%s)', h.lp)::tid), false)
		   FROM heap_page_items(get_raw_page('$relation', 0)) AS h
		   CROSS JOIN LATERAL
		     heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f}) eq 't';
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
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
	]);
$pair->start_pair;
usleep(2_000_000);
my ($node0, $node1) = ($pair->node0, $pair->node1);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 peers connected');

ok(wait_for(
		sub {
			my $f0 = $node0->safe_psql(
				'postgres',
				q{SELECT coalesce(max(value::bigint), 0)
				    FROM cluster_dump_state()
				   WHERE key = 'mxid_stripe_activated_floor'});
			my $f1 = $node1->safe_psql(
				'postgres',
				q{SELECT coalesce(max(value::bigint), 0)
				    FROM cluster_dump_state()
				   WHERE key = 'mxid_stripe_activated_floor'});
			return $f0 > 0 && $f1 > 0 && $f0 == $f1;
		},
		60),
	'L1 common MultiXact activation floor is ready') or BAIL_OUT('mxid activation floor did not converge');

ok(write_retry($node0, 'CREATE EXTENSION IF NOT EXISTS pageinspect'),
	'L1 pageinspect fixture support is writable') or BAIL_OUT('could not install pageinspect');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxh_t',
		'CREATE TABLE cmxh_t (id int, v int)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');
$node0->safe_psql('postgres', 'INSERT INTO cmxh_t VALUES (1, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');

# Position a node1 cursor on the original tuple before node0 creates the HOT
# successor.  WHERE CURRENT OF later presents that stale root TID directly to
# heap_update, so the current-DML path must prove the updater identity rather
# than letting a fresh MVCC scan skip straight to the successor.
my $seeker = $node1->background_psql('postgres', on_error_die => 1);
$seeker->query_safe('BEGIN');
$seeker->query_safe(
	'DECLARE hot_cur NO SCROLL CURSOR FOR SELECT id, v FROM cmxh_t WHERE id = 1');
my $positioned = $seeker->query_safe('FETCH hot_cur');
BAIL_OUT("could not position HOT root cursor: got '$positioned'")
  unless $positioned eq '1|0';

# A key-share locker plus a non-key UPDATE leaves an updater-bearing old
# version.  Updating only v keeps the successor on the HOT chain.
my $locker = $node0->background_psql('postgres', on_error_die => 1);
$locker->query_safe('BEGIN');
$locker->query_safe('SELECT v FROM cmxh_t WHERE id = 1 FOR KEY SHARE');
$node0->safe_psql('postgres', 'UPDATE cmxh_t SET v = v + 10 WHERE id = 1');
$locker->query_safe('COMMIT');
$locker->quit;

ok(tuple_has_hot_updater_multixact($node0, 'cmxh_t'),
	'RED-HOT fixture contains a HOT-updated updater-bearing MultiXact');
ok(state_key_exists($node1, 'hot_proof_hit_count')
	  && state_key_exists($node1, 'hot_proof_failclosed_count'),
	'current-MultiXact HOT observability keys are exposed');
my $before = state_int($node1, 'hot_proof_hit_count');
my $failclosed_before = state_int($node1, 'hot_proof_failclosed_count');
my ($out, $err) = ('', '');
my $updated = eval {
	$out = $seeker->query_safe(
		'UPDATE cmxh_t SET v = v + 1 WHERE CURRENT OF hot_cur RETURNING v');
	1;
};
$err = $@ unless $updated;
if ($updated)
{
	$seeker->query_safe('COMMIT');
}
else
{
	diag("foreign HOT UPDATE failed: $err");
	eval { $seeker->query_safe('ROLLBACK') };
	diag($node0->safe_psql(
		'postgres',
		q{SELECT string_agg(
		     format('lp=%s xmin=%s xmax=%s ctid=%s flags=%s',
		       h.lp, h.t_xmin, h.t_xmax, h.t_ctid,
		       array_to_string(f.raw_flags, '|')),
		     '; ' ORDER BY h.lp)
		   FROM heap_page_items(get_raw_page('cmxh_t', 0)) AS h
		   CROSS JOIN LATERAL
		     heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f}));
	diag($node1->safe_psql(
		'postgres',
		q{SELECT current_setting('cluster.multi_xmax_remote_resolve')
		     || '; ' || string_agg(key || '=' || value, ', ' ORDER BY key)
		    FROM cluster_dump_state()
		   WHERE key IN ('mxid_stripe_activated_floor',
		                 'mxid_stripe_disk_state',
		                 'mxid_stripe_underivable_reads')}));
	for my $node ($node0, $node1)
	{
		diag($node->safe_psql(
			'postgres',
			q{SELECT string_agg(key || '=' || value, ', ' ORDER BY key)
			    FROM pg_cluster_state
			   WHERE category = 'cr'
			     AND key IN (
			       'cr_server_multi_verdict_served_count',
			       'cr_server_multi_verdict_denied_count',
			       'vis53r97_leg_covers_refuse_count',
			       'vis53r97_leg_multi_unresolvable_count',
			       'vis53r97_leg_multi_member_serve_ask_count',
			       'vis53r97_leg_multi_member_serve_hit_count')}));
	}
}
$seeker->quit;
ok($updated, 'RED-HOT current DML follows the foreign updater chain');
is($out, '11', 'HOT successor is updated exactly once');
ok(wait_for(
	sub { state_int($node1, 'hot_proof_hit_count') > $before },
		10),
	'authoritative full-key HOT MATCH counter advanced');
is(state_int($node1, 'hot_proof_failclosed_count'), $failclosed_before,
	'positive HOT path did not accept raw-only or unknown evidence');
my ($read_rc, $read_out, $read_err) =
  $node1->psql('postgres', 'SELECT v FROM cmxh_t WHERE id = 1', timeout => 30);
is($read_rc, 0, 'committed HOT successor remains readable');
is($read_out, '11', 'committed value is visible through the HOT chain');

$pair->stop_pair;
done_testing();
