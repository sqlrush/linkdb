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


sub wait_for_event
{
	my ($node, $query_like, $event_name, $seconds, $observed) = @_;
	return wait_for(
		sub {
			my $event = $node->safe_psql(
				'postgres',
				qq{SELECT coalesce(wait_event, '')
				     FROM pg_stat_activity
				    WHERE query LIKE '$query_like'
				      AND pid <> pg_backend_pid()
				      AND state = 'active'
				    LIMIT 1});
			$$observed = $event if defined $observed && $event ne '';
			return $event eq $event_name;
		},
		$seconds);
}


sub tuple_has_hot_updater_multixact_sql
{
	my ($relation) = @_;
	return qq{SELECT coalesce(bool_or(
		     'HEAP_XMAX_IS_MULTI' = ANY(f.raw_flags)
		     AND 'HEAP_HOT_UPDATED' = ANY(f.raw_flags)
		     AND h.t_ctid <> format('(0,%s)', h.lp)::tid), false)
		   FROM heap_page_items(get_raw_page('$relation', 0)) AS h
		   CROSS JOIN LATERAL
		     heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f};
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
$node1->safe_psql('postgres', 'INSERT INTO cmxh_t VALUES (1, 0)');
$node1->safe_psql('postgres', 'CHECKPOINT');

# Position a node1 cursor on the original tuple before a second backend creates
# the HOT
# successor.  WHERE CURRENT OF later presents that stale root TID directly to
# heap_update, so the current-DML path must prove the updater identity rather
# than letting a fresh MVCC scan skip straight to the successor.
my $seeker = $node1->background_psql(
	'postgres', on_error_stop => 0, timeout => 45);
$seeker->query_safe('BEGIN');
$seeker->query_safe(
	'DECLARE hot_cur NO SCROLL CURSOR FOR SELECT id, v FROM cmxh_t WHERE id = 1');
my $positioned = $seeker->query_safe('FETCH hot_cur');
BAIL_OUT("could not position HOT root cursor: got '$positioned'")
  unless $positioned eq '1|0';

# A key-share locker plus a non-key UPDATE leaves an updater-bearing old
# version.  Updating only v keeps the successor on the HOT chain.
my $locker = $node1->background_psql('postgres', on_error_die => 1);
$locker->query_safe('BEGIN');
$locker->query_safe('SELECT v FROM cmxh_t WHERE id = 1 FOR KEY SHARE');
# Keep the independent locker and updater on the seeker's node.  Peer mode
# still routes their live updater-bearing MultiXact through the current-MX
# authority bridge, while removing any inter-node PCM-X conversion that could
# mask the exact TX-enqueue wait this test must observe.
my $updater = $node1->background_psql('postgres', on_error_die => 1);
$updater->query_safe('BEGIN');
my $updater_pid = int($updater->query_safe('SELECT pg_backend_pid()'));
is($updater->query_safe(
		'UPDATE cmxh_t SET v = v + 10 WHERE id = 1 RETURNING v'),
	'10', 'RED-HOT updater creates the successor and remains uncommitted');
$locker->query_safe('COMMIT');
$locker->quit;

is($node1->safe_psql(
		'postgres',
		qq{SELECT state = 'idle in transaction' AND xact_start IS NOT NULL
		     FROM pg_stat_activity
		    WHERE pid = $updater_pid}),
	't', 'RED-HOT exact updater transaction is still open');
my @counter_keys = qw(
  wait_count
  wait_resolved_count
  wait_timeout_count
  deadlock_victim_count
  wakeup_count
  hot_proof_hit_count
  hot_proof_failclosed_count
  restart_bucket_1_count);
my $missing_counter_keys
	= grep { !state_key_exists($node1, $_) } @counter_keys;
is($missing_counter_keys, 0,
	'current-MultiXact HOT observability keys are exposed');
my %before = map { $_ => state_int($node1, $_) } @counter_keys;
$seeker->query_until(
	qr/CURRENT_MX_HOT_FIRED/,
	"\\echo CURRENT_MX_HOT_FIRED\n"
	  . "UPDATE cmxh_t SET v = v + 1 WHERE CURRENT OF hot_cur RETURNING v;\n"
	  . "COMMIT;\n"
	  . "\\echo CURRENT_MX_HOT_DONE\n");
my $actual_wait_event = '';
my $wait_observed = wait_for_event(
	$node1,
	'%UPDATE cmxh_t SET v = v + 1 WHERE CURRENT OF hot_cur%',
	'GesTxEnqueueWait', 10, \$actual_wait_event);
if (!$wait_observed)
{
	diag("CURRENT OF observed wait_event=$actual_wait_event");
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
ok($wait_observed,
	'RED-HOT authenticated ACTIVE updater enters GesTxEnqueueWait');
is(state_int($node1, 'wait_count'), $before{wait_count} + 1,
	'RED-HOT current-MultiXact wait counter advances exactly once');
is(state_int($node1, 'hot_proof_hit_count'), $before{hot_proof_hit_count},
	'RED-HOT committed-proof hit is not recorded while updater is open');
is(state_int($node1, 'hot_proof_failclosed_count'),
	$before{hot_proof_failclosed_count},
	'RED-HOT exact ACTIVE proof is not rejected while updater is open');
is(state_int($node1, 'wait_resolved_count'),
	$before{wait_resolved_count},
	'RED-HOT wait remains unresolved before updater commit');
is(state_int($node1, 'restart_bucket_1_count'),
	$before{restart_bucket_1_count},
	'RED-HOT operation has not restarted before updater commit');

$updater->query_safe('COMMIT');
$updater->quit;
my $tail = $seeker->query_until(qr/CURRENT_MX_HOT_DONE/, "");
$seeker->quit;
is($node0->safe_psql(
		'postgres', tuple_has_hot_updater_multixact_sql('cmxh_t')),
	't', 'RED-HOT committed chain retains the updater-bearing MultiXact root');
like($tail, qr/(?:^|\n)11(?:\n|$)/,
	'RED-HOT waiter restarts and updates the successor exactly once');
# hot_proof_hit_count is per successful MATCH consumer, not per outer
# operation.  This WHERE CURRENT OF statement deterministically consumes one
# chain-helper MATCH and two ordinary-updated MATCHes after the wait restart.
# wakeup_count is orthogonal: it counts matching SetLatch signal events, and a
# same-node holder may resolve on the wait loop's exact terminal TT recheck
# without an inbound remote-hint signal.
my $post_commit_contract = wait_for(
		sub {
			return state_int($node1, 'wait_resolved_count')
			  == $before{wait_resolved_count} + 1
			  && state_int($node1, 'restart_bucket_1_count')
			  == $before{restart_bucket_1_count} + 1
			  && state_int($node1, 'hot_proof_hit_count')
			  == $before{hot_proof_hit_count} + 3;
		},
		10);
my %after = map { $_ => state_int($node1, $_) } qw(
  wait_resolved_count
  wakeup_count
  restart_bucket_1_count
  hot_proof_hit_count);
diag("RED-HOT matching SetLatch wakeup signal delta="
	  . ($after{wakeup_count} - $before{wakeup_count}));
if (!$post_commit_contract)
{
	diag(join(', ',
			map {
				"$_=$after{$_} (before=$before{$_}, delta="
				  . ($after{$_} - $before{$_}) . ')'
			} sort keys %after));
}
ok($post_commit_contract,
	'RED-HOT terminal recheck resolves once, restarts once, and consumes three MATCH proofs');
is(state_int($node1, 'wait_timeout_count'), $before{wait_timeout_count},
	'RED-HOT exact ACTIVE wait does not time out');
is(state_int($node1, 'deadlock_victim_count'),
	$before{deadlock_victim_count},
	'RED-HOT exact ACTIVE wait is not a deadlock victim');
is(state_int($node1, 'hot_proof_failclosed_count'),
	$before{hot_proof_failclosed_count},
	'RED-HOT exact ACTIVE proof never enters fail-closed');
my ($read_rc, $read_out, $read_err) =
  $node1->psql('postgres', 'SELECT v FROM cmxh_t WHERE id = 1', timeout => 30);
is($read_rc, 0, 'committed HOT successor remains readable');
is($read_out, '11', 'committed value is visible through the HOT chain');

$pair->stop_pair;
done_testing();
