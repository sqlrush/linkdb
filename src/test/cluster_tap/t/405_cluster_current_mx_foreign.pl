#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 405_cluster_current_mx_foreign.pl
#	  Foreign-origin MultiXact current-DML authority gate.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/405_cluster_current_mx_foreign.pl
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

my @stopped_stats_lms_pids;

END
{
	if (@stopped_stats_lms_pids)
	{
		kill('CONT', @stopped_stats_lms_pids);
		@stopped_stats_lms_pids = ();
	}
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


sub tuple_has_multixact
{
	my ($node, $relation) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(bool_or(
		     'HEAP_XMAX_IS_MULTI' = ANY(f.raw_flags)), false)
		   FROM heap_page_items(get_raw_page('$relation', 0)) AS h
		   CROSS JOIN LATERAL
		     heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f}) eq 't';
}

sub marker_counts
{
	my ($node, $relation, $lp) = @_;
	return $node->safe_psql(
		'postgres',
		qq{
WITH page AS (
  SELECT get_raw_page('$relation', 0) AS raw
),
decoded AS (
  SELECT raw, (page_header(raw)).special AS special,
         (SELECT t_xmax::text::bigint
            FROM heap_page_items(raw)
           WHERE lp = $lp) AS current_mx
    FROM page
),
slots AS (
  SELECT get_byte(raw, special + i * 48 + 6) AS flags,
         get_byte(raw, special + i * 48)::bigint
           + get_byte(raw, special + i * 48 + 1)::bigint * 256
           + get_byte(raw, special + i * 48 + 2)::bigint * 65536
           + get_byte(raw, special + i * 48 + 3)::bigint * 16777216 AS slot_xid,
         current_mx
    FROM decoded, generate_series(0, 7) AS g(i)
)
SELECT count(*) FILTER (WHERE flags = 8)::text || '|' ||
       count(*) FILTER (WHERE flags = 8 AND slot_xid = current_mx)::text
  FROM slots});
}


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


sub wait_for_any_wait
{
	my ($node, $query_like, $seconds) = @_;
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
			return $event eq 'GcsMultixactDescribeWait'
			  || $event eq 'GcsMultixactMemberProofWait'
			  || $event eq 'GesTxEnqueueWait';
		},
		$seconds);
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


sub start_blocking
{
	my ($handle, $sql) = @_;
	$handle->query_until(qr/CURRENT_MX_FIRED/, "\\echo CURRENT_MX_FIRED\n$sql;\n");
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'current_mx_foreign',
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
	]);
$pair->start_pair;
usleep(2_000_000);
my ($node0, $node1) = ($pair->node0, $pair->node1);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 peers connected');

is($node0->safe_psql(
		'postgres',
		q{SELECT string_agg(node_id::text || ':' || collection_status,
		                    ',' ORDER BY node_id)
		    FROM pg_stat_gcluster_multixact_current}),
	'0:OK,1:OK',
	'L1 global current-MultiXact stats collects both reachable nodes');

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

# Rule 4/F12: the stats fan-out is a distinct network blocking point, so its
# wait event must be observable at runtime rather than only present in the
# catalog.  Run this only after stripe activation converges: pausing the DATA
# workers during cold formation would perturb membership rather than isolate
# the stats CV.  SIGSTOP both node1 LMS shards so the kind-8 request cannot be
# served until the parent resumes them; the control-plane heartbeat remains
# alive, so this is a reply stall, not a node-death test.
@stopped_stats_lms_pids = split(
	/\n/,
	$node1->safe_psql(
		'postgres',
		q{SELECT pid
		    FROM pg_stat_activity
		   WHERE backend_type IN ('lms', 'lms worker')
		   ORDER BY pid}));
is(scalar(@stopped_stats_lms_pids), 2,
	'L1 stats wait fixture resolves both remote LMS data shards');
is(kill('STOP', @stopped_stats_lms_pids),
	scalar(@stopped_stats_lms_pids),
	'L1 stats wait fixture pauses both remote LMS data shards');
my $stats_probe = $node0->background_psql('postgres', on_error_die => 1);
my %stats_wait_seen;
start_blocking(
	$stats_probe,
	'SELECT count(*) FROM pg_stat_gcluster_multixact_current /* stats_wait_probe */');
my $stats_wait_observed = wait_for(
	sub {
		my $event = $node0->safe_psql(
			'postgres',
			q{SELECT coalesce(wait_event, '')
			    FROM pg_stat_activity
			   WHERE query LIKE '%stats_wait_probe%'
			     AND pid <> pg_backend_pid()
			     AND state = 'active'
			   LIMIT 1});
		$stats_wait_seen{$event eq '' ? '<none>' : $event}++;
		return $event eq 'GcsMultixactStatsWait';
	},
	5);
diag('stats wait events observed: '
	  . join(', ', sort keys %stats_wait_seen))
  unless $stats_wait_observed;
ok($stats_wait_observed,
	'L1 global stats RPC exposes GcsMultixactStatsWait at runtime');
my $stats_lms_resumed = kill('CONT', @stopped_stats_lms_pids);
is($stats_lms_resumed, scalar(@stopped_stats_lms_pids),
	'L1 stats wait fixture resumes both remote LMS data shards');
@stopped_stats_lms_pids = ()
  if $stats_lms_resumed == scalar(@stopped_stats_lms_pids);
$stats_probe->query_safe('SELECT 1');
$stats_probe->quit;

ok(write_retry($node0, 'CREATE EXTENSION IF NOT EXISTS pageinspect'),
	'L1 pageinspect fixture support is writable') or BAIL_OUT('could not install pageinspect');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxf_t',
		'CREATE TABLE cmxf_t (id int, v int)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');

$node0->safe_psql('postgres', 'INSERT INTO cmxf_t VALUES (1, 0), (2, 0), (3, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');

my $locker1 = $node0->background_psql('postgres', on_error_die => 1);
my $locker2 = $node0->background_psql('postgres', on_error_die => 1);
$locker1->query_safe('BEGIN');
$locker1->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');
$locker2->query_safe('BEGIN');
$locker2->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');

$locker1->query_safe('COMMIT');
ok(tuple_has_multixact($node0, 'cmxf_t'),
	'RED-F fixture retains a real foreign-origin MultiXact with one ACTIVE member');
ok(state_key_exists($node1, 'describe_remote_ask_count')
	  && state_key_exists($node1, 'member_proof_ask_count')
	  && state_key_exists($node1, 'wait_count')
	  && state_key_exists($node1, 'foreign_slru_guard_count'),
	'current-MultiXact observability keys are exposed');
my $describe_before = state_int($node1, 'describe_remote_ask_count');
my $proof_before = state_int($node1, 'member_proof_ask_count');
my $wait_before = state_int($node1, 'wait_count');
my $guard_before = state_int($node1, 'foreign_slru_guard_count');

my $writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($writer, 'UPDATE cmxf_t SET v = v + 1 WHERE id = 1');
ok(wait_for_any_wait(
		$node1,
		'%UPDATE cmxf_t SET v = v + 1 WHERE id = 1%',
		15),
	'RED-F exposes the authoritative wait through pg_stat_activity');
my $entered_current_wait = wait_for(
	sub {
		return state_int($node1, 'wait_count') > $wait_before;
	},
	15);
ok($entered_current_wait,
	'RED-F entered authoritative foreign-MX wait path');

$locker2->query_safe('COMMIT');
ok(wait_for(
		sub { $node1->safe_psql('postgres', 'SELECT v FROM cmxf_t WHERE id = 1') eq '1' },
		20),
	'foreign lock-only MultiXact rejudges and UPDATE succeeds');
eval { $writer->quit };
eval { $locker1->quit };
eval { $locker2->quit };

cmp_ok(state_int($node1, 'describe_remote_ask_count'), '>', $describe_before,
	'foreign descriptor RPC counter advanced');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>', $proof_before,
	'foreign member-proof RPC counter advanced');
is(state_int($node1, 'foreign_slru_guard_count'), $guard_before,
	'positive path never attempted requester-local foreign SLRU decode');

# A MultiXact marker is only a lossy page hint.  Repeatedly extending one
# tuple creates a fresh MXID each time and exhausts the fixed page marker
# slots, but that pressure must not evict the completed DATA ITL anchor still
# referenced by tuple.t_itl_slot_idx.  Before the spec-3.6b closure, the sixth
# locker on this fixture failed with "cluster TT slot recycled for xmin".
my @marker_lockers;
my $marker_pressure_ok = 1;
my $marker_pressure_err = '';

for my $locker_no (1 .. 12)
{
	my $locker = $node0->background_psql('postgres', on_error_die => 1);
	push @marker_lockers, $locker;
	my $locked = eval {
		$locker->query_safe('BEGIN');
		$locker->query_safe('SELECT v FROM cmxf_t WHERE id = 2 FOR SHARE');
		1;
	};
	if (!$locked)
	{
		$marker_pressure_ok = 0;
		$marker_pressure_err = $@ // "locker $locker_no failed without an error";
		last;
	}
}
diag($marker_pressure_err) unless $marker_pressure_ok;
ok($marker_pressure_ok,
	'marker pressure admits 12 concurrent lockers without recycling the DATA anchor');
is(marker_counts($node0, 'cmxf_t', 2), '1|1',
	'repeated MXID extension recasts one marker in place without consuming ITL capacity');

for my $locker (reverse @marker_lockers)
{
	eval { $locker->query_safe('COMMIT') };
	eval { $locker->quit };
}

# Deterministic markerless overflow.  One initial DATA slot + one live
# single-lock slot + six independent DATA writers fill all eight ITL slots.
# B then composes the first MultiXact with no safe marker slot.  Correctness
# must continue through the value-derived origin and on-demand
# describe/member-proof authority; the marker is optional.
ok(mirrored_coincident_create(
		$node0, $node1, 'cmxm_t',
		'CREATE TABLE cmxm_t (id int, v int)'),
	'markerless fixture relation identity coincides')
  or BAIL_OUT('could not create markerless coincident relation');
$node0->safe_psql(
	'postgres',
	'INSERT INTO cmxm_t SELECT g, 0 FROM generate_series(1, 7) AS g');
$node0->safe_psql('postgres', 'CHECKPOINT');

my $markerless_a = $node0->background_psql('postgres', on_error_die => 1);
$markerless_a->query_safe('BEGIN');
$markerless_a->query_safe('SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
for my $id (2 .. 7)
{
	$node0->safe_psql(
		'postgres',
		"UPDATE cmxm_t SET v = v + 1 WHERE id = $id");
}

my $markerless_b = $node0->background_psql('postgres', on_error_die => 1);
$markerless_b->query_safe('BEGIN');
$markerless_b->query_safe('SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
is(marker_counts($node0, 'cmxm_t', 1), '0|0',
	'full DATA/active-lock page publishes a real markerless current MultiXact');

$markerless_a->query_safe('COMMIT');
my $markerless_describe_before
	= state_int($node1, 'describe_remote_ask_count');
my $markerless_proof_before
	= state_int($node1, 'member_proof_ask_count');
my $markerless_recompose_before
	= state_int($node1, 'recompose_success_count');
my $markerless_c;
my $markerless_c_ready = 0;
for my $attempt (1 .. 10)
{
	my $candidate
		= $node1->background_psql('postgres', on_error_die => 1);
	my $locked = eval {
		$candidate->query_safe('BEGIN');
		$candidate->query_safe(
			'SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
		1;
	};
	if ($locked)
	{
		$markerless_c = $candidate;
		$markerless_c_ready = 1;
		last;
	}
	diag("markerless remote locker attempt $attempt: " . ($@ // 'unknown'));
	eval { $candidate->quit };
	usleep(500_000);
}
ok($markerless_c_ready,
	'markerless remote locker converges through transient remaster windows')
  or BAIL_OUT('markerless remote locker did not converge');
cmp_ok(state_int($node1, 'describe_remote_ask_count'), '>',
	$markerless_describe_before,
	'markerless remote locker asks the value-derived MXID origin');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>',
	$markerless_proof_before,
	'markerless remote locker obtains authoritative member proofs');
cmp_ok(state_int($node1, 'recompose_success_count'), '>',
	$markerless_recompose_before,
	'markerless remote locker recomposes successfully');

$markerless_c->query_safe('COMMIT');
$markerless_b->query_safe('COMMIT');
$markerless_c->quit;
$markerless_b->quit;
$markerless_a->quit;

$pair->stop_pair;
done_testing();
