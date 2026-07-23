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
my %observed_waits;
my $entered_current_wait = wait_for(
	sub {
		my $event = $node1->safe_psql(
			'postgres',
			q{SELECT coalesce(wait_event, '')
			     FROM pg_stat_activity
			    WHERE query LIKE '%UPDATE cmxf_t SET v = v + 1 WHERE id = 1%'
			      AND pid <> pg_backend_pid()
			      AND state = 'active'
			    LIMIT 1});
		$observed_waits{$event} = 1 if $event ne '';
		return state_int($node1, 'wait_count') > $wait_before;
	},
	15);
diag('observed writer waits: ' . join(', ', sort keys %observed_waits))
  unless $entered_current_wait;
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

$pair->stop_pair;
done_testing();
