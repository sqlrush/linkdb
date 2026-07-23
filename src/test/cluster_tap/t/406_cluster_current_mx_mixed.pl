#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 406_cluster_current_mx_mixed.pl
#	  Derived-own MultiXact with local and remote members.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/406_cluster_current_mx_mixed.pl
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
use PostgreSQL::Test::Utils;
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


sub current_mx_failclosed_logged
{
	my ($node) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	return $log =~ /cross-node write conflict|MULTIXACT row lock with remote member not supported/
	  || $log =~ /canceling statement due to statement timeout/;
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


sub wait_for_authority_wait
{
	my ($node, $seconds) = @_;
	return wait_for(
		sub {
			my $event = $node->safe_psql(
				'postgres',
				q{SELECT coalesce(wait_event, '')
				    FROM pg_stat_activity
				   WHERE query LIKE '%UPDATE cmxm_t SET v = v + 1%'
				     AND pid <> pg_backend_pid()
				     AND state = 'active'
				   LIMIT 1});
			return $event eq 'GcsMultixactMemberProofWait'
			  || $event eq 'GesTxEnqueueWait';
		},
		$seconds);
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'current_mx_mixed',
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

ok(write_retry($node1, 'CREATE EXTENSION IF NOT EXISTS pageinspect'),
	'L1 pageinspect fixture support is writable') or BAIL_OUT('could not install pageinspect');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxm_t',
		'CREATE TABLE cmxm_t (id int, v int)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');
$node0->safe_psql('postgres', 'INSERT INTO cmxm_t VALUES (1, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');

# Two node0 lockers first create a foreign-origin MultiXact.  The first one
# then commits, leaving a marker-only ACTIVE member: the page can transfer to
# node1 without weakening the active-data-ITL PCM boundary.  Node1's compatible
# lock must retain that remote member while creating a requester-owned MXID.
my $remote_seed = $node0->background_psql('postgres', on_error_die => 1);
my $remote_locker = $node0->background_psql('postgres', on_error_die => 1);
my $local_locker;
$remote_seed->query_safe('BEGIN');
$remote_seed->query_safe('SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
$remote_locker->query_safe('BEGIN');
$remote_locker->query_safe('SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
$remote_seed->query_safe('COMMIT');
my $recompose_before = state_int($node1, 'recompose_success_count');
my $mixed_ready = 0;
my $mixed_error = '';
for (1 .. 5)
{
	$local_locker = $node1->background_psql('postgres', on_error_die => 1);
	my $attempt = eval {
		$local_locker->query_safe('BEGIN');
		$local_locker->query_safe(q{SET LOCAL statement_timeout = '10s'});
		$local_locker->query_safe('SELECT v FROM cmxm_t WHERE id = 1 FOR SHARE');
		1;
	};
	if ($attempt)
	{
		$mixed_ready = 1;
		last;
	}
	$mixed_error = $@;
	eval { $local_locker->quit };
	$local_locker = undef;
	usleep(500_000);
}
ok($mixed_ready, 'RED-M compatible remote member composes into a local MultiXact');
if (!$mixed_ready)
{
	diag("mixed-member setup failed closed: $mixed_error");
	ok(current_mx_failclosed_logged($node1),
		'RED-M failure is the current-MultiXact authority floor, not harness infrastructure');
	eval { $remote_locker->query_safe('COMMIT') };
	eval { $local_locker->query_safe('ROLLBACK') } if defined $local_locker;
	eval { $remote_seed->quit };
	eval { $remote_locker->quit };
	eval { $local_locker->quit } if defined $local_locker;
	$pair->stop_pair;
	done_testing();
	exit 0;
}

ok(tuple_has_multixact($node1, 'cmxm_t'),
	'RED-M fixture contains a requester-owned mixed MultiXact');
ok(state_key_exists($node1, 'describe_local_count')
	  && state_key_exists($node1, 'member_proof_ask_count')
	  && state_key_exists($node1, 'wait_count')
	  && state_key_exists($node1, 'recompose_success_count')
	  && state_key_exists($node1, 'foreign_slru_guard_count'),
	'current-MultiXact mixed-path observability keys are exposed');
my $describe_before = state_int($node1, 'describe_local_count');
my $proof_before = state_int($node1, 'member_proof_ask_count');
my $wait_before = state_int($node1, 'wait_count');
my $guard_before = state_int($node1, 'foreign_slru_guard_count');

my $writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($writer, 'UPDATE cmxm_t SET v = v + 1 WHERE id = 1');
ok(wait_for_authority_wait($node1, 15),
	'RED-M exposes the authoritative wait through pg_stat_activity');
ok(wait_for(
		sub { state_int($node1, 'wait_count') > $wait_before },
		15),
	'RED-M entered the authoritative full-key wait path');

$remote_locker->query_safe('COMMIT');
$local_locker->query_safe('COMMIT');
ok(wait_for(
		sub { $node1->safe_psql('postgres', 'SELECT v FROM cmxm_t WHERE id = 1') eq '1' },
		20),
	'derived-own mixed-member UPDATE rejudges and succeeds');

eval { $writer->quit };
eval { $remote_seed->quit };
eval { $remote_locker->quit };
eval { $local_locker->quit };

cmp_ok(state_int($node1, 'describe_local_count'), '>', $describe_before,
	'own immutable descriptor was read at its origin');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>', $proof_before,
	'remote member was resolved at its member origin');
cmp_ok(state_int($node1, 'recompose_success_count'), '>', $recompose_before,
	'requester-owned recomposition completed');
is(state_int($node1, 'foreign_slru_guard_count'), $guard_before,
	'mixed-member path did not decode a foreign MXID locally');

$pair->stop_pair;
done_testing();
