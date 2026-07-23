#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 408_cluster_current_mx_deadlock.pl
#	  Typed deadlock completion for full-key TX waits.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/408_cluster_current_mx_deadlock.pl
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
use Time::HiRes qw(time usleep);


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
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		   FROM pg_cluster_state
		  WHERE category = '$category' AND key = '$key'});
	return int($value // 0);
}


sub pair_state_int
{
	my ($pair, $category, $key) = @_;
	return state_int($pair->node0, $category, $key)
	  + state_int($pair->node1, $category, $key);
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


sub start_blocking_script
{
	my ($handle, $sql) = @_;
	$handle->query_until(
		qr/CURRENT_MX_FIRED/,
		"\\echo CURRENT_MX_FIRED\n$sql;\nCOMMIT;\n");
}


sub wait_for_tx_wait
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
			return $event eq 'GesTxEnqueueWait';
		},
		$seconds);
}


sub deadlock_logged
{
	my ($pair) = @_;
	for my $node ($pair->node0, $pair->node1)
	{
		my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
		return 1 if $log =~ /deadlock detected/;
	}
	return 0;
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'current_mx_deadlock',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_request_timeout_ms = 60000',
		'cluster.global_dd_interval_ms = 1000',
		'cluster.deadlock_confirm_interval_ms = 500',
		'cluster.lmd_scan_interval_ms = 500',
		'cluster.deadlock_detection_enabled = on',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(2_000_000);
my ($node0, $node1) = ($pair->node0, $pair->node1);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 peers connected');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxd_t',
		'CREATE TABLE cmxd_t (id int PRIMARY KEY, v int)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');
$node0->safe_psql('postgres', 'INSERT INTO cmxd_t VALUES (1, 0), (2, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');

my $victims_before =
  pair_state_int($pair, 'multixact_current', 'deadlock_victim_count');
my $started_at = time();

my $h0 = $node0->background_psql('postgres', on_error_die => 1);
$h0->query_safe('BEGIN');
$h0->query_safe('SELECT v FROM cmxd_t WHERE id = 1 FOR UPDATE');

my $h1 = $node1->background_psql('postgres', on_error_die => 1);
$h1->query_safe('BEGIN');
$h1->query_safe('SELECT v FROM cmxd_t WHERE id = 2 FOR UPDATE');

start_blocking_script($h0, 'UPDATE cmxd_t SET v = v + 1 WHERE id = 2');
ok(wait_for_tx_wait($node0, '%UPDATE cmxd_t SET v = v + 1 WHERE id = 2%', 20),
	'RED-DL node0 publishes the first full-key TX wait');

start_blocking_script($h1, 'UPDATE cmxd_t SET v = v + 1 WHERE id = 1');
ok(wait_for_tx_wait($node1, '%UPDATE cmxd_t SET v = v + 1 WHERE id = 1%', 20),
	'RED-DL node1 publishes the second full-key TX wait');

ok(wait_for(sub { deadlock_logged($pair) }, 30),
	'one ABBA victim receives SQLSTATE 40P01 before the TX timeout');
cmp_ok(time() - $started_at, '<', 60,
	'deadlock resolved before cluster.ges_request_timeout_ms');

eval { $h0->quit };
eval { $h1->quit };

ok(wait_for(
		sub {
			my $sum = $node0->safe_psql('postgres', 'SELECT sum(v) FROM cmxd_t');
			return $sum eq '1';
		},
		20),
	'exactly one survivor commits after victim cleanup');
ok(wait_for(
		sub {
			return pair_state_int($pair, 'lmd', 'wait_edge_count') == 0;
		},
		20),
	'TX wait-for edges are removed after deadlock completion');
cmp_ok(pair_state_int($pair, 'multixact_current', 'deadlock_victim_count'), '>',
	$victims_before, 'typed TX-wait deadlock-victim counter advanced');
is($node1->safe_psql('postgres', 'SELECT 1'), '1',
	'cluster remains usable after deadlock cleanup');

$pair->stop_pair;
done_testing();
