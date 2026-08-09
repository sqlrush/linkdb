#!/usr/bin/env perl
#-------------------------------------------------------------------------
# R4 D9 real SOURCE backend-exit and exact PGPROC reuse witness.
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

sub bg_start_blocking
{
	my ($handle, $sql) = @_;
	$handle->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

sub handle_int
{
	my ($handle, $sql) = @_;
	my $value = $handle->query_safe($sql);
	return (defined $value && $value ne '') ? int($value) : 0;
}

sub backend_id_for_pid
{
	my ($handle, $pid) = @_;
	return handle_int($handle, qq{
		SELECT s.backend_id FROM pg_stat_get_backend_idset() AS s(backend_id)
		WHERE pg_stat_get_backend_pid(s.backend_id) = $pid});
}

sub wait_for_handle_value
{
	my ($handle, $sql, $want, $seconds) = @_;
	my $deadline = time() + $seconds;
	while (time() < $deadline)
	{
		return 1 if handle_int($handle, $sql) == $want;
		usleep(200_000);
	}
	return 0;
}

sub wait_for_wait_event
{
	my ($handle, $pid, $event, $seconds) = @_;
	my $deadline = time() + $seconds;
	while (time() < $deadline)
	{
		my $seen = $handle->query_safe(qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE pid = $pid AND state = 'active'});
		return 1 if defined $seen && $seen eq $event;
		usleep(200_000);
	}
	return 0;
}

sub wait_for_ctr
{
	my ($node, $want, $seconds) = @_;
	my $deadline = time() + $seconds;
	while (time() < $deadline)
	{
		my $seen = $node->safe_psql('postgres',
			'SELECT ctr FROM r4_d9_exit WHERE id = 1');
		return 1 if defined $seen && $seen eq $want;
		usleep(200_000);
	}
	return 0;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'r4_d9_source_exit',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

$pair->node0->safe_psql('postgres', 'CREATE TABLE r4_d9_exit (id int, ctr int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE r4_d9_exit (id int, ctr int)');
my $path0 = $pair->node0->safe_psql('postgres',
	"SELECT pg_relation_filepath('r4_d9_exit')");
my $path1 = $pair->node1->safe_psql('postgres',
	"SELECT pg_relation_filepath('r4_d9_exit')");
if ($path0 ne $path1)
{
	$pair->stop_pair;
	fail("same-DDL relfilepath identity is required (n0=$path0 n1=$path1)");
	done_testing();
	exit 1;
}
$pair->node0->safe_psql('postgres', 'INSERT INTO r4_d9_exit VALUES (1, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');

my $monitor = $pair->node1->background_psql('postgres', on_error_die => 1);
my $holder = $pair->node0->background_psql('postgres', on_error_die => 1);
$holder->query_safe('BEGIN');
$holder->query_safe('SELECT ctr FROM r4_d9_exit WHERE id = 1 FOR UPDATE');

my $edge_sql = q{SELECT value FROM pg_cluster_state
	WHERE category='lmd' AND key='wait_edge_count'};
my $edge_baseline = handle_int($monitor, $edge_sql);
my $victim = $pair->node1->background_psql('postgres', on_error_die => 0);
my $victim_pid = handle_int($victim, 'SELECT pg_backend_pid()');
my $victim_backend_id = backend_id_for_pid($monitor, $victim_pid);
cmp_ok($victim_backend_id, '>', 0, 'captured victim backend/PGPROC identity');

bg_start_blocking($victim, 'UPDATE r4_d9_exit SET ctr = ctr + 1 WHERE id = 1');
ok(wait_for_wait_event($monitor, $victim_pid, 'GesTxEnqueueWait', 20),
	'victim entered real SOURCE TX enqueue wait');
cmp_ok(handle_int($monitor, $edge_sql), '>', $edge_baseline,
	'victim published an exact WFG edge');
is($monitor->query_safe("SELECT pg_terminate_backend($victim_pid)"), 't',
	'controlled FATAL terminated the waiting backend');
ok(wait_for_handle_value($monitor,
	"SELECT count(*) FROM pg_stat_activity WHERE pid = $victim_pid", 0, 20),
	'victim backend exited before PGPROC reuse');
ok(wait_for_handle_value($monitor, $edge_sql, $edge_baseline, 20),
	'before_shmem_exit removed the exact WFG edge');

my $reuse = $pair->node1->background_psql('postgres', on_error_die => 1);
my $reuse_pid = handle_int($reuse, 'SELECT pg_backend_pid()');
is(backend_id_for_pid($monitor, $reuse_pid), $victim_backend_id,
	'next backend reused the exact released PGPROC slot');
bg_start_blocking($reuse, 'UPDATE r4_d9_exit SET ctr = ctr + 1 WHERE id = 1');
ok(wait_for_wait_event($monitor, $reuse_pid, 'GesTxEnqueueWait', 20),
	'reused PGPROC admitted a fresh SOURCE wait without REENTRANT poison');
$holder->query_safe('ROLLBACK');
ok(wait_for_ctr($pair->node1, '101', 20),
	'reused backend completed after holder release');
ok(wait_for_handle_value($monitor, $edge_sql, $edge_baseline, 20),
	'reused wait also removed its exact WFG edge');

$reuse->quit;
eval { $victim->quit; };
$holder->quit;
$monitor->quit;
$pair->stop_pair;
done_testing();
