#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 413_pcm_x_peer_restart_reform_2node.pl
#    S3-P0-06 -- a quiescent survivor reforms its PCM-X peer binding after
#    an ordinary same-epoch remote postmaster restart.
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

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'PCM-X peer restart reformation requires --enable-cluster';
}

sub poll_until
{
	my ($fn, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		return 1 if $fn->();
		usleep(100_000);
	}
	return 0;
}

sub state_value
{
	my ($node, $key) = @_;
	return $node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state "
		  . "WHERE category = 'pcm' AND key = '$key'");
}

sub state_int
{
	my ($node, $key) = @_;
	my $value = state_value($node, $key);

	die "missing or non-integer pcm.$key: [$value]"
	  unless defined($value) && $value =~ /\A\d+\z/;
	return int($value);
}

sub membership_int
{
	my ($node, $node_id, $field) = @_;

	die "invalid membership field $field"
	  unless $field eq 'presented_incarnation' || $field eq 'admitted_epoch';
	return int($node->safe_psql(
		'postgres',
		"SELECT $field FROM pg_cluster_membership WHERE node_id = $node_id"));
}

sub poll_write_ok
{
	my ($node, $seconds) = @_;

	return poll_until(
		sub {
			my ($rc, $out) =
			  $node->psql('postgres', 'SELECT txid_current() > 0');
			return defined($rc) && $rc == 0 && defined($out) && $out eq 't';
		},
		$seconds);
}

my @zero_gauges = qw(
	pcm_x_queue_depth
	pcm_x_queue_active_tags
	pcm_x_queue_live_tickets
	pcm_x_queue_live_slots
	pcm_x_local_retire_gate
	pcm_x_local_retire_marker_count
	pcm_x_local_retire_marker_ticket_id
);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pcm_x_peer_reform',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 1',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.block_self_contained = on',
	]);
$pair->start_pair;
usleep(3_000_000);

my ($node0, $node1) = ($pair->node0, $pair->node1);
ok($pair->wait_for_peer_state(0, 1, 'connected', 45),
	'L0 survivor sees peer connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 45),
	'L0 peer sees survivor connected');
ok(poll_until(
		sub {
			return state_int($node0, 'pcm_x_runtime_state') == 1;
		},
		45),
	'L0 survivor PCM-X runtime is ACTIVE');

my $generation_a = state_int($node0, 'pcm_x_runtime_generation');
my $boot_a = membership_int($node0, 1, 'presented_incarnation');
my $epoch_a = membership_int($node0, 1, 'admitted_epoch');
my $blocked_a = state_int($node0, 'pcm_x_queue_recovery_blocked_count');
my $reset_a = state_int($node0, 'pcm_x_queue_activating_reset_count');
my $site_a = state_value($node0, 'pcm_x_runtime_fail_closed_site');
my %gauges_a = map { $_ => state_int($node0, $_) } @zero_gauges;
my $survivor_log_offset = (-s $node0->logfile) // 0;

cmp_ok($generation_a, '>', 0, 'L0 survivor published generation A');
cmp_ok($boot_a, '>', 0, 'L0 peer boot A is nonzero');
is($site_a, '(none)', 'L0 survivor starts without a PCM-X fuse site');
is_deeply(\%gauges_a, { map { $_ => 0 } @zero_gauges },
	'L0 survivor allocator, queue, and RETIRE gauges are quiescent');

$node1->restart;
ok($pair->wait_for_peer_state(0, 1, 'connected', 60),
	'L1 survivor reconnects to restarted peer');
ok($pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1 restarted peer reconnects to survivor');
ok(poll_until(
		sub {
			return membership_int($node0, 1, 'presented_incarnation') > $boot_a;
		},
		60),
	'L1 survivor authenticates a strictly newer peer incarnation');
my $boot_b = membership_int($node0, 1, 'presented_incarnation');
my $epoch_b = membership_int($node0, 1, 'admitted_epoch');

ok(poll_until(
		sub {
			return state_int($node0, 'pcm_x_runtime_state') == 1
			  && state_int($node0, 'pcm_x_runtime_generation')
			  == $generation_a + 1;
		},
		60),
	'L1 quiescent same-epoch restart reforms ACTIVE generation exactly once');

my $generation_b = state_int($node0, 'pcm_x_runtime_generation');
my $blocked_b = state_int($node0, 'pcm_x_queue_recovery_blocked_count');
my $reset_b = state_int($node0, 'pcm_x_queue_activating_reset_count');
my $site_b = state_value($node0, 'pcm_x_runtime_fail_closed_site');
my %gauges_b = map { $_ => state_int($node0, $_) } @zero_gauges;

cmp_ok($boot_b, '>', $boot_a, 'L2 peer boot B strictly advances');
is($epoch_b, $epoch_a, 'L2 fast restart preserves the admitted epoch');
is($generation_b, $generation_a + 1,
	'L2 survivor gate generation advances by exactly one');
is($blocked_b, $blocked_a,
	'L2 peer restart does not increment the recovery-blocked counter');
is($reset_b, $reset_a,
	'L2 peer restart is not an abandoned-ACTIVATING reset');
is($site_b, '(none)', 'L2 survivor remains unfused after peer restart');
is_deeply(\%gauges_b, { map { $_ => 0 } @zero_gauges },
	'L2 reformation leaves allocator, queue, and RETIRE gauges quiescent');
my $survivor_write_poll_started = time();
my $survivor_write_ok = poll_write_ok($node0, 60);
my $survivor_write_wait = time() - $survivor_write_poll_started;
ok($survivor_write_ok,
	'L3 survivor write gate remains usable after reformation');
my $peer_write_poll_started = time();
my $peer_write_ok = poll_write_ok($node1, 90);
my $peer_write_wait = time() - $peer_write_poll_started;
ok($peer_write_ok, 'L3 restarted peer write gate becomes usable');
my $peer_membership_state = $node1->safe_psql(
	'postgres',
	q{SELECT state FROM pg_cluster_membership WHERE node_id = 1});
is($peer_membership_state, 'member',
	'L3 restarted peer is MEMBER when its write gate opens');

my $survivor_log = substr(slurp_file($node0->logfile), $survivor_log_offset);
unlike($survivor_log,
	qr/cluster PCM-X runtime fail-closed \(recovery blocked\)/,
	'L3 survivor log has no PCM-X fail-closed transition');

diag("survivor generation $generation_a->$generation_b, "
	. "peer incarnation $boot_a->$boot_b, epoch $epoch_a->$epoch_b, "
	. "recovery_blocked $blocked_a->$blocked_b, activating_reset $reset_a->$reset_b, "
	. sprintf('survivor_write_wait=%.3fs peer_write_wait=%.3fs peer_state=%s',
		$survivor_write_wait, $peer_write_wait, $peer_membership_state));

$pair->stop_pair;
done_testing();
