#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 411_stage8_r6_resource_x_identity.pl
#    Stage-8 R6 CURRENT-slice production closure for logical Resource-X
#    identity.  Concurrent backends on one requester node fan in behind the
#    same remote transfer; transport reconnect changes freshness witnesses,
#    not the (BufferTag, requester-node) assertion.  R10 proof keys remain
#    absent throughout.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use IPC::Run qw(finish start timeout);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my @o1_keys = qw(
	remote_install_observed_count
	remote_grant_after_image_count
	remote_image_at_or_after_grant_count
	remote_episode_excluded_no_install
	remote_episode_excluded_missing_grant
	remote_episode_excluded_missing_image
	last_remote_t_image_us
	last_remote_t_grant_us
	last_remote_t_install_us
);

my @terminal_gauges = qw(
	pcm_x_queue_depth
	pcm_x_queue_active_tags
	pcm_x_queue_live_tickets
	pcm_x_queue_live_slots
	pcm_x_local_retire_gate
	pcm_x_local_retire_marker_count
	pcm_x_local_retire_marker_ticket_id
);

my $quad;
my $stopped_lmon_pid = 0;
END
{
	kill('CONT', $stopped_lmon_pid) if $stopped_lmon_pid;
	eval { $quad->stop_quad; } if defined($quad);
}

sub state_value
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$category' AND key='$key'},
		timeout => 10);
	die "missing pg_cluster_state key $category.$key"
		unless defined($value) && $value ne '';
	return $value;
}

sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = state_value($node, $category, $key);
	die "non-integer pg_cluster_state key $category.$key: [$value]"
		unless $value =~ /\A\d+\z/;
	return 0 + $value;
}

sub aggregate_state_int
{
	my ($category, $key) = @_;
	my $total = 0;
	$total += state_int($_, $category, $key) for $quad->nodes;
	return $total;
}

sub current_epoch
{
	my ($node) = @_;
	my $epoch = $node->safe_psql(
		'postgres', 'SELECT new_epoch FROM pg_cluster_reconfig_state');
	return defined($epoch) && $epoch ne '' ? 0 + $epoch : 0;
}

sub proof_contract
{
	my ($node) = @_;
	my $quoted = join(',', map { "'$_'" } @o1_keys);
	return (
		state_value($node, 'pcm', 'resource_x_proof_readiness'),
		0 + $node->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_cluster_state WHERE key IN ($quoted)",
			timeout => 10));
}

sub wait_for_terminal_zero
{
	my ($timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my @last;

	while (1)
	{
		@last = map {
			my $node = $_;
			[ map { state_int($node, 'pcm', $_) } @terminal_gauges ]
		} $quad->nodes;
		my $nonzero = 0;
		for my $snapshot (@last)
		{
			$nonzero += grep { $_ != 0 } @{$snapshot};
		}
		return (1, \@last) if $nonzero == 0;
		last if time() >= $deadline;
		usleep(250_000);
	}
	return (0, \@last);
}

sub diag_terminal_state
{
	my ($label) = @_;
	for my $node_id (0 .. 3)
	{
		my $rows = $quad->node($node_id)->safe_psql(
			'postgres',
			q{SELECT key || '=' || value FROM pg_cluster_state
			  WHERE category='pcm'
			    AND (key LIKE 'pcm_x_ticket_%'
			         OR key LIKE 'pcm_x_ltag_%'
			         OR key = 'pcm_x_terminal_last_note')
			  ORDER BY key},
			timeout => 5);
		diag("$label node$node_id terminal state: [$rows]");
	}
}

sub write_file
{
	my ($path, $contents) = @_;
	open(my $fh, '>', $path) or die "open $path: $!";
	print {$fh} $contents;
	close($fh) or die "close $path: $!";
}

sub arm_transfer_window
{
	my ($node) = @_;
	$node->safe_psql(
		'postgres',
		q{ALTER SYSTEM SET cluster.injection_points =
		  'cluster-gcs-xfer-copy-drop-window:sleep:2000000';
		  SELECT pg_reload_conf()},
		timeout => 10);
	usleep(750_000);
}

sub disarm_transfer_window
{
	my ($node) = @_;
	$node->safe_psql(
		'postgres',
		q{ALTER SYSTEM RESET cluster.injection_points;
		  SELECT pg_reload_conf()},
		timeout => 10);
	usleep(500_000);
}

sub run_fanin
{
	my ($node, $table, $label, $clients, $id_base) = @_;
	my $start_at = $node->safe_psql(
		'postgres',
		q{SELECT (clock_timestamp() + interval '3 seconds')::text});
	my $script_dir = PostgreSQL::Test::Utils::tempdir();
	my $script = "$script_dir/$label.sql";
	write_file(
		$script,
		"BEGIN;\n"
		  . "SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
		  . "(TIMESTAMPTZ '$start_at' - clock_timestamp()))));\n"
		  # INSERT goes directly to the FSM-selected heap page and requests X
		  # before inspecting an existing tuple.  UPDATE would first run the
		  # unrelated CR/TT path over a peer's uncommitted same-page ITL.
		  . "INSERT INTO $table VALUES ($id_base + :client_id, 1);\n"
		  # Keep the stagger inside the database transaction.  pgbench does not
		  # wrap a multi-statement custom script in an implicit transaction.
		  . "SELECT pg_sleep(:client_id * 0.75);\n"
		  . "COMMIT;\n");

	my %run = (stdout => '', stderr => '', timed_out => 0);
	my @command = (
		$node->installed_command('pgbench'),
		'-n', '-c', "$clients", '-j', "$clients", '-t', '1',
		'--max-tries=1', '-f', $script,
		'-h', $node->host, '-p', $node->port, 'postgres');
	$run{handle} = start(\@command, '<', \undef, '>', \$run{stdout},
		'2>', \$run{stderr}, timeout(60));
	my $finished = eval { finish($run{handle}); 1 };
	unless ($finished)
	{
		$run{timed_out} = 1;
		$run{finish_error} = $@;
		eval { $run{handle}->kill_kill; };
	}
	$run{result} = eval { $run{handle}->result(0) };
	$run{result} = -1 unless defined($run{result});
	($run{transactions}) =
		$run{stdout} =~ /number of transactions actually processed:\s+(\d+)/;
	$run{transactions} //= 0;
	my @errors = $run{stderr} =~ /^.*(?:ERROR|FATAL|PANIC):.*$/mg;
	$run{errors} = scalar(@errors);
	diag("$label result=$run{result} timed_out=$run{timed_out} "
		. "transactions=$run{transactions} errors=$run{errors} "
		. 'finish_error=[' . ($run{finish_error} // '') . '] '
		. "stderr=[$run{stderr}]");
	return \%run;
}

$quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'stage8_r6_identity',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.read_scache = on',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 5000',
		'cluster.gcs_block_retransmit_max_retries = 8',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.interconnect_heartbeat_interval_ms = 500',
		'cluster.interconnect_recv_timeout_ms = 30000',
	]);

$quad->start_quad;
usleep(3_000_000);

for my $from (0 .. 3)
{
	is($quad->node($from)->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$from accepts SQL");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 60),
			"L1 node$from sees node$to connected");
	}
	my ($readiness, $o1_count) = proof_contract($quad->node($from));
	is($readiness, 'UNAVAILABLE_PROOF_KIND',
		"L1 node$from reports explicit pre-R10 proof unavailability");
	is($o1_count, 0,
		"L1 node$from exposes none of the nine R10-owned O1 keys");
}

my $table = 'stage8_r6_resource_x_identity';
for my $node ($quad->nodes)
{
	$node->safe_psql(
		'postgres',
		"CREATE TABLE $table(id integer, v bigint NOT NULL) "
		  . 'WITH (fillfactor=100)',
		timeout => 30);
}
my @paths = map {
	$_->safe_psql('postgres', "SELECT pg_relation_filepath('$table')")
} $quad->nodes;
is(scalar(grep { $_ eq $paths[0] } @paths), 4,
	'L2 all nodes map the fixture to the same shared relation identity');

$quad->node1->safe_psql(
	'postgres',
	"INSERT INTO $table SELECT g, 0 FROM generate_series(1, 16) g; CHECKPOINT",
	timeout => 30);
is($quad->node1->safe_psql('postgres',
		"SELECT count(*) FROM $table"), '16',
	'L2 remote node1 owns a committed sixteen-row heap image');
is($quad->node1->safe_psql('postgres', q{
	SELECT count(DISTINCT split_part(trim(both '()' from ctid::text), ',', 1))
	FROM stage8_r6_resource_x_identity
}), '1', 'L2 all client rows occupy one heap BufferTag');

my $coalesced_before = state_int(
	$quad->node0, 'pcm', 'pcm_x_queue_coalesced_count');
my $enqueue_before = aggregate_state_int('pcm', 'pcm_x_queue_enqueue_count');
my $transfer_before = aggregate_state_int('pcm', 'pcm_x_queue_transfer_count');
arm_transfer_window($quad->node1);
my $first = run_fanin($quad->node0, $table, 'first_fanin', 2, 100);
disarm_transfer_window($quad->node1);

is($first->{timed_out}, 0, 'L3 first fan-in met its hard deadline');
is($first->{result}, 0, 'L3 first fan-in process exited successfully');
is($first->{errors}, 0, 'L3 first fan-in surfaced zero client errors');
is($first->{transactions}, 2, 'L3 both first-wave backends committed once');
cmp_ok(
	state_int($quad->node0, 'pcm', 'pcm_x_queue_coalesced_count')
	  - $coalesced_before,
	'>=', 1,
	'L3 two same-node backends produced one leader and a joined follower');
cmp_ok(
	aggregate_state_int('pcm', 'pcm_x_queue_enqueue_count') - $enqueue_before,
	'>', 0,
	'L3 the fan-in submitted production cluster work');
cmp_ok(
	aggregate_state_int('pcm', 'pcm_x_queue_transfer_count') - $transfer_before,
	'>', 0,
	'L3 the fan-in completed a real remote-holder transfer');
my ($first_drained, $first_snapshots) = wait_for_terminal_zero(30);
ok($first_drained, 'L3 first-wave asynchronous queue retirement drained');
unless ($first_drained)
{
	diag('L3 first-wave terminal gauges: ' . join(' | ', map {
		my $node_id = $_;
		"node$node_id=" . join(',', @{$first_snapshots->[$node_id]})
	} (0 .. 3)));
	diag_terminal_state('L3 first-wave');
}
is($quad->node0->safe_psql(
		'postgres', "SELECT sum(v) FROM $table", timeout => 10), '2',
	'L3 committed value equals the first-wave transaction count');

my $epoch_before = current_epoch($quad->node0);
my $reconnect_before = 0 + $quad->node0->safe_psql(
	'postgres',
	q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id=1});
my $old_start = $quad->node1->safe_psql(
	'postgres', 'SELECT pg_postmaster_start_time()');
$stopped_lmon_pid = 0 + $quad->node1->safe_psql(
	'postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type='lmon'});
cmp_ok($stopped_lmon_pid, '>', 1,
	'L4 located node1 LMON for a transport-only reconnect');
kill('STOP', $stopped_lmon_pid)
	or die "SIGSTOP node1 LMON $stopped_lmon_pid failed: $!";
my $saw_reconnect = 0;
my $reconnect_after = $reconnect_before;
my $reconnect_deadline = time() + 15;
while (time() < $reconnect_deadline)
{
	$reconnect_after = 0 + $quad->node0->safe_psql(
		'postgres',
		q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id=1});
	if ($reconnect_after > $reconnect_before)
	{
		$saw_reconnect = 1;
		last;
	}
	usleep(250_000);
}
kill('CONT', $stopped_lmon_pid)
	or die "SIGCONT node1 LMON $stopped_lmon_pid failed: $!";
$stopped_lmon_pid = 0;
my $new_start = $quad->node1->safe_psql(
	'postgres', 'SELECT pg_postmaster_start_time()');
ok($saw_reconnect,
	'L4 node0 closed the stale CONTROL connection generation');
ok($quad->wait_for_peer_state(0, 1, 'connected', 30)
	&& $quad->wait_for_peer_state(1, 0, 'connected', 30),
	'L4 both CONTROL directions converged after reconnect');
ok($quad->wait_for_membership_count(0, 4, 60),
	'L4 reconnect stayed inside the same four-node membership');
is(current_epoch($quad->node0), $epoch_before,
	'L4 transport reconnect did not change the formation epoch');
is($new_start, $old_start,
	'L4 transport reconnect did not restart node1');

is($quad->node1->safe_psql(
		'postgres',
		"INSERT INTO $table VALUES (150, 1) RETURNING v",
		timeout => 30), '1',
	'L4 node1 retook the same page through the fresh transport');
my $coalesced_reconnect_before = state_int(
	$quad->node0, 'pcm', 'pcm_x_queue_coalesced_count');
arm_transfer_window($quad->node1);
my $second = run_fanin($quad->node0, $table, 'reconnect_fanin', 4, 200);
disarm_transfer_window($quad->node1);

is($second->{timed_out}, 0, 'L5 reconnect fan-in met its hard deadline');
is($second->{result}, 0, 'L5 reconnect fan-in process exited successfully');
is($second->{errors}, 0, 'L5 reconnect fan-in surfaced zero client errors');
is($second->{transactions}, 4,
	'L5 every reconnect-wave backend committed once');
cmp_ok(
	state_int($quad->node0, 'pcm', 'pcm_x_queue_coalesced_count')
	  - $coalesced_reconnect_before,
	'>=', 3,
	'L5 four backends still join the same requester-node assertion after reconnect');
my ($drained, $snapshots) = wait_for_terminal_zero(30);
ok($drained,
	'L5 requester membership, legacy carrier and transport gauges drained');
diag_terminal_state('L5 final') unless $drained;
for my $node_id (0 .. 3)
{
	for my $key_id (0 .. $#terminal_gauges)
	{
		is($snapshots->[$node_id][$key_id], 0,
			"L5 node$node_id final $terminal_gauges[$key_id] is zero");
	}
}
is($quad->node0->safe_psql(
		'postgres', "SELECT sum(v) FROM $table", timeout => 10), '7',
	'L5 exact contents include both fan-in waves and the ownership handback');
is($quad->node0->safe_psql(
		'postgres',
		"SELECT count(*) || '/' || count(DISTINCT id) FROM $table",
		timeout => 10), '23/23',
	'L5 every direct-X insert is present exactly once');

for my $node_id (0 .. 3)
{
	my ($readiness, $o1_count) = proof_contract($quad->node($node_id));
	is($readiness, 'UNAVAILABLE_PROOF_KIND',
		"L6 node$node_id proof readiness stayed unavailable after real transfers");
	is($o1_count, 0,
		"L6 node$node_id still exposes none of the nine O1 keys");
}
$quad->stop_quad;
$quad = undef;
done_testing();
