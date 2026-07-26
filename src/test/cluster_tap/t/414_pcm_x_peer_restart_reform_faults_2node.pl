#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 414_pcm_x_peer_restart_reform_faults_2node.pl
#    S3-P0-24 -- assertion-only negative runtime legs for peer-restart
#    reformation.  The environment selector is used only to preserve the two
#    independent old-binary unknown-GUC RED proofs; normal runs execute both.
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
	plan skip_all => 'PCM-X peer restart fault legs require --enable-cluster';
}

my @faults = defined($ENV{PGRAC_PCM_X_REFORM_FAULT_CASE})
  ? (int($ENV{PGRAC_PCM_X_REFORM_FAULT_CASE}))
  : (1, 2);

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
	my ($node, $category, $key) = @_;
	return $node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state "
		  . "WHERE category = '$category' AND key = '$key'");
}

sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = state_value($node, $category, $key);

	die "missing or non-integer $category.$key: [$value]"
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

sub create_aligned_fixture
{
	my ($node0, $node1) = @_;
	my @paths = ('', '', '', '');

	# Consume one relation OID on each private catalog before aligning the
	# two shared-storage relation files.  If background catalog activity has
	# skewed the relNumber frontier, burn exactly that difference on the
	# lagging node and retry, as in the request-boot ABA runtime leg.
	$node0->safe_psql('postgres', 'CREATE TABLE p024_align0 (x int)');
	$node1->safe_psql('postgres', 'CREATE TABLE p024_align1 (x int)');
	for my $attempt (1 .. 8)
	{
		for my $node ($node0, $node1)
		{
			$node->safe_psql('postgres', q{
				CREATE TABLE p024_control (id integer, v bigint NOT NULL)
					WITH (fillfactor = 100);
				CREATE TABLE p024_blocked (id integer, v bigint NOT NULL)
					WITH (fillfactor = 100)
			});
		}
		@paths = (
			$node0->safe_psql('postgres',
				q{SELECT pg_relation_filepath('p024_control')}),
			$node1->safe_psql('postgres',
				q{SELECT pg_relation_filepath('p024_control')}),
			$node0->safe_psql('postgres',
				q{SELECT pg_relation_filepath('p024_blocked')}),
			$node1->safe_psql('postgres',
				q{SELECT pg_relation_filepath('p024_blocked')})
		);
		last if $paths[0] eq $paths[1] && $paths[2] eq $paths[3];

		my ($r0) = $paths[0] =~ /(\d+)$/;
		my ($r1) = $paths[1] =~ /(\d+)$/;
		die "could not parse fixture relation paths: @paths"
		  unless defined($r0) && defined($r1);
		my ($lag, $burn) =
		  $r0 < $r1 ? ($node0, $r1 - $r0) : ($node1, $r0 - $r1);
		$lag->safe_psql(
			'postgres',
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)")
		  if $burn > 0;
		for my $node ($node0, $node1)
		{
			$node->safe_psql(
				'postgres', 'DROP TABLE p024_control, p024_blocked');
		}
	}
	return @paths;
}

sub snapshot
{
	my ($node, $category, $keys) = @_;
	return { map { $_ => state_int($node, $category, $_) } @{$keys} };
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
my @queue_counters = qw(
	pcm_x_queue_enqueue_count
	pcm_x_queue_admit_count
	pcm_x_queue_confirm_count
	pcm_x_queue_promotion_count
	pcm_x_queue_transfer_count
	pcm_x_queue_complete_count
	pcm_x_queue_cancel_count
	pcm_x_queue_revoke_count
	pcm_x_queue_coalesced_count
	pcm_x_queue_wait_count
	pcm_x_queue_full_count
	pcm_x_queue_stale_count
	pcm_x_queue_miss_count
);
my @gcs_accept_counters = qw(
	pcm_x_self_handoff_count
	pcm_x_self_handoff_drain_count
	dedup_pcm_x_stage_count
	dedup_pcm_x_release_count
	dedup_pcm_x_failclosed_count
);

for my $fault (@faults)
{
	die "invalid PGRAC_PCM_X_REFORM_FAULT_CASE=$fault"
	  unless $fault == 1 || $fault == 2;
	my $name = $fault == 1 ? 'pcm_x_live_fault' : 'pcm_x_epoch_fault';
	my $pair = PostgreSQL::Test::ClusterPair->new_pair(
		$name,
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
			"cluster.unsafe_test_pcm_x_peer_reform_fault = $fault",
		]);

	$pair->start_pair;
	usleep(3_000_000);
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	is($pair->node0->safe_psql(
			'postgres',
			'SHOW cluster.unsafe_test_pcm_x_peer_reform_fault'),
		"$fault", "fault $fault startup value is exact (diagnostic only)");
	ok($pair->wait_for_peer_state(0, 1, 'connected', 45),
		"fault $fault survivor sees peer connected");
	ok($pair->wait_for_peer_state(1, 0, 'connected', 45),
		"fault $fault peer sees survivor connected");
	ok(poll_until(
			sub {
				return state_int($node0, 'pcm', 'pcm_x_runtime_state') == 1;
			},
		45),
		"fault $fault survivor runtime is ACTIVE before restart");

	ok(poll_write_ok($node0, 90),
		"fault $fault survivor write gate opens before fixture setup");
	ok(poll_write_ok($node1, 90),
		"fault $fault peer write gate opens before fixture setup");

	my @fixture_paths = create_aligned_fixture($node0, $node1);
	my ($seed_rc, $seed_out, $seed_err) = $node0->psql('postgres', q{
		BEGIN;
		SET LOCAL cluster.gcs_block_local_cache = off;
		INSERT INTO p024_control VALUES (1, 0);
		INSERT INTO p024_blocked VALUES (1, 0);
		COMMIT
	}, timeout => 30);
	my $fixture_ready =
	  $fixture_paths[0] eq $fixture_paths[1]
	  && $fixture_paths[2] eq $fixture_paths[3]
	  && defined($seed_rc) && $seed_rc == 0;
	ok($fixture_ready,
		"fault $fault fixtures have exact shared relfilenodes and cache-off seeds")
	  or diag("fault $fault paths=[@fixture_paths] seed_rc="
		. (defined($seed_rc) ? $seed_rc : '<undef>')
		. " stdout=[$seed_out] stderr=[$seed_err]");
	BAIL_OUT("fault $fault could not establish exact shared fixture files")
	  unless $fixture_ready;

	# Both reads begin from the cache-off INSERT's N state and are the only
	# consumers of their independent relation tags.  First prove that this
	# construction takes the real sole-S self-handoff path while ACTIVE.
	# Only then may the second tag serve as the blocked-path authority probe.
	my ($control_read_rc, $control_read_out, $control_read_err) =
	  $node0->psql(
		'postgres', q{SELECT v FROM p024_control WHERE id = 1}, timeout => 30);
	my ($blocked_read_rc, $blocked_read_out, $blocked_read_err) =
	  $node0->psql(
		'postgres', q{SELECT v FROM p024_blocked WHERE id = 1}, timeout => 30);
	my $control_handoff_before =
	  state_int($node0, 'gcs', 'pcm_x_self_handoff_count');
	my $control_drain_before =
	  state_int($node0, 'gcs', 'pcm_x_self_handoff_drain_count');
	my ($control_write_rc, $control_write_out, $control_write_err) =
	  $node0->psql(
		'postgres',
		q{UPDATE p024_control SET v = v + 1 WHERE id = 1},
		timeout => 30);
	my ($control_handoff_after, $control_drain_after);
	my $control_drained = poll_until(
		sub {
			$control_handoff_after =
			  state_int($node0, 'gcs', 'pcm_x_self_handoff_count');
			$control_drain_after =
			  state_int($node0, 'gcs', 'pcm_x_self_handoff_drain_count');
			return $control_handoff_after > $control_handoff_before
			  && $control_drain_after > $control_drain_before;
		},
		15);
	my $control_value =
	  $node0->safe_psql(
		'postgres', q{SELECT v FROM p024_control WHERE id = 1});
	my $active_precondition =
	  defined($control_read_rc) && $control_read_rc == 0
	  && defined($control_read_out) && $control_read_out eq '0'
	  && defined($blocked_read_rc) && $blocked_read_rc == 0
	  && defined($blocked_read_out) && $blocked_read_out eq '0'
	  && defined($control_write_rc) && $control_write_rc == 0
	  && $control_drained && $control_value eq '1';
	ok($active_precondition,
		"fault $fault ACTIVE control proves sole-S handoff and independent blocked tag")
	  or diag("fault $fault control_read_rc="
		. (defined($control_read_rc) ? $control_read_rc : '<undef>')
		. " control_read=[$control_read_out] control_read_err=[$control_read_err]"
		. " blocked_read_rc="
		. (defined($blocked_read_rc) ? $blocked_read_rc : '<undef>')
		. " blocked_read=[$blocked_read_out] blocked_read_err=[$blocked_read_err]"
		. " control_write_rc="
		. (defined($control_write_rc) ? $control_write_rc : '<undef>')
		. " control_write=[$control_write_out] control_write_err=[$control_write_err]"
		. " handoff=$control_handoff_before->$control_handoff_after"
		. " drain=$control_drain_before->$control_drain_after"
		. " control_value=[$control_value]");
	BAIL_OUT("fault $fault ACTIVE sole-S control precondition did not hold")
	  unless $active_precondition;

	ok(poll_until(
			sub {
				for my $key (@zero_gauges)
				{
					return 0 if state_int($node0, 'pcm', $key) != 0;
				}
				return 1;
			},
			30),
		"fault $fault pre-restart queue and RETIRE gauges are zero");

	my $generation_a =
	  state_int($node0, 'pcm', 'pcm_x_runtime_generation');
	my $blocked_a =
	  state_int($node0, 'pcm', 'pcm_x_queue_recovery_blocked_count');
	my $boot_a = membership_int($node0, 1, 'presented_incarnation');
	my $epoch_a = membership_int($node0, 1, 'admitted_epoch');
	my $binding_a =
	  state_value($node0, 'pcm', 'pcm_x_peer_binding_1');
	my $queue_a = snapshot($node0, 'pcm', \@queue_counters);
	my $gcs_a = snapshot($node0, 'gcs', \@gcs_accept_counters);
	my $log_offset = (-s $node0->logfile) // 0;

	cmp_ok($generation_a, '>', 0,
		"fault $fault captured an ACTIVE generation");
	like($binding_a, qr/\bin_session=\Q$boot_a\E\b/,
		"fault $fault exact old binding contains peer boot A");
	like($binding_a, qr/\bout_session=\Q$boot_a\E\b/,
		"fault $fault exact old outbound binding contains peer boot A");

	$node1->restart;
	ok($pair->wait_for_peer_state(0, 1, 'connected', 60),
		"fault $fault survivor reconnects to restarted peer");
	ok($pair->wait_for_peer_state(1, 0, 'connected', 60),
		"fault $fault restarted peer reconnects to survivor");
	ok(poll_until(
			sub {
				return membership_int($node0, 1, 'presented_incarnation')
				  > $boot_a;
			},
			60),
		"fault $fault survivor authenticates boot B");
	my $boot_b = membership_int($node0, 1, 'presented_incarnation');
	my $epoch_b = membership_int($node0, 1, 'admitted_epoch');
	ok(poll_until(
			sub {
				return state_int($node0, 'pcm', 'pcm_x_runtime_state') == 0;
			},
			60),
		"fault $fault survivor fails closed to RECOVERY_BLOCKED");

	my $generation_b =
	  state_int($node0, 'pcm', 'pcm_x_runtime_generation');
	my $blocked_b =
	  state_int($node0, 'pcm', 'pcm_x_queue_recovery_blocked_count');
	my $binding_b =
	  state_value($node0, 'pcm', 'pcm_x_peer_binding_1');
	my $site_b =
	  state_value($node0, 'pcm', 'pcm_x_runtime_fail_closed_site');
	my $queue_b = snapshot($node0, 'pcm', \@queue_counters);
	my $gcs_b = snapshot($node0, 'gcs', \@gcs_accept_counters);
	my $gauges_b = snapshot($node0, 'pcm', \@zero_gauges);

	cmp_ok($boot_b, '>', $boot_a,
		"fault $fault peer boot B strictly advances");
	is($epoch_b, $epoch_a,
		"fault $fault physical restart itself preserves epoch");
	is($generation_b, $generation_a + 1,
		"fault $fault publishes only the fail-closed generation");
	is($blocked_b, $blocked_a + 1,
		"fault $fault records exactly one recovery-blocked transition");
	is($binding_b, $binding_a,
		"fault $fault leaves the complete old binding byte-exact");
	unlike($binding_b, qr/\b(?:in|out)_session=\Q$boot_b\E\b/,
		"fault $fault never publishes boot B into the old binding");
	like($site_b, qr/\Acluster_pcm_x_convert\.c:\d+\z/,
		"fault $fault records the exact converter fuse site");
	is_deeply($queue_b, $queue_a,
		"fault $fault admits no PCM-X queue work");
	is_deeply($gcs_b, $gcs_a,
		"fault $fault accepts no PCM-X GCS stage, release, or self handoff");
	is_deeply($gauges_b, { map { $_ => 0 } @zero_gauges },
		"fault $fault leaves all queue and RETIRE gauges zero");

	my $label = $fault == 1 ? 'live-slot' : 'epoch-drift';
	my $fault_log = substr(slurp_file($node0->logfile), $log_offset);
	my $consume_count =
	  () = $fault_log =~
	  /unsafe assertion-build PCM-X peer reformation fault consumed: \Q$label\E/g;
	my $fuse_count =
	  () = $fault_log =~
	  /cluster PCM-X runtime fail-closed \(recovery blocked\)/g;
	is($consume_count, 1,
		"fault $fault consumes exactly one $label assertion seam");
	is($fuse_count, 1,
		"fault $fault emits exactly one PCM-X fail-closed record");

	my ($write_rc, $write_out, $write_err) = $node0->psql(
		'postgres',
		q{UPDATE p024_blocked SET v = v + 1 WHERE id = 1},
		timeout => 30);
	isnt($write_rc, 0,
		"fault $fault blocks the sole-S to X write after the fuse")
	  or diag("fault $fault unexpected write stdout=[$write_out]");
	like($write_err // '', qr/(PCM-X|recovery.blocked|53R)/i,
		"fault $fault write rejection names the blocked authority gate");
	is($node0->safe_psql('postgres',
			q{SELECT v FROM p024_blocked WHERE id = 1}),
		'0', "fault $fault rejected write changes no page bytes");
	is_deeply(snapshot($node0, 'pcm', \@queue_counters), $queue_a,
		"fault $fault rejected write cannot enqueue under the blocked gate");
	is_deeply(snapshot($node0, 'gcs', \@gcs_accept_counters), $gcs_a,
		"fault $fault rejected write cannot accept a reply or handoff");

	diag("fault=$fault label=$label generation=$generation_a->$generation_b "
		. "boot=$boot_a->$boot_b epoch=$epoch_a->$epoch_b "
		. "blocked=$blocked_a->$blocked_b site=[$site_b] "
		. "paths=[@fixture_paths] "
		. "control_handoff=$control_handoff_before->$control_handoff_after "
		. "control_drain=$control_drain_before->$control_drain_after");
	$pair->stop_pair;
}

done_testing();
