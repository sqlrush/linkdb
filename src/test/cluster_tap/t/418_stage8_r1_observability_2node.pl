#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 418_stage8_r1_observability_2node.pl
#    Stage-8 R1 two-node observation-surface behavior.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Digest::SHA;
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use IPC::Run ();
use Math::BigInt;
use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(time usleep);

my @RESULT_LABELS = qw(
  ok duplicate retired not_found stale no_capacity counter_exhausted
  not_ready busy bad_state corrupt invalid gate_retry barrier_closed);
my @RESULT_KEYS = map { 'pcm_x_acquire_result_' . $_ . '_count' } @RESULT_LABELS;
my @BUCKET_KEYS =
  map { sprintf('pcm_x_acquire_success_us_le_2p%02d_count', $_) } 0 .. 31;
my @O2_KEYS = (
	'pcm_x_acquire_started_count',
	'pcm_x_acquire_active_count',
	'pcm_x_acquire_exception_count',
	@RESULT_KEYS,
	@BUCKET_KEYS,
	'pcm_x_acquire_success_us_overflow_count');
my @LMON_KEYS = qw(lmon_timed_duty_sample_count lmon_total_iter_us);
my @UNDO_KEYS = qw(
  segment_observation_status segment_allocated_count
  segment_allocated_high_water segment_effective_cap);
my @GCS_KEYS = qw(pi_master_metadata_retire_count);
my @R4_KEYS = qw(
  cr_route_started_count cr_holder_full_count cr_holder_retry_count
  cr_holder_failclosed_count undo_data_fetch_served_count
  undo_data_fetch_denied_count tx_resolve_unknown_count
  tx_resolve_in_progress_count tx_resolve_prepared_count
  tx_resolve_committed_count tx_resolve_aborted_count
  multi_resolve_served_count multi_resolve_unknown_count
  slot_capacity_retry_count);
my @UNDO_STATUS = qw(
  READY UNAVAILABLE_INVALID_OWNER UNAVAILABLE_IO_ERROR
  UNAVAILABLE_INVALID_HEADER);
my @O1_KEYS = qw(
  remote_install_observed_count
  remote_grant_after_image_count
  remote_image_at_or_after_grant_count
  remote_episode_excluded_no_install
  remote_episode_excluded_missing_grant
  remote_episode_excluded_missing_image
  last_remote_t_image_us
  last_remote_t_grant_us
  last_remote_t_install_us);
my $U64_MAX = Math::BigInt->new('18446744073709551615');

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'stage8_r1_obs2',
	quorum_voting_disks => 3,
	shared_data => 1,
	data_port_span => 2,
	extra_conf => [
		'autovacuum = off',
		'synchronous_commit = off',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.online_join = on',
		'cluster.read_scache = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.ges_bast = on',
		'cluster.gcs_block_local_cache = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.ges_request_timeout_ms = 30000' ]);

my $stopped_lmon_pid = 0;
my $injection_armed = 0;

sub wait_for
{
	my ($condition, $timeout_s, $step_us) = @_;
	$step_us //= 250_000;
	my $deadline = time() + $timeout_s;
	while (time() < $deadline)
	{
		return 1 if $condition->();
		usleep($step_us);
	}
	return $condition->() ? 1 : 0;
}

sub is_u64
{
	my ($value) = @_;
	return 0 unless defined($value) && $value =~ /\A\d+\z/;
	return Math::BigInt->new($value)->bcmp($U64_MAX) <= 0;
}

sub state_val
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$category' AND key='$key'});
	die "missing pg_cluster_state key $category.$key"
	  unless defined($value) && length($value);
	return $value;
}

sub state_num
{
	my ($node, $category, $key) = @_;
	my $value = state_val($node, $category, $key);
	die "non-u64 pg_cluster_state key $category.$key: [$value]"
	  unless is_u64($value);
	return 0 + $value;
}

sub key_count
{
	my ($node, $category, $key) = @_;
	return 0 + $node->safe_psql(
		'postgres',
		qq{SELECT count(*) FROM pg_cluster_state
		   WHERE category='$category' AND key='$key'});
}

sub o2_snapshot
{
	my ($node) = @_;
	my %expected = map { $_ => 1 } @O2_KEYS;
	my %values;
	my $rows = $node->safe_psql(
		'postgres',
		q{SELECT key || chr(9) || value
		  FROM pg_cluster_state
		  WHERE category = 'pcm' AND key LIKE 'pcm\_x\_acquire\_%'
		  ORDER BY key});
	for my $line (split(/\n/, $rows))
	{
		my ($key, $value) = split(/\t/, $line, 2);
		die "unexpected O2 key [$key]" unless $expected{$key};
		die "duplicate O2 key [$key]" if exists $values{$key};
		die "non-u64 O2 key $key: [$value]" unless is_u64($value);
		$values{$key} = $value;
	}
	for my $key (@O2_KEYS)
	{
		die "missing O2 key [$key]" unless exists $values{$key};
	}
	die 'O2 surface is not exactly 50 keys'
	  unless scalar(keys %values) == scalar(@O2_KEYS);
	return \%values;
}

sub lmon_snapshot
{
	my ($node) = @_;
	my %values;
	my $rows = $node->safe_psql(
		'postgres',
		q{SELECT key || chr(9) || value
		  FROM pg_cluster_state
		  WHERE category = 'lmon'
		    AND key IN ('lmon_timed_duty_sample_count',
		                'lmon_total_iter_us',
		                'lmon_main_loop_iters')
		  ORDER BY key});
	for my $line (split(/\n/, $rows))
	{
		my ($key, $value) = split(/\t/, $line, 2);
		die "duplicate LMON key [$key]" if exists $values{$key};
		die "non-u64 LMON key $key: [$value]" unless is_u64($value);
		$values{$key} = 0 + $value;
	}
	die 'incomplete LMON triplet' unless scalar(keys %values) == 3;
	return \%values;
}

sub file_set_sha256
{
	my (@paths) = @_;
	my $sha = Digest::SHA->new(256);
	for my $path (@paths)
	{
		open(my $fh, '<:raw', $path) or die "open $path: $!";
		$sha->add($path, "\0");
		$sha->addfile($fh);
		close($fh) or die "close $path: $!";
	}
	return $sha->hexdigest;
}

sub command_stdout
{
	my (@command) = @_;
	my ($stdout, $stderr) = ('', '');
	IPC::Run::run(\@command, '>', \$stdout, '2>', \$stderr)
	  or die "command failed [@command]: $stderr";
	$stdout =~ s/\s+\z//;
	return $stdout;
}

sub postmaster_pid
{
	my ($node) = @_;
	my $path = $node->data_dir . '/postmaster.pid';
	open(my $fh, '<', $path) or die "open $path: $!";
	my $pid = <$fh>;
	close($fh) or die "close $path: $!";
	chomp($pid);
	die "invalid postmaster pid [$pid]" unless $pid =~ /\A[1-9]\d*\z/;
	return 0 + $pid;
}

sub process_stat
{
	my ($pid) = @_;
	my ($stdout, $stderr) = ('', '');
	my $ok = IPC::Run::run(
		[ 'ps', '-o', 'stat=', '-p', "$pid" ],
		'>', \$stdout, '2>', \$stderr);
	return '' unless $ok;
	$stdout =~ s/\s+\z//;
	return $stdout;
}

sub set_injection
{
	my ($node, $arm) = @_;
	my $point = 'cluster-gcs-block-invalidate-stall-ack';
	$node->safe_psql(
		'postgres',
		"ALTER SYSTEM SET cluster.injection_points = '$point:$arm'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

sub clear_injection
{
	my ($node) = @_;
	set_injection($node, 'none');
	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

END
{
	kill('CONT', $stopped_lmon_pid) if $stopped_lmon_pid;
	if ($injection_armed)
	{
		eval { clear_injection($pair->node0); };
		eval { clear_injection($pair->node1); };
	}
}

$pair->start_pair;
my $node0 = $pair->node0;
my $node1 = $pair->node1;

ok(
	$pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'bring-up: both peer links connected');
ok(
	wait_for(
		sub {
			my ($rc0) = $node0->psql(
				'postgres', 'CREATE TABLE IF NOT EXISTS r1_join_probe0(i int)');
			my ($rc1) = $node1->psql(
				'postgres', 'CREATE TABLE IF NOT EXISTS r1_join_probe1(i int)');
			return $rc0 == 0 && $rc1 == 0;
		},
		90),
	'bring-up: both nodes accept writable transactions');

# L1: freeze source, binary, config, node and postmaster-incarnation identity.
my $source_root = "$FindBin::RealBin/../../../..";
my $source_commit =
  command_stdout('git', '-C', $source_root, 'rev-parse', 'HEAD');
like($source_commit, qr/\A[0-9a-f]{40}\z/, 'L1 source commit identity frozen');
my $postgres_binary = $node0->config_data('--bindir') . '/postgres';
my $binary_sha = file_set_sha256($postgres_binary);
like($binary_sha, qr/\A[0-9a-f]{64}\z/, 'L1 product binary SHA-256 frozen');

my %identity;
for my $entry ([ 0, $node0 ], [ 1, $node1 ])
{
	my ($node_id, $node) = @$entry;
	my $row = $node->safe_psql(
		'postgres',
		q{SELECT current_setting('cluster.node_id') || chr(9) ||
		         pg_postmaster_start_time() || chr(9) || version()});
	my ($reported_id, $started_at, $version) = split(/\t/, $row, 3);
	my $pid = postmaster_pid($node);
	my $config_sha = file_set_sha256(
		$node->data_dir . '/postgresql.conf',
		$node->data_dir . '/pgrac.conf');
	$identity{$node_id} = {
		node_id => $reported_id,
		pid => $pid,
		started_at => $started_at,
		version => $version,
		config_sha => $config_sha };
	is($reported_id, "$node_id", "L1 node$node_id id frozen");
	cmp_ok($pid, '>', 1, "L1 node$node_id postmaster pid frozen");
	ok(length($started_at) > 0, "L1 node$node_id postmaster start frozen");
	like($version, qr/PostgreSQL 16\.13/, "L1 node$node_id binary version frozen");
	like($config_sha, qr/\A[0-9a-f]{64}\z/,
		"L1 node$node_id canonical test config identity frozen");
}

# L2: all 71 exact rows exist once and numeric rows are unsigned 64-bit.
for my $entry ([ 0, $node0 ], [ 1, $node1 ])
{
	my ($node_id, $node) = @$entry;
	my (%count, %value);
	my $rows = $node->safe_psql(
		'postgres',
		q{SELECT category || '/' || key || chr(9) || value
		  FROM pg_cluster_state
		  WHERE category IN ('pcm', 'lmon', 'undo', 'gcs', 'r4')});
	for my $line (split(/\n/, $rows))
	{
		my ($category_key, $row_value) = split(/\t/, $line, 2);
		$count{$category_key}++;
		$value{$category_key} = $row_value;
	}
	my $bad_presence = 0;
	my $bad_numeric = 0;
	for my $group (
		[ 'pcm', \@O2_KEYS ],
		[ 'lmon', \@LMON_KEYS ],
		[ 'undo', \@UNDO_KEYS ],
		[ 'gcs', \@GCS_KEYS ],
		[ 'r4', \@R4_KEYS ])
	{
		my ($category, $keys) = @$group;
		for my $key (@$keys)
		{
			my $compound = "$category/$key";
			$bad_presence++ if ($count{$compound} // 0) != 1;
			next if $key eq 'segment_observation_status';
			$bad_numeric++ unless is_u64($value{$compound});
		}
	}
	is($bad_presence, 0, "L2 node$node_id has every exact R1 row once");
	is($bad_numeric, 0, "L2 node$node_id R1 numeric rows are unsigned64");
	my $status = $value{'undo/segment_observation_status'} // '';
	ok(grep($_ eq $status, @UNDO_STATUS),
		"L2 node$node_id undo status is in the closed set");
}

my $natural_baseline0 = o2_snapshot($node0);
my $natural_baseline1 = o2_snapshot($node1);

# Create a relation whose per-node catalog entries map to one shared file.
my $table;
for my $attempt (1 .. 16)
{
	my $candidate = "r1_obs_$attempt";
	$_->safe_psql('postgres', "CREATE TABLE $candidate (id int, v bigint)")
	  for ($node0, $node1);
	my $path0 =
	  $node0->safe_psql('postgres', "SELECT pg_relation_filepath('$candidate')");
	my $path1 =
	  $node1->safe_psql('postgres', "SELECT pg_relation_filepath('$candidate')");
	if ($path0 eq $path1)
	{
		$table = $candidate;
		last;
	}
}
die 'could not create a relation with one shared file identity'
  unless defined($table);
$node0->safe_psql('postgres', "INSERT INTO $table VALUES (1, 0), (2, 0)");
$node0->safe_psql('postgres', "UPDATE $table SET v = v + 1 WHERE id = 1");

# L3: one real cross-node successful writer acquisition.
my $before_l3 = o2_snapshot($node1);
$node1->safe_psql('postgres', "UPDATE $table SET v = v + 1 WHERE id = 1");
my $after_l3 = o2_snapshot($node1);
is(
	0 + $after_l3->{pcm_x_acquire_started_count},
	1 + $before_l3->{pcm_x_acquire_started_count},
	'L3 one writer acquisition started');
is(
	0 + $after_l3->{pcm_x_acquire_result_ok_count},
	1 + $before_l3->{pcm_x_acquire_result_ok_count},
	'L3 matching OK terminal incremented exactly once');
is(
	0 + $after_l3->{pcm_x_acquire_active_count},
	0 + $before_l3->{pcm_x_acquire_active_count},
	'L3 active returned to its before value');
my $l3_hist_delta = 0;
for my $key (@BUCKET_KEYS, 'pcm_x_acquire_success_us_overflow_count')
{
	$l3_hist_delta += (0 + $after_l3->{$key}) - (0 + $before_l3->{$key});
}
is($l3_hist_delta, 1, 'L3 exactly one success histogram sample recorded');

# L4: stopping the remote LMON makes the existing GES preflight time out
# before PCM-X publishes a wait; the complete O2 vector is unchanged.
$node0->safe_psql('postgres', "UPDATE $table SET v = v + 1 WHERE id = 1");
my $before_l4 = o2_snapshot($node1);
my $lmon0_pid = 0 + $node0->safe_psql(
	'postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon'});
cmp_ok($lmon0_pid, '>', 1, 'L4 located the remote LMON process');
ok(kill('STOP', $lmon0_pid) == 1, 'L4 stopped the remote LMON');
$stopped_lmon_pid = $lmon0_pid;
ok(
	wait_for(sub { process_stat($lmon0_pid) =~ /T/ }, 10, 100_000),
	'L4 kernel confirms the remote LMON is stopped');
my ($rc_l4, $out_l4, $err_l4) = $node1->psql(
	'postgres',
	"SET cluster.ges_request_timeout_ms = 500; "
	  . "SET statement_timeout = '5s'; "
	  . "UPDATE $table SET v = v + 1 WHERE id = 1");
ok(kill('CONT', $lmon0_pid) == 1, 'L4 resumed the remote LMON');
$stopped_lmon_pid = 0;
isnt($rc_l4, 0, 'L4 real preflight refusal surfaces an error');
like($err_l4, qr/cluster.*lock acquire.*timeout/i,
	'L4 error is the existing cluster lock-acquire timeout');
my $after_l4 = o2_snapshot($node1);
is_deeply($after_l4, $before_l4,
	'L4 preflight refusal leaves all 50 O2 values unchanged');
ok(
	wait_for(sub { process_stat($lmon0_pid) !~ /T/ }, 10, 100_000),
	'L4 remote LMON remains resumed');

# L6-L8: exact nearest-rank helper behavior.
sub nearest_rank_percentile
{
	my ($buckets, $overflow, $q_num, $q_den) = @_;
	my $sample_count = $overflow;
	$sample_count += $_ for @$buckets;
	return 'UNAVAILABLE_NO_SAMPLE' if $sample_count == 0;
	my $rank =
	  int(($q_num * $sample_count + $q_den - 1) / $q_den);
	my $cumulative = 0;
	for my $index (0 .. $#$buckets)
	{
		$cumulative += $buckets->[$index];
		return 2**$index if $cumulative >= $rank;
	}
	return 'UNBOUNDED_GT_2147483648_US';
}

{
	my @vector = (0) x 32;
	$vector[0] = 10;
	$vector[3] = 80;
	$vector[7] = 10;
	is(nearest_rank_percentile(\@vector, 0, 1, 2), 8,
		'L6 p50 nearest-rank upper bound');
	is(nearest_rank_percentile(\@vector, 0, 99, 100), 128,
		'L6 p99 nearest-rank upper bound');
	is(nearest_rank_percentile(\@vector, 0, 999, 1000), 128,
		'L6 p999 nearest-rank upper bound');
	is(nearest_rank_percentile([ (0) x 32 ], 0, 1, 2),
		'UNAVAILABLE_NO_SAMPLE', 'L7 no sample is unavailable, not zero');
	is(nearest_rank_percentile([ (0) x 32 ], 1, 999, 1000),
		'UNBOUNDED_GT_2147483648_US', 'L8 overflow rank is unbounded');
}

# L9: one bounded predicate captures the advancing count/time/loop triplet.
my $before_l9 = lmon_snapshot($node0);
my $after_l9;
ok(
	wait_for(
		sub {
			$after_l9 = lmon_snapshot($node0);
			return
			  $after_l9->{lmon_timed_duty_sample_count}
			    > $before_l9->{lmon_timed_duty_sample_count}
			  && $after_l9->{lmon_total_iter_us}
			    > $before_l9->{lmon_total_iter_us}
			  && $after_l9->{lmon_main_loop_iters}
			    > $before_l9->{lmon_main_loop_iters};
		},
		30),
	'L9 timed-duty count/time pair and existing loop count advance together');
is(
	$node0->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
	$identity{0}->{started_at},
	'L9 LMON delta stayed on the frozen postmaster incarnation');

# L10: current/high-water/effective-cap are coherent after real DML.
for my $entry ([ 0, $node0 ], [ 1, $node1 ])
{
	my ($node_id, $node) = @$entry;
	is(state_val($node, 'undo', 'segment_observation_status'), 'READY',
		"L10 node$node_id undo observation is READY");
	my $current = state_num($node, 'undo', 'segment_allocated_count');
	my $high_water =
	  state_num($node, 'undo', 'segment_allocated_high_water');
	my $effective = state_num($node, 'undo', 'segment_effective_cap');
	cmp_ok($current, '>=', 1, "L10 node$node_id counts real undo files");
	cmp_ok($current, '<=', $high_water,
		"L10 node$node_id current does not exceed high-water");
	cmp_ok($high_water, '<=', 256,
		"L10 node$node_id high-water stays within native cap");
	cmp_ok($effective, '>=', $current,
		"L10 node$node_id effective cap covers current");
}

# L12: SIGHUP lowering retains the no-retro-shrink floor and write behavior.
{
	my $current = state_num($node1, 'undo', 'segment_allocated_count');
	$node1->safe_psql(
		'postgres',
		'ALTER SYSTEM SET cluster.undo_segments_max_per_instance = 16');
	$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(500_000);
	my $effective = state_num($node1, 'undo', 'segment_effective_cap');
	my $expected = $current > 16 ? $current : 16;
	is($effective, $expected,
		'L12 effective cap is max(clamped GUC, current)');
	$node1->safe_psql(
		'postgres', "UPDATE $table SET v = v + 1 WHERE id = 2");
	pass('L12 lowered observation cap does not change allocator semantics');
	$node1->safe_psql(
		'postgres',
		'ALTER SYSTEM RESET cluster.undo_segments_max_per_instance');
	$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

# L13/L14: one read supplies both master metadata aliases, while holder
# outcomes remain separate product rows.
{
	my $rows = $node1->safe_psql(
		'postgres',
		q{SELECT key || '=' || value
		  FROM pg_cluster_state
		  WHERE category = 'gcs'
		    AND key IN ('pi_watermark_retire_count',
		                'pi_master_metadata_retire_count')
		  ORDER BY key});
	my %values = map { split(/=/, $_, 2) } split(/\n/, $rows);
	is($values{pi_master_metadata_retire_count},
		$values{pi_watermark_retire_count},
		'L13 old and new PI metadata keys are equal in one snapshot');
	is(key_count($node1, 'xnode_lever', 'h_pi_discarded_count'), 1,
		'L14 holder discard success remains separately labeled');
	is(key_count($node1, 'xnode_lever', 'h_pi_discard_miss_count'), 1,
		'L14 holder discard miss remains separately labeled');
	is(
		$node1->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_cluster_state
			  WHERE key ~ 'pi.*(completion|complete_rate)'}),
		'0',
		'L14 no fabricated global completion row exists');
}

# L5: a real published live-peer stall is canceled by statement_timeout.
# PG_CATCH records one exception, closes active, and does not fabricate an
# ordinary result or success sample.
$node0->safe_psql('postgres', "UPDATE $table SET v = v + 1 WHERE id = 1");
my $before_l5 = o2_snapshot($node1);
$injection_armed = 1;
set_injection($_, 'skip') for ($node0, $node1);
usleep(700_000);
my ($rc_l5, $out_l5, $err_l5) = $node1->psql(
	'postgres',
	"\\set VERBOSITY verbose\n"
	  . "SET cluster.ges_request_timeout_ms = 60000;\n"
	  . "SET statement_timeout = '500ms';\n"
	  . "UPDATE $table SET v = v + 1 WHERE id = 1;");
clear_injection($_) for ($node0, $node1);
$injection_armed = 0;
isnt($rc_l5, 0, 'L5 published stalled acquisition raises ERROR');
like(
	$err_l5,
	qr/ERROR:\s+57014:\s+canceling statement due to statement timeout/,
	'L5 original SQLSTATE and statement-timeout message are preserved');
unlike($err_l5, qr/^DETAIL:/m, 'L5 absent ErrorData detail remains absent');
unlike($err_l5, qr/^HINT:/m, 'L5 absent ErrorData hint remains absent');
my $after_l5;
ok(
	wait_for(
		sub {
			$after_l5 = o2_snapshot($node1);
			return
			  0 + $after_l5->{pcm_x_acquire_started_count}
			    == 1 + $before_l5->{pcm_x_acquire_started_count}
			  && 0 + $after_l5->{pcm_x_acquire_exception_count}
			    == 1 + $before_l5->{pcm_x_acquire_exception_count}
			  && 0 + $after_l5->{pcm_x_acquire_active_count}
			    == 0 + $before_l5->{pcm_x_acquire_active_count};
		},
		15),
	'L5 started/exception advance once and active closes to baseline');
for my $key (@RESULT_KEYS, @BUCKET_KEYS,
	'pcm_x_acquire_success_us_overflow_count')
{
	is($after_l5->{$key}, $before_l5->{$key},
		"L5 $key unchanged by exception");
}

# L15: the nine proof-kind-dependent O1 rows are absent, not registered zeroes.
my $o1_sql = join(',', map { "'$_'" } @O1_KEYS);
is(
	state_val($node1, 'pcm', 'resource_x_proof_readiness'),
	'UNAVAILABLE_PROOF_KIND',
	'L15 proof readiness is explicitly unavailable through R6');
is(
	$node1->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE key IN ($o1_sql)"),
	'0',
	'L15 all proof-kind-dependent O1 SQL rows remain absent through R9');

# L16: with no load in flight, active returns to each node's initial value.
ok(
	wait_for(
		sub {
			my $snap0 = o2_snapshot($node0);
			my $snap1 = o2_snapshot($node1);
			return
			  $snap0->{pcm_x_acquire_active_count}
			    eq $natural_baseline0->{pcm_x_acquire_active_count}
			  && $snap1->{pcm_x_acquire_active_count}
			    eq $natural_baseline1->{pcm_x_acquire_active_count};
		},
		15),
	'L16 both active gauges return to their frozen before values');

# L11 runs last: first observation after a clean restart reconstructs existing
# undo files before any post-restart test DML.
my $old_start0 = $identity{0}->{started_at};
$node0->stop;
$node0->start;
my $new_start0 =
  $node0->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');
isnt($new_start0, $old_start0, 'L11 restart created a new incarnation');
my $restart_rows = $node0->safe_psql(
	'postgres',
	q{SELECT key || '=' || value
	  FROM pg_cluster_state
	  WHERE category = 'undo'
	    AND key IN ('segment_observation_status',
	                'segment_allocated_count')
	  ORDER BY key});
my %restart = map { split(/=/, $_, 2) } split(/\n/, $restart_rows);
is($restart{segment_observation_status}, 'READY',
	'L11 first post-restart observation is READY');
is(
	state_val($node0, 'pcm', 'resource_x_proof_readiness'),
	'UNAVAILABLE_PROOF_KIND',
	'L11 proof readiness stays explicitly unavailable after restart');
ok(
	is_u64($restart{segment_allocated_count})
	  && $restart{segment_allocated_count} >= 1,
	'L11 pre-existing undo files reconstruct before post-restart DML');

$pair->stop_pair;

done_testing();
