#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 419_stage8_r1_observability_4node.pl
#    Exercise current public observation surfaces on four product nodes.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use Test::More;
use Time::HiRes qw(time usleep);

my @result_labels = qw(
	ok duplicate retired not_found stale no_capacity counter_exhausted
	not_ready busy bad_state corrupt invalid gate_retry barrier_closed);
my @pcm_numeric_keys = (
	'pcm_x_acquire_started_count',
	'pcm_x_acquire_active_count',
	'pcm_x_acquire_exception_count',
	(map { "pcm_x_acquire_result_${_}_count" } @result_labels),
	(map { sprintf('pcm_x_acquire_success_us_le_2p%02d_count', $_) } 0 .. 31),
	'pcm_x_acquire_success_us_overflow_count');
my @lmon_numeric_keys =
	qw(lmon_timed_duty_sample_count lmon_total_iter_us);
my @undo_numeric_keys =
	qw(segment_allocated_count segment_allocated_high_water segment_effective_cap);
my @gcs_numeric_keys = qw(pi_master_metadata_retire_count);
my @r4_numeric_keys = qw(
  cr_route_started_count cr_holder_full_count cr_holder_retry_count
  cr_holder_failclosed_count undo_data_fetch_served_count
  undo_data_fetch_denied_count tx_resolve_unknown_count
  tx_resolve_in_progress_count tx_resolve_prepared_count
  tx_resolve_committed_count tx_resolve_aborted_count
  multi_resolve_served_count multi_resolve_unknown_count
  slot_capacity_retry_count);

my $quad;
END
{
	eval { $quad->stop_quad; } if defined($quad);
}

sub wait_for
{
	my ($predicate, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;

	while (time() < $deadline)
	{
		return 1 if $predicate->();
		usleep(250_000);
	}
	return $predicate->() ? 1 : 0;
}

sub state_map
{
	my ($node) = @_;
	my %state;
	my $rows = $node->safe_psql(
		'postgres',
		q{SELECT category || E'\t' || key || E'\t' || value
		  FROM pg_cluster_state
		  WHERE category IN ('pcm', 'lmon', 'undo', 'gcs', 'r4')
		  ORDER BY category, key},
		timeout => 10);

	for my $row (grep { $_ ne '' } split(/\n/, $rows))
	{
		my ($category, $key, $value) = split(/\t/, $row, 3);
		die "malformed pg_cluster_state row [$row]"
			unless defined($category) && defined($key) && defined($value);
		my $identity = "$category/$key";
		die "duplicate pg_cluster_state row $identity"
			if exists($state{$identity});
		$state{$identity} = $value;
	}
	return \%state;
}

sub missing_numeric_keys
{
	my ($state) = @_;
	my @missing;

	for my $entry (
		[ pcm => \@pcm_numeric_keys ],
		[ lmon => \@lmon_numeric_keys ],
		[ undo => \@undo_numeric_keys ],
		[ gcs => \@gcs_numeric_keys ],
		[ r4 => \@r4_numeric_keys ])
	{
		my ($category, $keys) = @$entry;
		for my $key (@$keys)
		{
			my $value = $state->{"$category/$key"};
			push @missing, "$category.$key"
				if !defined($value) || $value !~ /\A[0-9]+\z/;
		}
	}
	return \@missing;
}

$quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'stage8_r1_obs4',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'synchronous_commit = off',
		'cluster.online_join = on',
		'cluster.read_scache = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
	]);

$quad->start_quad;
usleep(3_000_000);
my @nodes = $quad->nodes;
is(scalar(@nodes), 4, 'L1 four product nodes are running');

for my $from (0 .. 3)
{
	is($nodes[$from]->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$from accepts SQL");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 60),
			"L1 node$from sees node$to connected");
	}
}

for my $node_id (0 .. 3)
{
	my $count = $nodes[$node_id]->safe_psql(
		'postgres',
		qq{CREATE TEMP TABLE stage8_r1_local_write(i integer);
		   INSERT INTO stage8_r1_local_write VALUES ($node_id);
		   SELECT count(*) FROM stage8_r1_local_write;},
		timeout => 20);
	is($count, '1', "L2 node$node_id completes a real write transaction");
}

my @before;
for my $node_id (0 .. 3)
{
	$before[$node_id] = state_map($nodes[$node_id]);
	my $missing = missing_numeric_keys($before[$node_id]);
	is_deeply($missing, [],
		"L3 node$node_id exposes every public numeric observation key")
		or diag(join(',', @$missing));
}

my $table = 'stage8_r1_public_observation';
for my $node_id (0 .. 3)
{
	my ($rc, $stdout, $stderr) = $nodes[$node_id]->psql(
		'postgres',
		"CREATE TABLE $table(id integer, value bigint NOT NULL) WITH (fillfactor=100)",
		timeout => 30);
	is($rc, 0, "L4 node$node_id creates the shared relation identity")
		or diag("stdout=[$stdout] stderr=[$stderr]");
}

my @relation_paths = map {
	$_->safe_psql('postgres', "SELECT pg_relation_filepath('$table')")
} @nodes;
is(scalar(grep { $_ eq $relation_paths[0] } @relation_paths), 4,
	'L4 all nodes map the relation to one shared identity');

my ($seed_rc, $seed_stdout, $seed_stderr) = $nodes[0]->psql(
	'postgres',
	"INSERT INTO $table VALUES (1, 0)",
	timeout => 30);
is($seed_rc, 0, 'L4 node0 seeds the shared relation')
	or diag("stdout=[$seed_stdout] stderr=[$seed_stderr]");

for my $node_id (0 .. 3)
{
	my ($rc, $stdout, $stderr) = $nodes[$node_id]->psql(
		'postgres',
		"SET statement_timeout='20s'; UPDATE $table SET value=value+1 WHERE id=1",
		timeout => 30);
	is($rc, 0, "L4 node$node_id completes a real shared-page update")
		or diag("stdout=[$stdout] stderr=[$stderr]");
}

for my $node_id (0 .. 3)
{
	is($nodes[$node_id]->safe_psql('postgres',
			"SELECT value FROM $table WHERE id=1", timeout => 20),
		'4', "L4 node$node_id observes all four completed updates");
}

my @after;
for my $node_id (0 .. 3)
{
	ok(wait_for(
		sub {
			$after[$node_id] = state_map($nodes[$node_id]);
			return ($after[$node_id]{'lmon/lmon_timed_duty_sample_count'} // 0)
				> ($before[$node_id]{'lmon/lmon_timed_duty_sample_count'} // 0)
				&& ($after[$node_id]{'lmon/lmon_total_iter_us'} // 0)
				> ($before[$node_id]{'lmon/lmon_total_iter_us'} // 0);
		},
		30),
		"L5 node$node_id completed-duty count and time advance together");
	$after[$node_id] //= state_map($nodes[$node_id]);

	cmp_ok(($after[$node_id]{'pcm/pcm_x_acquire_started_count'} // 0),
		'>=', ($before[$node_id]{'pcm/pcm_x_acquire_started_count'} // 0),
		"L6 node$node_id acquisition count is monotonic");
	is(($after[$node_id]{'pcm/pcm_x_acquire_active_count'} // ''), '0',
		"L6 node$node_id has no acquisition active at natural close");
	is(($after[$node_id]{'undo/segment_observation_status'} // ''), 'READY',
		"L7 node$node_id undo observation is ready");
	is(($after[$node_id]{'gcs/pi_master_metadata_retire_count'} // ''),
		($after[$node_id]{'gcs/pi_watermark_retire_count'} // ''),
		"L8 node$node_id PI metadata alias equals the existing counter");
}

$quad->stop_quad;
$quad = undef;
done_testing();
