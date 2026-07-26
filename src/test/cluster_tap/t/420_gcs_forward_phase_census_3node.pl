#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 420_gcs_forward_phase_census_3node.pl
#    Observe a real three-corner forward cancellation and prove the atomic
#    all-shard phase census drains to a stable terminal state.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep time);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'GCS forward census requires --enable-cluster';
}

my $point = 'cluster-gcs-forward-cancelling-window';
my @phase_keys = qw(
	dedup_forward_phase_snapshot_valid
	dedup_forward_marker_count
	dedup_forwarded_phase_count
	dedup_cancelling_phase_count
	dedup_forward_phase_invalid_count
);

sub gcs_uint
{
	my ($node, $key) = @_;
	my $row = $node->safe_psql(
		'postgres',
		"SELECT count(*) || E'\\t' || coalesce(min(value), '') "
		  . "FROM pg_cluster_state "
		  . "WHERE category = 'gcs' AND key = '$key'",
		timeout => 5);
	my ($count, $value) = split(/\t/, $row, 2);
	die "gcs.$key must exist exactly once: [$row]"
	  unless defined($count) && $count eq '1' && defined($value);
	die "gcs.$key must be unsigned decimal: [$value]"
	  unless $value =~ /\A\d+\z/;
	return int($value);
}

sub phase_snapshot
{
	my ($node) = @_;
	return {map { $_ => gcs_uint($node, $_) } @phase_keys};
}

sub assert_terminal
{
	my ($node, $node_id, $name) = @_;
	my $snapshot = phase_snapshot($node);
	is_deeply(
		$snapshot,
		{
			dedup_forward_phase_snapshot_valid => 1,
			dedup_forward_marker_count         => 0,
			dedup_forwarded_phase_count        => 0,
			dedup_cancelling_phase_count       => 0,
			dedup_forward_phase_invalid_count  => 0,
		},
		"$name node$node_id phase census is terminal");
	return $snapshot;
}

sub set_window
{
	my ($on, @nodes) = @_;
	my $sql = $on
	  ? "ALTER SYSTEM SET cluster.injection_points = '$point:sleep:6000000'"
	  : q{ALTER SYSTEM RESET cluster.injection_points};
	for my $node (@nodes)
	{
		$node->safe_psql('postgres', $sql);
		$node->safe_psql('postgres', q{SELECT pg_reload_conf()});
	}
	usleep(1_000_000);
}

sub write_retry
{
	my ($node, $sql) = @_;
	for (1 .. 10)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(300_000);
	}
	return 0;
}

my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'forward_census',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 2',
		'cluster.read_scache = on',
	]);
$triple->start_triple;
usleep(3_000_000);

my @nodes = map { $triple->node($_) } 0 .. 2;
for my $from (0 .. 2)
{
	is($nodes[$from]->safe_psql('postgres', 'SELECT 1'), '1',
		"L0 node$from is alive");
	for my $to (0 .. 2)
	{
		next if $from == $to;
		ok($triple->wait_for_peer_state($from, $to, 'connected', 45),
			"L0 node$from sees node$to connected");
	}
	assert_terminal($nodes[$from], $from, 'baseline');
}

set_window(1, @nodes);
my ($converter, $target_table, $live_node, $live_snapshot);
for my $probe (1 .. 12)
{
	my $table = "t420_$probe";
	$_->safe_psql('postgres', "CREATE TABLE $table (id int, v int)")
	  for @nodes;
	next unless write_retry($nodes[1],
		"INSERT INTO $table VALUES (1, 10), (2, 20)");
	next unless write_retry($nodes[1], 'CHECKPOINT');
	$nodes[1]->safe_psql('postgres', "SELECT sum(v) FROM $table");
	next unless eval {
		$nodes[2]->safe_psql('postgres', "SELECT sum(v) FROM $table");
		1;
	};

	my $candidate = $nodes[1]->background_psql(
		'postgres', on_error_die => 0, timeout => 30);
	$candidate->query_until(
		qr/T420_CONVERT_STARTED/,
		"\\echo T420_CONVERT_STARTED\n"
		  . "UPDATE $table SET v = v + 1 WHERE id = 1;\n"
		  . "\\echo T420_CONVERT_DONE\n");

	my $deadline = time() + 2;
	while (time() < $deadline && !defined($live_node))
	{
		for my $node_id (0 .. 2)
		{
			my $snapshot = phase_snapshot($nodes[$node_id]);
			if ($snapshot->{dedup_cancelling_phase_count} == 1)
			{
				($live_node, $live_snapshot) = ($node_id, $snapshot);
				last;
			}
		}
		usleep(100_000) unless defined($live_node);
	}

	if (defined($live_node))
	{
		($converter, $target_table) = ($candidate, $table);
		last;
	}
	$candidate->query_until(qr/T420_CONVERT_DONE/, q{});
	$candidate->quit;
}

ok(defined($live_node),
	'L1 a real three-corner conversion exposes one CANCELLING marker');
if (defined($live_node))
{
	is($live_snapshot->{dedup_forward_phase_snapshot_valid}, 1,
		'L1 live all-shard census is valid');
	is($live_snapshot->{dedup_forward_marker_count}, 1,
		'L1 live census contains exactly one forward marker');
	is($live_snapshot->{dedup_forwarded_phase_count}, 0,
		'L1 marker has atomically left FORWARDED');
	is($live_snapshot->{dedup_cancelling_phase_count}, 1,
		'L1 marker is atomically visible as CANCELLING');
	is($live_snapshot->{dedup_forward_phase_invalid_count}, 0,
		'L1 live marker shape is valid');
}

set_window(0, @nodes);
if (defined($converter))
{
	$converter->query_until(qr/T420_CONVERT_DONE/, q{});
	$converter->quit;
	is($nodes[1]->safe_psql(
			'postgres', "SELECT v FROM $target_table WHERE id = 1"),
		'11', 'L2 real S-to-X conversion commits');
}

my @settle1;
for my $node_id (0 .. 2)
{
	push @settle1, assert_terminal($nodes[$node_id], $node_id, 'settle-1');
}
usleep(500_000);
for my $node_id (0 .. 2)
{
	my $settle2 = assert_terminal($nodes[$node_id], $node_id, 'settle-2');
	is_deeply($settle2, $settle1[$node_id],
		"L2 node$node_id terminal phase census is stable");
}

$triple->stop_triple;
done_testing();
