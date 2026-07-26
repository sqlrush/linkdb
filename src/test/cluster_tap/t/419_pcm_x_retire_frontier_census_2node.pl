#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 419_pcm_x_retire_frontier_census_2node.pl
#    Observe a real PCM-X RETIRE window and prove that the persistent peer
#    frontier census remains exact after the conversion drains.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep time);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'PCM-X retire census requires --enable-cluster';
}

my $point = 'cluster-pcm-x-retire-frontier-window';
my @aggregate_keys = qw(
	pcm_x_retire_frontier_snapshot_valid
	pcm_x_retire_frontier_configured_node_count
	pcm_x_retire_frontier_bound_node_count
	pcm_x_retire_frontier_idle_node_count
	pcm_x_retire_frontier_in_progress_node_count
	pcm_x_retire_frontier_invalid_node_count
);

sub exact_state
{
	my ($node, $category, $key) = @_;
	my $row = $node->safe_psql(
		'postgres',
		"SELECT count(*) || E'\\t' || coalesce(min(value), '') "
		  . "FROM pg_cluster_state "
		  . "WHERE category = '$category' AND key = '$key'",
		timeout => 5);
	my ($count, $value) = split(/\t/, $row, 2);
	die "$category.$key must exist exactly once: [$row]"
	  unless defined($count) && $count eq '1' && defined($value);
	return $value;
}

sub pcm_uint
{
	my ($node, $key) = @_;
	my $value = exact_state($node, 'pcm', $key);
	die "pcm.$key must be unsigned decimal: [$value]"
	  unless $value =~ /\A\d+\z/;
	return int($value);
}

sub peer_record
{
	my ($node, $peer) = @_;
	my $value = exact_state($node, 'pcm', "pcm_x_retire_frontier_peer_$peer");
	die "invalid peer $peer frontier grammar: [$value]"
	  unless $value =~
	  /\Aphase=(?:UNBOUND|IDLE|RETIRING|INVALID) epoch=\d+ session=\d+ next_prehandle=\d+ retired_prehandle=\d+ retired_ticket=\d+ in_progress_ticket=\d+\z/;
	return $value;
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

sub terminal_snapshot
{
	my ($node) = @_;
	my %snapshot = map { $_ => pcm_uint($node, $_) } @aggregate_keys;
	$snapshot{pcm_x_local_retire_gate} =
	  pcm_uint($node, 'pcm_x_local_retire_gate');
	$snapshot{pcm_x_local_retire_marker_count} =
	  pcm_uint($node, 'pcm_x_local_retire_marker_count');
	$snapshot{peer_0} = peer_record($node, 0);
	$snapshot{peer_1} = peer_record($node, 1);
	return \%snapshot;
}

sub assert_terminal
{
	my ($node, $node_id, $name) = @_;
	my $snapshot = terminal_snapshot($node);
	is($snapshot->{pcm_x_retire_frontier_snapshot_valid}, 1,
		"$name node$node_id snapshot is valid");
	is($snapshot->{pcm_x_retire_frontier_configured_node_count}, 2,
		"$name node$node_id has two configured peers");
	is($snapshot->{pcm_x_retire_frontier_bound_node_count}, 2,
		"$name node$node_id has two bound peers");
	is($snapshot->{pcm_x_retire_frontier_idle_node_count}, 2,
		"$name node$node_id has two idle peers");
	is($snapshot->{pcm_x_retire_frontier_in_progress_node_count}, 0,
		"$name node$node_id has no retiring peer");
	is($snapshot->{pcm_x_retire_frontier_invalid_node_count}, 0,
		"$name node$node_id has no invalid peer");
	is($snapshot->{pcm_x_local_retire_gate}, 0,
		"$name node$node_id retire gate is open");
	is($snapshot->{pcm_x_local_retire_marker_count}, 0,
		"$name node$node_id has no retire marker");
	like($snapshot->{peer_0}, qr/\Aphase=IDLE\b/,
		"$name node$node_id peer 0 is IDLE");
	like($snapshot->{peer_1}, qr/\Aphase=IDLE\b/,
		"$name node$node_id peer 1 is IDLE");
	return $snapshot;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'retire_census',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 1',
		'cluster.online_join = on',
		'cluster.read_scache = on',
		'cluster.crossnode_write_write = on',
	]);
$pair->start_pair;
usleep(3_000_000);

my ($node0, $node1) = ($pair->node0, $pair->node1);
ok($pair->wait_for_peer_state(0, 1, 'connected', 45),
	'L0 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 45),
	'L0 node1 sees node0 connected');

$_->safe_psql(
	'postgres',
	q{CREATE TABLE t419 (id int, v int not null)})
  for ($node0, $node1);
is(
	$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('t419')}),
	$node0->safe_psql('postgres', q{SELECT pg_relation_filepath('t419')}),
	'L0 relation catalog identity coincides on both nodes');
$node0->safe_psql(
	'postgres',
	q{
		INSERT INTO t419 SELECT g, 0 FROM generate_series(1, 200) g;
		CHECKPOINT;
		SELECT sum(v) FROM t419;
	});
is($node1->safe_psql('postgres', q{SELECT sum(v) FROM t419}), '0',
	'L0 remote read establishes a real shared-cache holder');

assert_terminal($node0, 0, 'baseline');
assert_terminal($node1, 1, 'baseline');

set_window(1, $node0, $node1);
my $converter = $node0->background_psql(
	'postgres', on_error_die => 1, timeout => 30);
$converter->query_until(
	qr/T419_CONVERT_STARTED/,
	"\\echo T419_CONVERT_STARTED\n"
	  . "UPDATE t419 SET v = v + 1 WHERE id = 1;\n"
	  . "\\echo T419_CONVERT_DONE\n");

my $deadline = time() + 8;
my ($retiring_node, $retiring_peer, $retiring_record);
while (time() < $deadline && !defined($retiring_node))
{
	for my $node_id (0 .. 1)
	{
		my $node = $node_id == 0 ? $node0 : $node1;
		next unless pcm_uint(
			$node, 'pcm_x_retire_frontier_in_progress_node_count') == 1;
		for my $peer (0 .. 1)
		{
			my $record = peer_record($node, $peer);
			if ($record =~ /\Aphase=RETIRING\b/)
			{
				($retiring_node, $retiring_peer, $retiring_record) =
				  ($node_id, $peer, $record);
				last;
			}
		}
	}
	usleep(100_000) unless defined($retiring_node);
}
ok(defined($retiring_node),
	'L1 census observes exactly one real RETIRING peer');
if (defined($retiring_node))
{
	my $node = $retiring_node == 0 ? $node0 : $node1;
	is(pcm_uint($node, 'pcm_x_retire_frontier_snapshot_valid'), 1,
		'L1 live RETIRE snapshot is valid');
	is(pcm_uint($node, 'pcm_x_retire_frontier_invalid_node_count'), 0,
		'L1 live RETIRE snapshot has no invalid peer');
	is(pcm_uint($node, 'pcm_x_local_retire_gate'), $retiring_peer + 1,
		'L1 retire gate names the exact RETIRING peer');
	like(
		$retiring_record,
		qr/\bretired_ticket=(\d+) in_progress_ticket=(\d+)\z/,
		'L1 RETIRING row exposes both persistent ticket frontiers');
	my ($retired, $in_progress) =
	  $retiring_record =~ /\bretired_ticket=(\d+) in_progress_ticket=(\d+)\z/;
	cmp_ok($in_progress, '>', $retired,
		'L1 in-progress ticket is newer than the retired frontier');
}

set_window(0, $node0, $node1);
$converter->query_until(qr/T419_CONVERT_DONE/, q{});
$converter->quit;

my @settle1;
for my $node_id (0 .. 1)
{
	my $node = $node_id == 0 ? $node0 : $node1;
	push @settle1, assert_terminal($node, $node_id, 'settle-1');
}
usleep(500_000);
for my $node_id (0 .. 1)
{
	my $node = $node_id == 0 ? $node0 : $node1;
	my $settle2 = assert_terminal($node, $node_id, 'settle-2');
	is_deeply($settle2, $settle1[$node_id],
		"node$node_id persistent frontier snapshot is byte-stable");
}
is($node0->safe_psql('postgres', q{SELECT v FROM t419 WHERE id = 1}), '1',
	'L2 real conversion commits its data change');

$pair->stop_pair;
done_testing();
