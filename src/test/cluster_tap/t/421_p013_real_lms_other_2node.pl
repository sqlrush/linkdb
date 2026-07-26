#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 421_p013_real_lms_other_2node.pl
#    Drive the real undo-verdict wire path to a non-authoritative LMS and
#    prove its OTHER/RESIDUAL producer updates the conserved counters once.
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
use Time::HiRes qw(usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'real LMS OTHER coverage requires --enable-cluster';
}

my $source_root = "$FindBin::RealBin/../../..";
my $visibility_source =
  slurp_file("$source_root/backend/cluster/cluster_visibility_inject.c");
my $real_fetch_calls =
  () = $visibility_source =~ /\bcluster_gcs_block_undo_verdict_fetch_and_wait\s*\(/g;
is($real_fetch_calls, 1,
	'source contract calls the real undo-verdict request path exactly once');
ok(
	$visibility_source =~
	  /\bcluster_gcs_block_undo_verdict_fetch_and_wait\s*\((?:(?!\);).)*,\s*false\s*,/s,
	'source contract fixes authoritative=false on the real request');
for my $forbidden (
	qw(lms_note_other_refusal_detail
	cluster_cr_server_other_refusal_detail_bump
	cluster_vis53r97_note_srv_other))
{
	my $count = () = $visibility_source =~ /\b\Q$forbidden\E\s*\(/g;
	is($count, 0, "source contract forbids direct $forbidden");
}

my @detail_keys = qw(
	cr_server_other_refuse_not_authoritative_count
	cr_server_other_refuse_not_mine_count
	cr_server_other_refuse_expected_segment_invalid_count
	cr_server_other_refuse_expected_slot_invalid_count
	cr_server_other_refuse_segment_mismatch_count
	cr_server_other_refuse_slot_mismatch_count
	cr_server_other_refuse_confirm_resolve_kind_count
	cr_server_other_refuse_confirm_segment_mismatch_count
	cr_server_other_refuse_confirm_slot_mismatch_count
	cr_server_other_refuse_confirm_wrap_mismatch_count
	cr_server_other_refuse_confirm_scn_mismatch_count
	cr_server_other_refuse_terminal_unknown_count
	cr_server_other_refuse_residual_count
);
my $aggregate_key = 'vis53r97_leg_srv_other_refuse_count';
my @independent_keys = qw(
	vis_freshref_verdict_failclosed_count
	cr_server_terminal_resample_commit_count
	cr_server_terminal_resample_abort_count
	cr_server_terminal_resample_unknown_count
);
my @p013_keys = (@detail_keys, $aggregate_key, @independent_keys);

sub p013_snapshot
{
	my ($node) = @_;
	my %snapshot;
	for my $key (@p013_keys)
	{
		my $row = $node->safe_psql(
			'postgres',
			"SELECT count(*) || E'\\t' || coalesce(min(value), '') "
			  . "FROM pg_cluster_state "
			  . "WHERE category = 'cr' AND key = '$key'",
			timeout => 5);
		my ($count, $value) = split(/\t/, $row, 2);
		die "cr.$key must exist exactly once: [$row]"
		  unless defined($count) && $count eq '1' && defined($value);
		die "cr.$key must be unsigned decimal: [$value]"
		  unless $value =~ /\A\d+\z/;
		$snapshot{$key} = int($value);
	}
	return \%snapshot;
}

sub assert_conservation
{
	my ($snapshot, $name) = @_;
	my $sum = 0;
	$sum += $snapshot->{$_} for @detail_keys;
	is($snapshot->{$aggregate_key}, $sum, $name);
}

sub delta
{
	my ($before, $after, $key) = @_;
	return $after->{$key} - $before->{$key};
}

sub wait_for_membership_count
{
	my ($node, $expected, $seconds) = @_;
	my $deadline = time() + $seconds;
	my $last;

	do
	{
		$last = $node->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_cluster_membership WHERE state = 'member'},
			timeout => 5);
		return (1, int($last))
		  if defined($last) && $last =~ /\A\d+\z/ && int($last) == $expected;
		usleep(100_000);
	} while (time() < $deadline);
	return (0, $last);
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'p013_real_other',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 1',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.undo_segments_per_instance = 64',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.block_self_contained = on',
	]);
$pair->start_pair;
usleep(3_000_000);

my ($node0, $node1) = ($pair->node0, $pair->node1);
ok($pair->wait_for_peer_state(0, 1, 'connected', 45),
	'L0 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 45),
	'L0 node1 sees node0 connected');
for my $node_id (0 .. 1)
{
	my $node = $node_id == 0 ? $node0 : $node1;
	my ($ready, $count) = wait_for_membership_count($node, 2, 60);
	ok($ready, "L0 node$node_id sees two MEMBER nodes")
	  or diag("last node$node_id membership count=$count");
}
is(
	$node1->safe_psql(
		'postgres',
		q{
			SELECT count(*)
			FROM pg_proc
			WHERE proname = 'cluster_test_request_undo_verdict_other'
			  AND oidvectortypes(proargtypes) = 'integer, xid'
			  AND prorettype = 'bool'::regtype}),
	'1', 'L0 catalog exposes exactly one bool(int4,xid) test UDF');

my @before = (p013_snapshot($node0), p013_snapshot($node1));
assert_conservation($before[0], 'L0 node0 P013 counters conserve before trigger');
assert_conservation($before[1], 'L0 node1 P013 counters conserve before trigger');

my ($local_rc) = $node1->psql(
	'postgres',
	q{
		SELECT cluster_test_request_undo_verdict_other(
			1, pg_current_xact_id()::text::xid);
	});
isnt($local_rc, 0, 'L0 UDF rejects a local target before sending');

my $foreign_xid =
  $node0->safe_psql('postgres', q{SELECT pg_current_xact_id()::text});
my ($foreign_rc) = $node1->psql(
	'postgres',
	"SELECT cluster_test_request_undo_verdict_other(0, '$foreign_xid'::xid)");
isnt($foreign_rc, 0, 'L0 UDF rejects an xid outside the requester stripe');

$node1->safe_psql('postgres', q{CREATE ROLE t421_unprivileged});
my ($unprivileged_rc) = $node1->psql(
	'postgres',
	q{
		SET ROLE t421_unprivileged;
		SELECT cluster_test_request_undo_verdict_other(
			0, pg_current_xact_id()::text::xid);
	});
isnt($unprivileged_rc, 0, 'L0 UDF is superuser-only');

my $result = $node1->safe_psql(
	'postgres',
	q{
		BEGIN;
		SELECT cluster_test_request_undo_verdict_other(
			0, pg_current_xact_id()::text::xid);
		COMMIT;
	});
is($result, 'f',
	'L1 requester receives the real non-authoritative OTHER denial');

my @after = (p013_snapshot($node0), p013_snapshot($node1));
assert_conservation($after[0], 'L1 node0 P013 counters conserve after trigger');
assert_conservation($after[1], 'L1 node1 P013 counters conserve after trigger');

is(delta($before[0], $after[0], $aggregate_key), 1,
	'L1 serving node aggregate advances once');
is(delta(
		$before[0], $after[0],
		'cr_server_other_refuse_residual_count'),
	1, 'L1 real LMS early not-mine branch is classified RESIDUAL once');
for my $key (@detail_keys)
{
	next if $key eq 'cr_server_other_refuse_residual_count';
	is(delta($before[0], $after[0], $key), 0,
		"L1 serving node leaves unrelated detail $key unchanged");
}
for my $key (@detail_keys, $aggregate_key)
{
	is(delta($before[1], $after[1], $key), 0,
		"L1 requester does not synthesize $key");
}
for my $node_id (0 .. 1)
{
	for my $key (@independent_keys)
	{
		is(delta($before[$node_id], $after[$node_id], $key), 0,
			"L1 node$node_id leaves independent key $key unchanged");
	}
}

usleep(500_000);
my @settled = (p013_snapshot($node0), p013_snapshot($node1));
is_deeply($settled[0], $after[0],
	'L2 node0 P013 counters are stable after the trigger');
is_deeply($settled[1], $after[1],
	'L2 node1 P013 counters are stable after the trigger');

my $logs = slurp_file($node0->logfile) . slurp_file($node1->logfile);
unlike($logs, qr/\b(?:PANIC|FATAL)\b/,
	'L2 trigger emits no PANIC or FATAL');

$pair->stop_pair;
done_testing();
