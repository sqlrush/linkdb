#!/usr/bin/env perl
#-------------------------------------------------------------------------
# Active-ITL current-block transfer behavioral checks (L1-L10).
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

sub state_value
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$category' AND key='$key'});
	return defined($value) && $value ne '' ? $value + 0 : 0;
}

sub pair_state_value
{
	my ($pair, $category, $key) = @_;
	return state_value($pair->node0, $category, $key)
		+ state_value($pair->node1, $category, $key);
}

sub start_blocking_query
{
	my ($session, $sql) = @_;
	$session->query_until(qr/PGRAC_FIRED/,
		"\\echo PGRAC_FIRED\n$sql;\n");
}

sub wait_for_wait_event
{
	my ($node, $query_pattern, $event, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		my $observed = $node->safe_psql('postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			 WHERE query LIKE '$query_pattern'
			   AND pid <> pg_backend_pid()
			   AND state = 'active' LIMIT 1});
		return 1 if defined($observed) && $observed eq $event;
		usleep(200_000);
	}
	return 0;
}

sub wait_for_value
{
	my ($node, $table, $ctid, $expected, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		my $observed = eval {
			$node->safe_psql('postgres',
				"SELECT ctr FROM $table WHERE ctid = '$ctid'");
		};
		return 1 if defined($observed) && $observed eq $expected;
		usleep(200_000);
	}
	return 0;
}

sub make_same_block_table
{
	my ($node0, $node1, $table, $label, $nrows, $split_seed) = @_;
	$nrows //= 2;
	$node0->safe_psql('postgres', "CREATE TABLE $table (id int, ctr int)");
	$node1->safe_psql('postgres', "CREATE TABLE $table (id int, ctr int)");
	my $path0 = $node0->safe_psql('postgres', "SELECT pg_relation_filepath('$table')");
	my $path1 = $node1->safe_psql('postgres', "SELECT pg_relation_filepath('$table')");
	is($path0, $path1, "$label shared relation path matches");
	if ($split_seed)
	{
		my $source_values = join(', ', map { "($_, 100)" } 2 .. $nrows);
		$node0->safe_psql('postgres', "INSERT INTO $table VALUES $source_values");
		$node1->safe_psql('postgres', "INSERT INTO $table VALUES (1, 100)");
	}
	else
	{
		my $values = join(', ', map { "($_, 100)" } 1 .. $nrows);
		$node0->safe_psql('postgres', "INSERT INTO $table VALUES $values");
	}
	$node0->safe_psql('postgres', 'CHECKPOINT');
	is($node0->safe_psql('postgres',
		"SELECT count(DISTINCT (ctid::text::point)[0]::int) FROM $table"),
		'1', "$label exact CTIDs occupy one heap block");
	return $path0 eq $path1;
}

sub different_row_transfer
{
	my ($pair, $table, $kind, $terminal, $label, $split_seed) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);
	my $prepare0 = pair_state_value($pair, 'pcm', 'pcm_x_queue_revoke_count');
	my $active0 = pair_state_value($pair, 'xnode_lever', 'g_active_itl_transfer_count');
	my $self0 = pair_state_value($pair, 'gcs', 'block_x_self_ship_count');
	my $remote0 = pair_state_value($pair, 'gcs', 'block_x_transfer_ship_count');
	my $holder = $node0->background_psql('postgres', on_error_die => 1);

	$holder->query_safe('BEGIN');
	my $source_ctid = $split_seed ? '(0,1)' : '(0,2)';
	my $requester_ctid = $split_seed ? '(0,2)' : '(0,1)';
	if ($kind eq 'data')
	{
		$holder->query_safe("UPDATE $table SET ctr = 200 WHERE ctid = '$source_ctid'");
	}
	else
	{
		$holder->query_safe("SELECT ctr FROM $table WHERE ctid = '$source_ctid' FOR UPDATE");
	}
	my ($rc, undef, $err) = $node1->psql('postgres',
		"UPDATE $table SET ctr = ctr + 1 WHERE ctid = '$requester_ctid'");
	is($rc, 0, "$label different-row requester completes while source is active");
	unlike($err, qr/53R9H|held in X by a remote|cross-node write/i,
		"$label did not enter legacy refusal");
	cmp_ok(pair_state_value($pair, 'pcm', 'pcm_x_queue_revoke_count'), '>', $prepare0,
		"$label entered canonical SourcePrepare");
	cmp_ok(pair_state_value($pair, 'xnode_lever', 'g_active_itl_transfer_count'), '>', $active0,
		"$label counted the first immutable active-X source image");
	is(pair_state_value($pair, 'gcs', 'block_x_self_ship_count'), $self0,
		"$label did not use legacy master-holder destructive success");
	is(pair_state_value($pair, 'gcs', 'block_x_transfer_ship_count'), $remote0,
		"$label did not use legacy remote-holder destructive success");
	$holder->query_safe(uc($terminal));
	$holder->quit;
	return $rc == 0;
}

sub same_row_wait
{
	my ($pair, $table, $kind, $terminal, $expected, $label) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);
	my $holder = $node0->background_psql('postgres', on_error_die => 1);
	my $waiter = $node1->background_psql('postgres', on_error_die => 1);

	$holder->query_safe('BEGIN');
	if ($kind eq 'data')
	{
		$holder->query_safe("UPDATE $table SET ctr = 200 WHERE ctid = '(0,1)'");
	}
	else
	{
		$holder->query_safe("SELECT ctr FROM $table WHERE ctid = '(0,1)' FOR UPDATE");
	}
	start_blocking_query($waiter, "UPDATE $table SET ctr = ctr + 1 WHERE ctid = '(0,1)'");
	ok(wait_for_wait_event($node1, "%UPDATE $table SET ctr = ctr + 1%", 'GesTxEnqueueWait', 20),
		"$label same-row requester waits on the exact transaction");
	$holder->query_safe(uc($terminal));
	ok(wait_for_value($node1, $table, '(0,1)', $expected, 20),
		"$label requester wakes and requalifies after $terminal");
	$waiter->quit;
	$holder->quit;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'stage8_r3_active_itl',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');
my ($node0, $node1) = ($pair->node0, $pair->node1);
is($node0->safe_psql('postgres', 'SHOW cluster.block_self_contained'), 'off',
	'L1 compatibility setting starts off');

if (make_same_block_table($node0, $node1, 'r3_l1', 'L1 DATA different-row', 2, 1))
{
	different_row_transfer($pair, 'r3_l1', 'data', 'rollback', 'L1 DATA', 1);
}

if (make_same_block_table($node0, $node1, 'r3_l4', 'L4 LOCK_ONLY different-row', 2))
{
	different_row_transfer($pair, 'r3_l4', 'lock', 'rollback', 'L4 LOCK_ONLY');
}

if (make_same_block_table($node0, $node1, 'r3_l2', 'L2 DATA same-row commit', 2))
{
	same_row_wait($pair, 'r3_l2', 'data', 'commit', 201, 'L2 DATA commit');
}

if (make_same_block_table($node0, $node1, 'r3_l3', 'L3 DATA same-row abort', 2))
{
	same_row_wait($pair, 'r3_l3', 'data', 'rollback', 101, 'L3 DATA abort');
}

if (make_same_block_table($node0, $node1, 'r3_l5', 'L5 LOCK_ONLY same-row', 2))
{
	same_row_wait($pair, 'r3_l5', 'lock', 'commit', 101, 'L5 LOCK_ONLY');
}

if (make_same_block_table($node0, $node1, 'r3_l6', 'L6 multi-slot', 3, 1))
{
	my $active0 = pair_state_value($pair, 'xnode_lever', 'g_active_itl_transfer_count');
	my $holder = $node0->background_psql('postgres', on_error_die => 1);
	$holder->query_safe('BEGIN');
	$holder->query_safe('UPDATE r3_l6 SET ctr = 200 WHERE id = 2');
	$holder->query_safe('SELECT ctr FROM r3_l6 WHERE id = 3 FOR UPDATE');
	my ($rc, undef, $err) = $node1->psql('postgres',
		'UPDATE r3_l6 SET ctr = ctr + 1 WHERE id = 1');
	is($rc, 0, 'L6 multi-slot DATA+LOCK_ONLY page transfers through one SourcePrepare');
	unlike($err, qr/53R9H|held in X by a remote|cross-node write/i,
		'L6 multi-slot page did not enter legacy refusal');
	cmp_ok(pair_state_value($pair, 'xnode_lever', 'g_active_itl_transfer_count'), '>', $active0,
		'L6 multi-slot transfer counted once at immutable source preparation');
	$holder->query_safe('ROLLBACK');
	$holder->quit;
}

if (make_same_block_table($node0, $node1, 'r3_l7', 'L7 source commit', 2, 1))
{
	my $skip0 = pair_state_value($pair, 'xnode_lever', 'g_stamp_skipped_count');
	different_row_transfer($pair, 'r3_l7', 'data', 'commit', 'L7 source commit', 1);
	cmp_ok(pair_state_value($pair, 'xnode_lever', 'g_stamp_skipped_count'), '>', $skip0,
		'L7 transferred-away source commit omits the local terminal hint');
	is($node1->safe_psql('postgres', q{SELECT ctr FROM r3_l7 WHERE ctid = '(0,1)'}), '200',
		'L7 TT/CLOG/undo resolves the committed source version');
}

if (make_same_block_table($node0, $node1, 'r3_l8', 'L8 source abort', 2, 1))
{
	different_row_transfer($pair, 'r3_l8', 'data', 'rollback', 'L8 source abort', 1);
	is($node1->safe_psql('postgres', q{SELECT ctr FROM r3_l8 WHERE ctid = '(0,1)'}), '100',
		'L8 TT/CLOG/undo resolves the aborted source version');
}

if (make_same_block_table($node0, $node1, 'r3_l9', 'L9 away-and-back ABA', 2))
{
	different_row_transfer($pair, 'r3_l9', 'lock', 'rollback', 'L9 away leg');
	my $prepare0 = pair_state_value($pair, 'pcm', 'pcm_x_queue_revoke_count');
	is($node0->safe_psql('postgres', 'UPDATE r3_l9 SET ctr = ctr + 1 WHERE id = 1'), '',
		'L9 block returns to the former holder under a fresh ownership round');
	cmp_ok(pair_state_value($pair, 'pcm', 'pcm_x_queue_revoke_count'), '>', $prepare0,
		'L9 return trip used a new canonical SourcePrepare rather than stale proof');
}

for my $setting ('on', 'off')
{
	for my $node ($node0, $node1)
	{
		$node->safe_psql('postgres',
			"ALTER SYSTEM SET cluster.block_self_contained = $setting");
	}
	$pair->stop_pair;
	$pair->start_pair;
	usleep(3_000_000);
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		"L10 peers reconnect after $setting restart");
	is($node0->safe_psql('postgres', 'SHOW cluster.block_self_contained'), $setting,
		"L10 compatibility setting accepts $setting after restart");
	my $table = "r3_l10_$setting";
	if (make_same_block_table($node0, $node1, $table, "L10 GUC $setting", 2))
	{
		different_row_transfer($pair, $table, 'lock', 'rollback', "L10 GUC $setting");
	}
}

$pair->stop_pair;
done_testing();
