#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 411_ges_dedup_mixed_binary_2node.pl
#    S3-P0-10 -- real old/new binary compatibility for GES dedup lifecycle.
#
#    This test is intentionally environment-gated because a normal source
#    tree has only one installation.  PGRAC_NEW_PREFIX names the binary with
#    GES_DEDUP_DONE/ACK support; PGRAC_OLD_PREFIX names a pre-protocol binary.
#
#    Contract:
#      M1  real old and new postmasters form one CONTROL mesh;
#      M2  new->old requests execute normally but never reserve/send DONE;
#      M3  old->new requests execute normally and remain legacy-pinned at the
#          new receiver (the old peer cannot send DONE);
#      M4  no ACK is observed, no lifecycle journal row is created, and the
#          mixed link does not reconnect merely because the peer lacks types
#          68/69.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/411_ges_dedup_mixed_binary_2node.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my $new_prefix = $ENV{PGRAC_NEW_PREFIX} // '';
my $old_prefix = $ENV{PGRAC_OLD_PREFIX} // '';

plan skip_all => 'set PGRAC_NEW_PREFIX and PGRAC_OLD_PREFIX for real mixed binaries'
  unless $new_prefix ne '' && $old_prefix ne '';
plan skip_all => "new postgres missing under $new_prefix"
  unless -x "$new_prefix/bin/postgres";
plan skip_all => "old postgres missing under $old_prefix"
  unless -x "$old_prefix/bin/postgres";

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

sub ges_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql(
		'postgres', qq{
		SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
		WHERE category = 'ges' AND key = '$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

sub wait_peer
{
	my ($node, $peer, $seconds) = @_;
	return poll_until(
		sub {
			my ($rc, $out) = $node->psql(
				'postgres',
				"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $peer");
			defined($rc) && $rc == 0 && defined($out) && $out eq 'connected';
		},
		$seconds);
}

my $disk_dir = PostgreSQL::Test::Utils::tempdir();
my @disks;
for my $i (0 .. 2)
{
	my $path = "$disk_dir/disk$i";
	open(my $fh, '>', $path) or die "open $path: $!";
	binmode $fh;
	print $fh ("\0" x (2 * 128 * 512));
	close $fh;
	push @disks, $path;
}
my $disks_csv = join(',', @disks);

my $pg0 = PostgreSQL::Test::Cluster::get_free_port();
my $pg1 = PostgreSQL::Test::Cluster::get_free_port();
my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();
my $data0 = PostgreSQL::Test::Cluster::get_free_port_range(2);
my $data1 = PostgreSQL::Test::Cluster::get_free_port_range(2);

my $new = PostgreSQL::Test::Cluster->new(
	'ges_dedup_mixed_new', port => $pg0, install_path => $new_prefix);
my $old = PostgreSQL::Test::Cluster->new(
	'ges_dedup_mixed_old', port => $pg1, install_path => $old_prefix);
$new->init;
$old->init;

my $common = <<EOC;
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.grd_max_entries = 1024
cluster.ges_dedup_max_entries = 256
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
shared_buffers = 16MB
autovacuum = off
EOC
$new->append_conf('postgresql.conf', $common);
$old->append_conf('postgresql.conf', $common);
$new->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$old->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = ges_dedup_mixed_binary

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data1
EOC
PostgreSQL::Test::Utils::append_to_file(
	$new->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file(
	$old->data_dir . '/pgrac.conf', $pgrac_conf);

$new->start;
$old->start;

ok(wait_peer($new, 1, 30), 'M1 new binary sees old peer connected');
ok(wait_peer($old, 0, 30), 'M1 old binary sees new peer connected');
is($new->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		  WHERE category='ges'
		    AND key IN ('ges_dedup_entry_count',
		                'ges_dedup_journal_count',
		                'ges_dedup_journal_ack_count')}),
	'3', 'M1 new binary exposes the lifecycle observability rows');

my $journal0 = ges_int($new, 'ges_dedup_journal_count');
my $ack0 = ges_int($new, 'ges_dedup_journal_ack_count');
my $full0 =
  ges_int($new, 'ges_dedup_full_reject_count')
  + ges_int($new, 'ges_dedup_journal_full_count');
is($journal0, 0, 'M1 new requester journal starts empty');
is($ack0, 0, 'M1 no lifecycle ACK has been accepted');

my ($old_master_key, $new_master_key);
for my $key (41101 .. 41164)
{
	my $entries_before = ges_int($new, 'ges_dedup_entry_count');
	my $probe = $old->background_psql('postgres', on_error_die => 1);
	$probe->query_safe("SELECT pg_advisory_lock($key)");
	my $remote_to_new = poll_until(
		sub {
			ges_int($new, 'ges_dedup_entry_count') > $entries_before;
		},
		1);
	is($probe->query_safe("SELECT pg_advisory_unlock($key)"),
		't', "M2/M3 released old-binary master probe key $key");
	$probe->quit;

	if ($remote_to_new)
	{
		$new_master_key //= $key;
	}
	else
	{
		$old_master_key //= $key;
	}
	last if defined($old_master_key) && defined($new_master_key);
}

ok(defined($old_master_key),
	'M2 found a key mastered by the old binary');
ok(defined($new_master_key),
	'M3 found a key mastered by the new binary');

my $new_reconnect_before = int($new->safe_psql(
	'postgres',
	q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = 1}));
my $old_reconnect_before = int($old->safe_psql(
	'postgres',
	q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = 0}));

SKIP:
{
	skip 'no old-master key found', 4 unless defined($old_master_key);

	my $holder = $old->background_psql('postgres', on_error_die => 1);
	$holder->query_safe("SELECT pg_advisory_lock($old_master_key)");
	my $try = $new->background_psql('postgres', on_error_die => 1);
	is($try->query_safe("SELECT pg_try_advisory_lock($old_master_key)"),
		'f', 'M2 new->old remote NOWAIT still observes mutual exclusion');
	$try->quit;
	is(ges_int($new, 'ges_dedup_journal_count'), 0,
		'M2 new requester creates no journal row for a capability-less old peer');
	is(ges_int($new, 'ges_dedup_journal_ack_count'), 0,
		'M2 new requester accepts no ACK from the old peer');
	is($holder->query_safe("SELECT pg_advisory_unlock($old_master_key)"),
		't', 'M2 old-master holder releases cleanly');
	$holder->quit;
}

SKIP:
{
	skip 'no new-master key found', 4 unless defined($new_master_key);

	my $entries_before = ges_int($new, 'ges_dedup_entry_count');
	my $holder = $new->background_psql('postgres', on_error_die => 1);
	$holder->query_safe("SELECT pg_advisory_lock($new_master_key)");
	my $try = $old->background_psql('postgres', on_error_die => 1);
	is($try->query_safe("SELECT pg_try_advisory_lock($new_master_key)"),
		'f', 'M3 old->new remote NOWAIT still observes mutual exclusion');
	$try->quit;
	ok(poll_until(
			sub {
				ges_int($new, 'ges_dedup_entry_count') > $entries_before;
			},
			5),
		'M3 old request remains legacy-pinned at the new receiver');
	is($holder->query_safe("SELECT pg_advisory_unlock($new_master_key)"),
		't', 'M3 new-master holder releases cleanly');
	$holder->quit;
	is(ges_int($new, 'ges_dedup_journal_ack_count'), 0,
		'M3 old binary never sends a lifecycle ACK');
}

is(ges_int($new, 'ges_dedup_journal_count'), 0,
	'M4 new requester journal stays empty across real mixed traffic');
is(ges_int($new, 'ges_dedup_journal_ack_count'), 0,
	'M4 ACK count stays zero across real mixed traffic');
is( ges_int($new, 'ges_dedup_full_reject_count')
	  + ges_int($new, 'ges_dedup_journal_full_count'),
	$full0, 'M4 mixed traffic causes no lifecycle capacity rejection');
is(int($new->safe_psql(
		'postgres',
		q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = 1})),
	$new_reconnect_before,
	'M4 new binary did not reconnect because the old peer lacks DONE/ACK');
is(int($old->safe_psql(
		'postgres',
		q{SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = 0})),
	$old_reconnect_before,
	'M4 old binary did not reconnect on mixed traffic');
is($new->safe_psql('postgres', 'SELECT 1'), '1',
	'M4 new binary remains SQL-healthy');
is($old->safe_psql('postgres', 'SELECT 1'), '1',
	'M4 old binary remains SQL-healthy');

$new->stop;
$old->stop;
done_testing();
