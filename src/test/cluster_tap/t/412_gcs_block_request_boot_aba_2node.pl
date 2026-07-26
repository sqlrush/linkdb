#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 412_gcs_block_request_boot_aba_2node.pl
#    S3-P0-18 -- ordinary current-boot GCS BLOCK_REQUEST/DONE ABA closure.
#
# A requester postmaster restart resets its per-backend raw request sequence.
# The receiver dedup identity must therefore include the requester's durable
# boot incarnation.  This gate forces the same numeric request ids before and
# after a fast requester restart and proves both the same-tag stale-cache and
# different-tag validator-collision cases become fresh misses under boot B.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'GCS request boot identity requires --enable-cluster';
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

sub gcs_int_session
{
	my ($session, $key) = @_;
	return int($session->query_safe(qq{
		SELECT value::bigint FROM pg_cluster_state
		WHERE category = 'gcs' AND key = '$key'}));
}

sub poll_sql_eq
{
	my ($node, $sql, $want, $seconds) = @_;
	return poll_until(
		sub {
			my ($rc, $out) = $node->psql('postgres', $sql);
			return defined($rc) && $rc == 0 && defined($out) && $out eq $want;
		},
		$seconds);
}

sub poll_write_ok
{
	my ($node, $seconds) = @_;
	return poll_until(
		sub {
			my ($rc, $out) = $node->psql(
				'postgres', q{SELECT txid_current() > 0});
			return defined($rc) && $rc == 0 && defined($out) && $out eq 't';
		},
		$seconds);
}

sub backend_id
{
	my ($session) = @_;
	return int($session->query_safe(q{
		SELECT b.backendid
		FROM pg_stat_get_backend_idset() AS b(backendid)
		WHERE pg_stat_get_backend_pid(b.backendid) = pg_backend_pid()}));
}

sub requester_id
{
	my ($node_id, $backend_id, $sequence) = @_;
	return ($node_id << 56) | (($backend_id - 1) << 40) | $sequence;
}

sub membership_field
{
	my ($node, $node_id, $field) = @_;
	die "invalid membership field $field"
	  unless $field eq 'presented_incarnation'
	  || $field eq 'last_admitted_incarnation'
	  || $field eq 'admitted_epoch';
	return int($node->safe_psql(
		'postgres',
		"SELECT $field FROM pg_cluster_membership WHERE node_id = $node_id"));
}

sub parse_debug_exact
{
	my ($out) = @_;
	my @v = split(/\|/, $out, -1);
	return undef unless @v == 16;
	return {
		found => int($v[0]), match_count => int($v[1]),
		worker => int($v[2]), rel => int($v[3]), fork => int($v[4]),
		block => int($v[5]), kind => int($v[6]), transition => int($v[7]),
		status => int($v[8]), completed => int($v[9]), done => int($v[10]),
		hit => int($v[11]), miss => int($v[12]), collision => int($v[13]),
		done_marked => int($v[14]), done_mismatch => int($v[15]),
	};
}

sub debug_exact
{
	my ($node, $backend_id, $request_id, $epoch, $boot) = @_;
	my ($rc, $out) = $node->psql(
		'postgres', qq{
		SELECT (CASE WHEN found THEN 1 ELSE 0 END)::text || '|' ||
		       match_count || '|' || worker_id || '|' ||
		       rel_number || '|' || fork_num || '|' || block_num || '|' ||
		       entry_kind || '|' || transition_id || '|' || status || '|' ||
		       (CASE WHEN completed THEN 1 ELSE 0 END)::text || '|' ||
		       (CASE WHEN done THEN 1 ELSE 0 END)::text || '|' ||
		       hit_count || '|' || miss_count || '|' || collision_count || '|' ||
		       done_marked_count || '|' || done_mismatch_count
		FROM pg_cluster_gcs_block_dedup_debug_exact(
		     1, $backend_id, $request_id, $epoch, $boot)});
	return undef unless defined($rc) && $rc == 0 && defined($out);
	return parse_debug_exact($out);
}

sub debug_exact_session
{
	my ($session, $backend_id, $request_id, $epoch, $boot) = @_;
	my $out = $session->query_safe(qq{
		SELECT (CASE WHEN found THEN 1 ELSE 0 END)::text || '|' ||
		       match_count || '|' || worker_id || '|' ||
		       rel_number || '|' || fork_num || '|' || block_num || '|' ||
		       entry_kind || '|' || transition_id || '|' || status || '|' ||
		       (CASE WHEN completed THEN 1 ELSE 0 END)::text || '|' ||
		       (CASE WHEN done THEN 1 ELSE 0 END)::text || '|' ||
		       hit_count || '|' || miss_count || '|' || collision_count || '|' ||
		       done_marked_count || '|' || done_mismatch_count
		FROM pg_cluster_gcs_block_dedup_debug_exact(
		     1, $backend_id, $request_id, $epoch, $boot)});
	return parse_debug_exact($out);
}

sub read_page_with_sequence
{
	my ($session, $sequence, $block) = @_;
	$session->query_safe(
		"SET cluster.unsafe_test_gcs_block_next_request_sequence = $sequence");
	return $session->query_safe(
		"SELECT count(*) FROM p018_boot_t "
		  . "WHERE ctid = '($block,1)'::tid");
}

sub wait_debug_done
{
	my ($observer, $backend_id, $request_id, $epoch, $boot, $seconds) = @_;
	my $last;
	my $ok = poll_until(
		sub {
			$last = debug_exact_session(
				$observer, $backend_id, $request_id, $epoch, $boot);
			return defined($last) && $last->{found} == 1
			  && $last->{match_count} == 1
			  && $last->{completed} == 1 && $last->{done} == 1;
		},
		$seconds);
	return ($ok, $last);
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'gcs_request_boot_aba',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 1',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 30000',
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
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L0 node0 sees requester connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L0 requester sees node0 connected');
ok(poll_sql_eq(
		$node0,
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1},
		'member', 60),
	'L0 requester is a member');

my $debug_assertions = $node0->safe_psql('postgres', 'SHOW debug_assertions');
is($debug_assertions, 'on', 'L0 assertion build exposes the exact-probe body');
is($node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_proc
		  WHERE oid = 8978
		    AND proname = 'pg_cluster_gcs_block_dedup_debug_exact'}),
	'1', 'L0 OID8978 exact-probe catalog row exists');

my ($nonsuper_rc, undef, $nonsuper_err) = $node0->psql(
	'postgres', q{
	SET ROLE pg_read_all_data;
	SELECT found FROM pg_cluster_gcs_block_dedup_debug_exact(1,1,1,1,1)});
isnt($nonsuper_rc, 0, 'L0 non-superuser exact probe is rejected');
like($nonsuper_err // '', qr/must be superuser/,
	'L0 non-superuser rejection names the privilege gate');

my $caps = $node1->safe_psql(
	'postgres', q{
	SELECT value FROM pg_cluster_state
	WHERE category = 'ic' AND key = 'peer_capabilities'});
my ($cap_hex) = $caps =~ /n0:bits=0x([0-9A-Fa-f]+),gen=[1-9][0-9]*,v=1/;
ok(defined($cap_hex) && (hex($cap_hex) & 0x00010000) != 0,
	'L0 requester current HELLO advertises GCS_REQUEST_BOOT_V1');
ok(poll_write_ok($node0, 90), 'L0 node0 write gate is open');
ok(poll_write_ok($node1, 90), 'L0 requester write gate is open');

# Create the same catalog relation on both nodes with the same physical
# relNumber, then seed its shared block-device bytes from node0.
$node0->safe_psql('postgres', 'CREATE TABLE p018_align0 (x int)');
$node1->safe_psql('postgres', 'CREATE TABLE p018_align1 (x int)');
my ($path0, $path1) = ('', '');
for my $attempt (1 .. 8)
{
	$node0->safe_psql(
		'postgres',
		'CREATE TABLE p018_boot_t (id int, pad bigint) WITH (fillfactor = 50)');
	$node1->safe_psql(
		'postgres',
		'CREATE TABLE p018_boot_t (id int, pad bigint) WITH (fillfactor = 50)');
	$path0 = $node0->safe_psql(
		'postgres', q{SELECT pg_relation_filepath('p018_boot_t')});
	$path1 = $node1->safe_psql(
		'postgres', q{SELECT pg_relation_filepath('p018_boot_t')});
	last if $path0 eq $path1;
	my ($r0) = $path0 =~ /(\d+)$/;
	my ($r1) = $path1 =~ /(\d+)$/;
	my ($lag, $burn) = $r0 < $r1 ? ($node0, $r1 - $r0) : ($node1, $r0 - $r1);
	$lag->safe_psql(
		'postgres',
		"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	$node0->safe_psql('postgres', 'DROP TABLE p018_boot_t');
	$node1->safe_psql('postgres', 'DROP TABLE p018_boot_t');
}
is($path0, $path1, 'L0 requester and master relation paths coincide');
my $seeded = poll_sql_eq(
		$node0,
		q{WITH seeded AS (
		    INSERT INTO p018_boot_t
		    SELECT g, g FROM generate_series(1, 20000) g
		    RETURNING 1)
		  SELECT count(*) FROM seeded},
		'20000', 90);
ok($seeded,
	'L0 seed write survives transient per-tag rejoin recovery');
BAIL_OUT('could not seed the shared test relation') unless $seeded;
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'CHECKPOINT');

# Keep one warm observer backend on the master for all exact probes and
# master-side counter snapshots.  Reopening an observer for every poll would
# make its own cold catalog reads part of the dedup workload under test.
my $observer = $node0->background_psql('postgres', on_error_die => 1);
$observer->query_safe(q{
	SELECT count(*) FROM pg_cluster_state WHERE category = 'gcs'});
$observer->query_safe(q{
	SELECT found
	FROM pg_cluster_gcs_block_dedup_debug_exact(1, 1, 1, 0, 1)});

ok(poll_until(
		sub {
			return membership_field($node0, 1, 'presented_incarnation') > 0;
		},
		30),
	'L1 qvotec publishes requester boot A');
my $boot_a = membership_field($node0, 1, 'presented_incarnation');
my $epoch_a = membership_field($node0, 1, 'admitted_epoch');
cmp_ok($boot_a, '>', 0, 'L1 boot A is nonzero');
cmp_ok($epoch_a, '>=', 0,
	'L1 captured request epoch (initial epoch zero is valid)');

my $session_a = $node1->background_psql('postgres', on_error_die => 1);
my $backend_a = backend_id($session_a);
cmp_ok($backend_a, '>', 0, 'L1 captured requester MyBackendId');
$session_a->query_safe('SELECT 1 FROM p018_boot_t LIMIT 0');

my @remote;
for my $block (0 .. 127)
{
	my $sequence = 100000 + $block;
	my $request_id = requester_id(1, $backend_a, $sequence);
	read_page_with_sequence($session_a, $sequence, $block);
	my ($done, $snapshot) = wait_debug_done(
		$observer, $backend_a, $request_id, $epoch_a, $boot_a, 2);
	next unless $done;
	push @remote, {
		block => $block, sequence => $sequence, request_id => $request_id,
		snapshot => $snapshot,
	};
	last if @remote == 3;
}
is(scalar(@remote), 3,
	'L1 found three distinct node0-mastered ordinary request tags under boot A');
BAIL_OUT('could not find three remote-mastered request tags') unless @remote == 3;
isnt($remote[0]{snapshot}{block}, $remote[1]{snapshot}{block},
	'L1 target A entries have distinct tags');
ok($remote[0]{snapshot}{done} && $remote[1]{snapshot}{done},
	'L1 both target A entries consumed exact DONE');
$session_a->quit;

# Restart before the long CSSD deadband expires.  The cluster epoch must stay
# fixed; only the requester's durable boot incarnation changes.
$node1->restart;
ok($pair->wait_for_peer_state(0, 1, 'connected', 60),
	'L2 node0 reconnects to restarted requester');
ok($pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L2 restarted requester reconnects to node0');
ok(poll_until(
		sub {
			return membership_field($node0, 1, 'presented_incarnation') > $boot_a;
		},
		60),
	'L2 qvotec publishes fresh boot B');
my $boot_b = membership_field($node0, 1, 'presented_incarnation');
my $epoch_b = membership_field($node0, 1, 'admitted_epoch');
cmp_ok($boot_b, '>', $boot_a, 'L2 boot B differs and advances');
is($epoch_b, $epoch_a, 'L2 fast restart preserves cluster epoch');

$caps = $node1->safe_psql(
	'postgres', q{
	SELECT value FROM pg_cluster_state
	WHERE category = 'ic' AND key = 'peer_capabilities'});
($cap_hex) = $caps =~ /n0:bits=0x([0-9A-Fa-f]+),gen=[1-9][0-9]*,v=1/;
ok(defined($cap_hex) && (hex($cap_hex) & 0x00010000) != 0,
	'L2 boot-B connection still advertises GCS_REQUEST_BOOT_V1');

my $session_b = $node1->background_psql('postgres', on_error_die => 1);
my $backend_b = backend_id($session_b);
is($backend_b, $backend_a,
	'L2 restarted requester reuses the same numeric MyBackendId');
BAIL_OUT("requester BackendId changed A=$backend_a B=$backend_b")
  unless $backend_b == $backend_a;
$session_b->query_safe('SELECT 1 FROM p018_boot_t LIMIT 0');

# Requester counter reads are themselves ordinary node1 catalog work.  Sample
# them before the master-side baseline, then sample the master miss counter
# last so all observer warm-up traffic is outside the exact two-leg window.
my $sent_before = gcs_int_session($session_b, 'done_sent_count');
my $drop_before = gcs_int_session($session_b, 'done_enqueue_drop_count');
my $hit_before = gcs_int_session($observer, 'dedup_hit_count');
my $collision_before = gcs_int_session($observer, 'dedup_collision_count');
my $marked_before = gcs_int_session($observer, 'dedup_done_marked_count');
my $mismatch_before = gcs_int_session($observer, 'dedup_done_mismatch_count');
my $miss_before = gcs_int_session($observer, 'dedup_miss_count');

# Same request id + same tag, now under boot B.
read_page_with_sequence(
	$session_b, $remote[0]{sequence}, $remote[0]{block});
my ($same_done, $same_b) = wait_debug_done(
	$observer, $backend_b, $remote[0]{request_id}, $epoch_b, $boot_b, 10);
ok($same_done, 'L3 same-id/same-tag under boot B is a completed fresh entry');

# Same request id as old target 2, but a different already-proven remote tag.
read_page_with_sequence(
	$session_b, $remote[1]{sequence}, $remote[2]{block});
my ($diff_done, $diff_b) = wait_debug_done(
	$observer, $backend_b, $remote[1]{request_id}, $epoch_b, $boot_b, 10);
ok($diff_done, 'L3 same-id/different-tag under boot B is a completed fresh entry');
isnt($diff_b->{block}, $remote[1]{snapshot}{block},
	'L3 different-tag leg changed the stored block tag');

my $old_same = debug_exact_session(
	$observer, $backend_a, $remote[0]{request_id}, $epoch_a, $boot_a);
my $old_diff = debug_exact_session(
	$observer, $backend_a, $remote[1]{request_id}, $epoch_a, $boot_a);
ok($old_same->{found} && $same_b->{found},
	'L4 boot A and boot B same-tag keys coexist exactly');
ok($old_diff->{found} && $diff_b->{found},
	'L4 boot A and boot B different-tag keys coexist exactly');
is($same_b->{block}, $old_same->{block},
	'L4 same-tag A/B entries name the same block');

my $hit_after = gcs_int_session($observer, 'dedup_hit_count');
my $collision_after = gcs_int_session($observer, 'dedup_collision_count');
my $marked_after = gcs_int_session($observer, 'dedup_done_marked_count');
my $mismatch_after = gcs_int_session($observer, 'dedup_done_mismatch_count');
my $miss_after = gcs_int_session($observer, 'dedup_miss_count');
my $sent_after = gcs_int_session($session_b, 'done_sent_count');
my $drop_after = gcs_int_session($session_b, 'done_enqueue_drop_count');

is($miss_after - $miss_before, 2,
	'L5 boot-B legs add exactly two dedup misses');
is($collision_after - $collision_before, 0,
	'L5 boot-B legs add no validator collision');
is($hit_after - $hit_before, 0,
	'L5 no stale boot-A cached reply is hit');
cmp_ok($marked_after - $marked_before, '>=', 2,
	'L5 master marks both boot-B DONE proofs');
is($mismatch_after - $mismatch_before, 0,
	'L5 master records no DONE mismatch');
cmp_ok($sent_after - $sent_before, '>=', 2,
	'L5 requester sends both boot-B DONE proofs');
is($drop_after - $drop_before, 0,
	'L5 requester drops no boot-B DONE enqueue');

$session_b->quit;
$observer->quit;
$pair->stop_pair;
done_testing();
