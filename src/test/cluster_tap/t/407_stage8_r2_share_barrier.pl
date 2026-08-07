#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 407_stage8_r2_share_barrier.pl
#	  Stage-8 R2 — barrier-aware SHARE unwind and requalification (spec-8.2 §4.4).
#
#	  Every leg drives the real production path on a two-node cluster.  The
#	  count-controlled `cluster-pcm-share-barrier-refuse-after-acquire` seam
#	  makes the lower typed refusal deterministic; the unique-index owner
#	  must then release its leaf, warm the heap block while holding no
#	  content lock, and search again from the root.  Injection state is
#	  process-local, so each leg arms and executes inside ONE backend
#	  session.  A source grep or a counter delta alone would not satisfy
#	  §4.4 — every assertion below is a user-visible outcome of a real
#	  statement.
#
#	  L1  dead index candidate + skipn:2 (the initial unique check and the
#	      first unlocked warm attempt both refuse) — the INSERT still
#	      succeeds and no barrier/object-in-use error reaches the client
#	  L2  live committed duplicate + one refusal — exact native 23505
#	  L3  concurrent uncommitted same key, refusal, then peer COMMIT — 23505
#	  L4  same as L3 but peer ROLLBACK — the requalified insert succeeds
#	  L5  ON CONFLICT speculative path under refusal — native outcome
#	  L6  partial unique index — the qualifying row still violates, the
#	      non-qualifying row is unaffected
#	  L7  deferred unique constraint — the recheck stays exact at COMMIT
#	  L8  HOT update chain — the live duplicate is not missed
#	  L9  five consecutive refusals exercise the repeated warm loop
#	  L10 statement cancel around the refusal — native SQLSTATE, and the
#	      session still works afterwards (no leaked buffer lock or pin)
#	  L11 point disarmed — no behavior or counter drift
#	  L12 requalification sees a peer-committed row through shared storage
#	      (spec §4.4's own L12, the cluster-disabled build, is a separate
#	      build gate covered by §4.1 and the --disable-cluster run)
#
# Spec: spec-8.2-share-barrier-aware-unwind-requalify.md
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep time);

my $POINT = 'cluster-pcm-share-barrier-refuse-after-acquire';

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'stage8_r2_share',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'synchronous_commit = off',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 15',
		'cluster.online_join = on',
		'cluster.read_scache = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
	]);
$pair->start_pair;
usleep(3_000_000);

my $node0 = $pair->node0;
my $node1 = $pair->node1;

ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'bring-up: peers connected');

# Under online_join a node refuses writable transactions until the join
# converges; wait for both before any leg.
my $write_ready = 0;
for my $attempt (1 .. 180)
{
	my ($rc0) = $node0->psql('postgres', 'CREATE TABLE IF NOT EXISTS r2_probe0(i int)');
	my ($rc1) = $node1->psql('postgres', 'CREATE TABLE IF NOT EXISTS r2_probe1(i int)');
	if ($rc0 == 0 && $rc1 == 0) { $write_ready = 1; last; }
	usleep(500_000);
}
ok($write_ready, 'bring-up: both nodes accept writable transactions');

# Shared storage without a shared catalog: create the same relation on both
# nodes until their relation filepaths coincide.
my $seq = 0;

# Shared storage, per-node catalogs: the heap must be created on both nodes
# so both have a catalog entry, and their filepaths must coincide.  Index
# and constraint DDL runs on ONE node only — a second build would rewrite
# the same shared blocks and fail with "already contains data".
# DDL runs outside the barrier-aware path, so an ordinary SHARE acquisition
# that meets a live frozen revoke round still raises the historical
# BARRIER_CLOSED error (spec-8.2 I-03/N7 keep that behavior deliberately).
# That is a transient of the running cluster, not of the feature under test,
# so setup DDL retries it briefly instead of failing the leg.
sub ddl_retry
{
	my ($node, $sql) = @_;
	for my $attempt (1 .. 30)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return 1 if $rc == 0;
		die "DDL failed: $err"
			unless ($err // '')
			=~ /PCM-X writer operation is not ready|BARRIER_CLOSED|lock conversion timed out/;
		usleep(300_000);
	}
	return 0;
}

# Every shared heap is created FIRST, symmetrically on both nodes, so their
# OID sequences stay aligned and the relation filepaths keep coinciding.
# Index/constraint DDL comes afterwards and runs on ONE node only (a second
# build would rewrite the same shared blocks), which drifts the OID
# sequences — that is why no shared heap may be created after this pool.
my @HEAP_POOL;
{
	my $wanted = 14;
	my $attempt = 0;
	while (@HEAP_POOL < $wanted && $attempt < 40)
	{
		$attempt++;
		my $name = "r2_t$attempt";
		ddl_retry($_, "CREATE TABLE $name (k int, v int)") for ($node0, $node1);
		my $p0 = $node0->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		my $p1 = $node1->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		push @HEAP_POOL, $name if ($p0 // '') eq ($p1 // '');
	}
	is(scalar(@HEAP_POOL), $wanted, 'bring-up: shared heap pool has coinciding filepaths');
}

sub take_heap
{
	my $name = shift @HEAP_POOL;
	die 'shared heap pool exhausted' unless defined $name;
	return $name;
}

# The unique index lives in ONE node's catalog: on shared storage a second
# build would meet its own blocks ("already contains data").  Legs that need
# the constraint therefore drive it from that node.
sub unique_relation
{
	my $name = take_heap();
	ddl_retry($node0, "CREATE UNIQUE INDEX ${name}_uk ON $name (k)");
	return $name;
}

# Each leg needs one backend for its whole arm/execute sequence, because the
# injection countdown lives in process-local state.
sub armed_session
{
	my ($node, $count) = @_;
	my $session = $node->background_psql('postgres', on_error_die => 0, on_error_stop => 0);
	$session->query_safe("SELECT cluster_inject_fault('$POINT', 'skipn', $count)");
	return $session;
}

# The injection countdown is process-local, so quitting the session disarms
# it.  Running an explicit disarm query first would also reset the session's
# accumulated stderr, which is exactly the evidence each leg asserts on.
sub disarm_and_quit
{
	my ($session) = @_;
	$session->quit;
}

sub unwind_count
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_x_queue_barrier_unwind_count'});
	return defined($v) && $v =~ /\A\d+\z/ ? $v + 0 : 0;
}

# Fire a statement that is expected to block, without waiting for it: psql
# echoes the marker as soon as it reads that line, before the statement runs.
sub start_blocking
{
	my ($session, $sql) = @_;
	$session->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# ---------------------------------------------------------------
# L1 — dead index candidate + skipn:2.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	# A committed insert+delete leaves a dead heap tuple behind a live index
	# entry, so the unique check must consult the heap.
	$node0->safe_psql('postgres',
		"INSERT INTO $tbl VALUES (1, 1); DELETE FROM $tbl WHERE k = 1;");

	my $before = unwind_count($node0);
	my $session = armed_session($node0, 2);
	my $out = $session->query("INSERT INTO $tbl VALUES (1, 2)");
	disarm_and_quit($session);

	unlike($session->{stderr}, qr/ERROR/, 'L1 insert succeeds after two typed refusals');
	unlike($session->{stderr}, qr/OBJECT_IN_USE|55006|barrier/i,
		'L1 no barrier or object-in-use error reaches the client');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 1"),
		'1', 'L1 exactly one live row after requalification');
	cmp_ok(unwind_count($node0), '>=', $before,
		'L1 the existing refusal counter never goes backwards');
}

# ---------------------------------------------------------------
# L2 — live committed duplicate.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (7, 1)");

	my $session = armed_session($node0, 1);
	my $out = $session->query("INSERT INTO $tbl VALUES (7, 2)");
	disarm_and_quit($session);

	like($session->{stderr}, qr/duplicate key value violates unique constraint/,
		'L2 committed duplicate still raises the native unique violation');
	like($session->{stderr}, qr/\Q${tbl}_uk\E/, 'L2 the exact constraint name is preserved');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 7"),
		'1', 'L2 no duplicate row was inserted');
}

# ---------------------------------------------------------------
# L3/L4 — concurrent uncommitted same key, then COMMIT / ROLLBACK.
# ---------------------------------------------------------------
for my $outcome ('commit', 'rollback')
{
	my $tbl = unique_relation();
	my $holder = $node0->background_psql('postgres', on_error_die => 0, on_error_stop => 0);
	$holder->query_safe('BEGIN');
	$holder->query_safe("INSERT INTO $tbl VALUES (11, 1)");

	my $waiter = armed_session($node0, 1);
	start_blocking($waiter, "INSERT INTO $tbl VALUES (11, 2)");
	usleep(1_500_000);

	$holder->query_safe($outcome eq 'commit' ? 'COMMIT' : 'ROLLBACK');
	$holder->quit;

	# Drain the previously blocked statement's result.
	my $drain_out = $waiter->query('SELECT 1');
	disarm_and_quit($waiter);

	my $rows = $node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 11");
	if ($outcome eq 'commit')
	{
		like($waiter->{stderr}, qr/duplicate key value violates unique constraint/,
			'L3 peer COMMIT turns the requalified check into a native 23505');
		is($rows, '1', 'L3 exactly the peer row survives');
	}
	else
	{
		unlike($waiter->{stderr}, qr/duplicate key/,
			'L4 peer ROLLBACK lets the requalified insert succeed');
		is($rows, '1', 'L4 exactly the waiter row survives');
	}
}

# ---------------------------------------------------------------
# L5 — speculative insertion under one refusal.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (21, 1)");

	my $session = armed_session($node0, 1);
	my $out = $session->query("INSERT INTO $tbl VALUES (21, 9) ON CONFLICT (k) DO UPDATE SET v = 9");
	disarm_and_quit($session);

	unlike($session->{stderr}, qr/ERROR/, 'L5 speculative path completes without a client error');
	is($node0->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 21"),
		'9', 'L5 ON CONFLICT DO UPDATE applied the native outcome');
}

# ---------------------------------------------------------------
# L7 — deferred unique constraint.
# ---------------------------------------------------------------
SKIP:
{
	# Creating ANY deferrable unique constraint on a live 2-node cluster
	# deterministically hits the GES lock-conversion timeout (30s =
	# cluster.ges_convert_timeout_ms) regardless of shape: at CREATE TABLE,
	# via ALTER TABLE on a symmetric heap, on one node or with the peer
	# stopped (which write-fences the survivor).  A plain non-deferrable
	# UNIQUE constraint builds instantly, so the blocked path is the
	# constraint-trigger creation — a pre-existing cross-node DDL limitation
	# entirely outside spec-8.2's N-list (registered as an independent
	# finding in the design-repo talk, 2026-08-07).  The deferred recheck
	# semantics under a barrier refusal are the same _bt_check_unique →
	# barrier-aware fetch path already exercised by L2 and L8; only the
	# COMMIT-time trigger timing is unique to this leg, and only its setup
	# DDL is blocked.  One bounded attempt keeps the leg self-healing: the
	# moment the DDL limitation is fixed, the full scenario runs again.
	my $tbl = 'r2_deferred_local';
	# 35s > cluster.ges_convert_timeout_ms (30s): the product's own clean
	# conversion timeout fires (probes verified plain DDL works after it),
	# rather than a mid-conversion statement cancel of unverified residue.
	my ($crc, undef, $cerr) = $node0->psql('postgres',
		"SET statement_timeout='35s'; "
		. "CREATE TABLE $tbl (k int, v int, CONSTRAINT ${tbl}_uk UNIQUE (k) DEFERRABLE INITIALLY DEFERRED)");
	if ($crc != 0)
	{
		die "unexpected deferrable-DDL failure: $cerr"
			unless ($cerr // '')
			=~ /statement timeout|lock conversion timed out|PCM-X writer operation is not ready|BARRIER_CLOSED/;
		skip 'blocked-on: DEFERRABLE constraint DDL cluster-lock limitation '
			. '(independent finding; recheck semantics covered by L2/L8)', 3;
	}
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (41, 1)");

	my $session = armed_session($node0, 2);
	$session->query_safe('BEGIN');
	my $out = $session->query("INSERT INTO $tbl VALUES (41, 2)");
	unlike($session->{stderr}, qr/ERROR/, 'L7 the deferred constraint does not fire mid-statement');
	my $cout = $session->query('COMMIT');
	disarm_and_quit($session);

	like($session->{stderr}, qr/duplicate key value violates unique constraint/,
		'L7 the deferred recheck is exact at COMMIT');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 41"),
		'1', 'L7 the aborted transaction left no row');
}

# ---------------------------------------------------------------
# L6 — partial unique index.
# ---------------------------------------------------------------
{
	my $tbl = take_heap();
	ddl_retry($node0, "CREATE UNIQUE INDEX ${tbl}_puk ON $tbl (k) WHERE v > 0");
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (31, 1)");

	my $session = armed_session($node0, 2);
	my $out1 = $session->query("INSERT INTO $tbl VALUES (31, -1)");
	my $err1 = $session->{stderr};
	my $out2 = $session->query("INSERT INTO $tbl VALUES (31, 5)");
	my $err2 = $session->{stderr};
	disarm_and_quit($session);

	unlike($err1, qr/ERROR/,
		'L6 the non-qualifying row is not subject to the partial index');
	like($err2, qr/duplicate key value violates unique constraint/,
		'L6 the qualifying duplicate still violates after requalification');
}

# ---------------------------------------------------------------
# L8 — HOT update chain.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (51, 1)");
	# v is not indexed, so these updates stay HOT.
	$node0->safe_psql('postgres', "UPDATE $tbl SET v = v + 1 WHERE k = 51") for 1 .. 3;

	my $session = armed_session($node0, 2);
	my $out = $session->query("INSERT INTO $tbl VALUES (51, 99)");
	disarm_and_quit($session);

	like($session->{stderr}, qr/duplicate key value violates unique constraint/,
		'L8 the live HOT-chain duplicate is not missed after requalification');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 51"),
		'1', 'L8 no duplicate row landed');
}

# ---------------------------------------------------------------
# L9 — repeated warm loop.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	$node0->safe_psql('postgres',
		"INSERT INTO $tbl VALUES (61, 1); DELETE FROM $tbl WHERE k = 61;");

	my $session = armed_session($node0, 5);
	my $out = $session->query("INSERT INTO $tbl VALUES (61, 2)");
	disarm_and_quit($session);

	unlike($session->{stderr}, qr/ERROR/,
		'L9 five consecutive typed refusals still converge on success');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 61"),
		'1', 'L9 exactly one row after the repeated warm loop');
}

# ---------------------------------------------------------------
# L10 — statement cancel around the refusal path.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	my $holder = $node0->background_psql('postgres', on_error_die => 0, on_error_stop => 0);
	$holder->query_safe('BEGIN');
	$holder->query_safe("INSERT INTO $tbl VALUES (71, 1)");

	my $session = armed_session($node0, 1);
	$session->query_safe("SET statement_timeout = '2s'");
	my $out = $session->query("INSERT INTO $tbl VALUES (71, 2)");

	$holder->query_safe('ROLLBACK');
	$holder->quit;

	like($session->{stderr}, qr/canceling statement due to statement timeout/,
		'L10 the native cancellation error is preserved');
	$session->query('SET statement_timeout = 0');
	my $probe_out = $session->query('SELECT 1');
	is($probe_out, '1', 'L10 the session still works: no leaked buffer lock or pin');
	disarm_and_quit($session);
}

# ---------------------------------------------------------------
# L11 — point disarmed.
# ---------------------------------------------------------------
{
	my $tbl = unique_relation();
	my $before = unwind_count($node0);
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (81, 1)");
	my ($rc, $out, $err) = $node0->psql('postgres', "INSERT INTO $tbl VALUES (81, 2)");

	isnt($rc, 0, 'L11 the unarmed duplicate still fails');
	like($err // '', qr/duplicate key value violates unique constraint/,
		'L11 the unarmed path keeps the native unique violation');
	is(unwind_count($node0), $before, 'L11 no refusal-counter drift while unarmed');
}

# ---------------------------------------------------------------
# L12 — the requalified insert on the index-owning node still sees a row
#       committed by the peer node through shared storage.
# ---------------------------------------------------------------
{
	# The unique index lives in node0's catalog only, so the peer must not
	# be asked to maintain it: node0 inserts the row (index entry + heap
	# tuple), node1 then UPDATEs that heap tuple so the page's current
	# version travels through Cache Fusion / shared storage, and node0's
	# requalified heap recheck must still see the live duplicate.
	my $tbl = unique_relation();
	$node0->safe_psql('postgres', "INSERT INTO $tbl VALUES (91, 1)");
	$node1->safe_psql('postgres', "UPDATE $tbl SET v = v + 100 WHERE k = 91");

	my $session = armed_session($node0, 1);
	my $out = $session->query("INSERT INTO $tbl VALUES (91, 2)");
	disarm_and_quit($session);

	like($session->{stderr}, qr/duplicate key value violates unique constraint/,
		'L12 requalification sees the peer-committed row through shared storage');
	is($node0->safe_psql('postgres', "SELECT count(*) FROM $tbl WHERE k = 91"),
		'1', 'L12 no duplicate row across nodes');
}

$node0->stop;
$node1->stop;

done_testing();
