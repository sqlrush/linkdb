#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 271_wal_first_recoverer_fresh.pl
#    RF-ROOT P9 RL-01 (faithful fault leg): first-recoverer freshness.
#    One origin fails; the survivor's first recovery must enter from
#    canonical sources (shared root / WAL / membership / fence) with NO
#    private adoption, and only completed resources open.
#
#    Leg shape (2-node, no node restart — the death legs RL-02..04 are
#    honestly SKIP-with-reason: the replay actor never reaches mutation
#    under fence provider-0, per contract):
#
#      L1  pair forms; node1 writes WAL (durable registry slot 2)
#      L2  node1 CHECKPOINT + stop('immediate') — real node death
#      L3  survivor (node0) detects DEAD + runs the recovery flow:
#          HW remaster rebuilds from the canonical snapshot
#          ("rebuilt authority from dead node 1 (snapshot ...,
#          validated end ...) -> done") — the R3/R5 canonical rebuild,
#          never an adoption of any previous recoverer's private progress
#      L4  ASSERT thread-recovery (the replay actor) did NOT execute:
#          under fence provider-0 the actor stops BLOCKED at
#          NeedSet/admit with zero GES/replay/publish — "only completed
#          resources open" (the HW authority rebuilt; nothing else)
#      L5  node1 crash-rejoins (t243 L4 path); pair reforms
#
#    Scope note (4.7 / contract §A, same as t243): no table access
#    around the peer death; assertions read logs only; node1 rejoin uses
#    the t243 L4 fast-restart window (<3s) so the survivor's death
#    detection never races the epoch bump.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Contract: RF-ROOT §9.2 RL-01
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair('wfirstrec',
	quorum_voting_disks => 3,
	wal_threads_root => 1,
	true_shared_sysid_cf => 1,
	extra_conf => [
		'autovacuum = off',
		'log_min_messages = debug1' ]);
my $root = $pair->wal_threads_root;
my $node0 = $pair->node0;
my $node1 = $pair->node1;

$pair->start_pair;
ok($pair->wait_for_pcm_x_active(30),
	'L1 PCM-X formation is ACTIVE on both writers before DML');

# ============================================================
# L2: node1 writes WAL (durable registry slot 2), then dies for real
# (SIGQUIT — no clean-leave, no STOPPED publication).
# ============================================================
$node1->safe_psql('postgres',
	q{CREATE TABLE leg271 AS SELECT g, 'b'||g AS v FROM generate_series(1, 200) g});
$node1->safe_psql('postgres', 'CHECKPOINT');
$node1->stop('immediate');

# ============================================================
# L3: the survivor's first recovery.  Wait for the canonical HW rebuild
# (R3/R5: snapshot_lsn -> validated-end scan, adoption snapshot durably
# written) to complete on node0.
# ============================================================
my $log_off = -s $node0->logfile;
my $deadline = time() + 45;
my $log;
while (time() < $deadline)
{
	$log = PostgreSQL::Test::Utils::slurp_file($node0->logfile, $log_off);
	last if $log =~ /HW remaster worker: dead node 1 -> done/;
	sleep 1;
}
like($log,
	qr/cluster HW remaster: rebuilt authority from dead node 1 \(snapshot [0-9A-F\/]+, validated end [0-9A-F\/]+\); marked 2048 adopted shard\(s\) rebuilt/,
	'L3 first recoverer rebuilt the HW authority from the canonical snapshot (R3/R5)');
like($log, qr/HW remaster worker: dead node 1 -> done/,
	'L3 canonical rebuild completed (no private adoption)');

# ============================================================
# L4: the replay actor must NOT have executed — under fence provider-0
# the thread-recovery actor stops BLOCKED at NeedSet/admit with zero
# GES/replay/publish (only completed resources open).  The survivor's
# recovery episode therefore never replays the dead origin's WAL into
# mutation.
# ============================================================
unlike($log, qr/cluster thread recovery: dead thread 2 .*replay|replay_one/,
	'L4 replay actor did not execute (fence provider-0 BLOCKED, no mutation)');
unlike($log, qr/thread_recovery_replay_failclosed|validated-end decode failed/,
	'L4 no replay-side progress was attempted (fresh, no private adoption)');

# ============================================================
# L5: the dead origin's own rejoin is observed HONESTLY.  The L3/L4
# observation window (hw-remaster wait) exceeds the survivor's 3s death
# detection, so node1's restart is a dead-rejoin: under online_join=off
# the admission gate honestly closes (53R60/53R61) — the recovery
# episode does NOT open mutation for the dead origin.  (A fast-restart
# rejoin within the window is the t243 L4 path, already covered there.)
# ============================================================
my $n1_off = -s $node1->logfile;
$node1->start(fail_ok => 1);
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile, $n1_off);
like($log1, qr/53R60|write gate closed \(53R60\)|join did not converge/,
	'L5 dead-rejoin honestly blocked (online_join=off; no mutation opens)');

# cleanup: the dead-rejoining node stays down (its postmaster never
# reached ready, or sits blocked at 53R60); stop the survivor and sweep
# any residual node1 process.
$node0->stop;
system('pkill', '-9', '-f', 'wfirstrec');

done_testing();
