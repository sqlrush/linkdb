#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 418_pcm_x_barrier_propagation_2node.pl
#    T400-P0-08: a nested PCM-X barrier refusal is intent-typed through
#    cleanup, deferred prepare/reuse, caller-owned unwind, and exact drain.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'PCM-X barrier propagation requires --enable-cluster';
}

my $point = 'cluster-bufmgr-pcm-x-retry-barrier';
my @gauges = qw(
	pcm_x_queue_depth
	pcm_x_queue_active_tags
	pcm_x_queue_live_tickets
	pcm_x_queue_live_slots
);
my @ledger_keys = qw(
	pcm_x_bufmgr_holder_live_count
	pcm_x_bufmgr_holder_deferred_count
	pcm_x_bufmgr_writer_live_count
	pcm_x_bufmgr_writer_deferred_count
);

sub pcm_int
{
	my ($node, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		"SELECT value FROM pg_cluster_state "
		  . "WHERE category = 'pcm' AND key = '$key'",
		timeout => 5);
	die "missing or non-integer pcm.$key: [$value]"
	  unless defined($value) && $value =~ /\A\d+\z/;
	return int($value);
}

sub injection_hits
{
	my ($session) = @_;
	my $value = $session->query_safe(
		"SELECT hits FROM pg_stat_cluster_injections WHERE name = '$point'");
	die "missing or non-integer injection hits: [$value]"
	  unless defined($value) && $value =~ /\A\d+\z/;
	return int($value);
}

sub pcm_value
{
	my ($session, $key) = @_;
	my $value = $session->query_safe(
		"SELECT value FROM pg_cluster_state "
		  . "WHERE category = 'pcm' AND key = '$key'");
	die "missing pcm.$key" unless defined($value) && length($value);
	return $value;
}

sub target_ltag
{
	my ($session, $tag) = @_;
	my (undef, undef, $rel, $fork, $block) = split(m{/}, $tag);
	return $session->query_safe(
		"SELECT value FROM pg_cluster_state "
		  . "WHERE category = 'pcm' AND key LIKE 'pcm_x_ltag_%' "
		  . "AND value LIKE 'rel=$rel fork=$fork blk=$block %'");
}

sub wait_for_target_ltag
{
	my ($node, $tag, $seconds) = @_;
	my (undef, undef, $rel, $fork, $block) = split(m{/}, $tag);
	my $deadline = time() + $seconds;
	my $last = '';

	do
	{
		$last = $node->safe_psql(
			'postgres',
			"SELECT value FROM pg_cluster_state "
			  . "WHERE category = 'pcm' AND key LIKE 'pcm_x_ltag_%' "
			  . "AND value LIKE 'rel=$rel fork=$fork blk=$block %'",
			timeout => 5);
		return (1, $last)
		  if $last =~ /\bmembers=[1-9]\d*\b/
		  && $last =~ /\bactive_writer=\d+\b/;
		usleep(50_000);
	} while (time() < $deadline);
	return (0, $last);
}

sub ledger_counts
{
	my ($session) = @_;
	my $keys = join(',', map { "'$_'" } @ledger_keys);
	my $rows = $session->query_safe(
		"SELECT key || '=' || value FROM pg_cluster_state "
		  . "WHERE category = 'pcm' AND key IN ($keys) ORDER BY key");
	my %counts;

	for my $row (split(/\n/, $rows))
	{
		my ($key, $value) = split(/=/, $row, 2);
		die "invalid PCM-X ledger row: [$row]"
		  unless defined($key) && defined($value) && $value =~ /\A\d+\z/;
		$counts{$key} = int($value);
	}
	die "missing PCM-X ledger rows: [$rows]"
	  unless scalar(keys %counts) == scalar(@ledger_keys);
	return \%counts;
}

sub assert_ledger_counts
{
	my ($session, $expected, $name) = @_;
	is_deeply(ledger_counts($session), $expected, $name);
}

sub arm_once
{
	my ($session) = @_;
	is($session->query_safe(
			"SELECT cluster_inject_fault('$point', 'skipn', 1)"),
		't', "arm $point for one exact retry");
}

sub set_barrier_target
{
	my ($session, $tag, $name) = @_;
	$session->query_safe(
		"SET cluster.pcm_x_retry_barrier_target = '$tag'");
	is($session->query_safe('SHOW cluster.pcm_x_retry_barrier_target'),
		$tag, $name);
}

sub wait_for_gauges
{
	my ($node, $baseline, $seconds) = @_;
	my $deadline = time() + $seconds;
	my %last;

	do
	{
		%last = map { $_ => pcm_int($node, $_) } @gauges;
		return (1, \%last)
		  if !grep { $last{$_} != $baseline->{$_} } @gauges;
		usleep(100_000);
	} while (time() < $deadline);
	return (0, \%last);
}

sub wait_for_pcm_value
{
	my ($node, $key, $expected, $seconds) = @_;
	my $deadline = time() + $seconds;
	my $last;

	do
	{
		$last = pcm_int($node, $key);
		return (1, $last) if $last == $expected;
		usleep(100_000);
	} while (time() < $deadline);
	return (0, $last);
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
	'p008_barrier',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 16MB',
		'cluster.lms_workers = 1',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.undo_segments_per_instance = 64',
		'cluster.read_scache = on',
		'cluster.crossnode_write_write = on',
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
for my $node_index (0 .. 1)
{
	my $member_node = $node_index == 0 ? $node0 : $node1;
	my ($membership_ready, $membership_count) =
	  wait_for_membership_count($member_node, 2, 60);
	ok($membership_ready, "L0 node$node_index sees two MEMBER nodes")
	  or diag("last node$node_index membership count=$membership_count");
}
my ($runtime_active, $runtime_state) =
  wait_for_pcm_value($node0, 'pcm_x_runtime_state', 1, 45);
ok($runtime_active, 'L0 PCM-X runtime becomes ACTIVE')
  or diag("last pcm_x_runtime_state=$runtime_state");

my $copy_rows = join("\n",
	map { "$_\t0" } 1 .. 2000);
my $copy_other = join("\n", map { "$_\t0" } 1 .. 1000);
$node0->safe_psql(
	'postgres',
	qq{
		BEGIN;
		CREATE TABLE t418 (id int, v int NOT NULL);
		CREATE TABLE t418_other (LIKE t418 INCLUDING ALL);
		CREATE TABLE t418_writer (LIKE t418 INCLUDING ALL);
		COPY t418 (id, v) FROM STDIN FREEZE;
$copy_rows
\\.

		COPY t418_other (id, v) FROM STDIN FREEZE;
$copy_other
\\.
		COPY t418_writer (id, v) FROM STDIN FREEZE;
$copy_rows
\\.
		COMMIT;
		ANALYZE t418;
		ANALYZE t418_other;
		ANALYZE t418_writer;
	});
cmp_ok(int($node0->safe_psql(
		'postgres',
		q{SELECT min(relallvisible) FROM pg_class
		  WHERE oid IN ('t418'::regclass, 't418_writer'::regclass)})),
	'>=', 6, 'L0 COPY FREEZE made all selected heap blocks all-visible');

my %baseline = map { $_ => pcm_int($node0, $_) } @gauges;
my $unwind0 = pcm_int($node0, 'pcm_x_queue_barrier_unwind_count');
my $log_offset = (-s $node0->logfile) // 0;
my $session = $node0->background_psql(
	'postgres', on_error_die => 1, timeout => 30);
$session->set_query_timer_restart();
$session->query_safe(q{
	SET enable_seqscan = off;
	PREPARE holder_cleanup AS
		DELETE FROM t418 WHERE ctid = '(0,1)'::tid RETURNING id;
	PREPARE other_vm_cleanup AS
		DELETE FROM t418_other WHERE ctid = '(1,1)'::tid RETURNING id;
	PREPARE holder_reuse AS
		DELETE FROM t418 WHERE ctid = '(1,1)'::tid RETURNING id;
	PREPARE writer_cleanup AS
		DELETE FROM t418_writer WHERE ctid = '(0,1)'::tid RETURNING id;
	PREPARE writer_reuse AS
		DELETE FROM t418_writer WHERE ctid = '(1,1)'::tid RETURNING id;
	PREPARE final_drain AS
		DELETE FROM t418_writer WHERE ctid = '(2,1)'::tid RETURNING id;
});
my $target_prefix = $session->query_safe(q{
	SELECT format(
		'%s/%s/%s',
		CASE WHEN c.reltablespace = 0 THEN 1663 ELSE c.reltablespace END,
		d.oid,
		pg_relation_filenode(c.oid))
	FROM pg_class c
	CROSS JOIN pg_database d
	WHERE c.oid = 't418'::regclass
	  AND d.datname = current_database()
});
my $writer_target_prefix = $session->query_safe(q{
	SELECT format(
		'%s/%s/%s',
		CASE WHEN c.reltablespace = 0 THEN 1663 ELSE c.reltablespace END,
		d.oid,
		pg_relation_filenode(c.oid))
	FROM pg_class c
	CROSS JOIN pg_database d
	WHERE c.oid = 't418_writer'::regclass
	  AND d.datname = current_database()
});
# Both selected heap blocks map to visibility-map block zero.  The seam binds
# that exact VM BufferTag; the distinct heap blocks let two successive updates
# exercise the same VM evidence without an intervening VACUUM.
my $target_vm_block = 0;
my $target_holder_tag = "$target_prefix/2/$target_vm_block/holder";
my $target_writer_tag = "$writer_target_prefix/2/$target_vm_block/writer";
my (undef, undef, $target_rel) = split(m{/}, $target_holder_tag);
my (undef, undef, $writer_target_rel) = split(m{/}, $target_writer_tag);
my %ledger_zero = map { $_ => 0 } @ledger_keys;
assert_ledger_counts($session, \%ledger_zero,
	'L0 current-backend holder/writer ledgers start at zero');

my $hits0 = injection_hits($session);

# Leg 1: an all-visible heap update locks its VM page, whose holder cleanup
# receives BARRIER_CLOSED.  The exact handle remains DEFERRED and SQL returns.
set_barrier_target($session, $target_holder_tag,
	'L1 selector binds the exact target heap holder relation and block');
arm_once($session);
cmp_ok(int($session->query_safe('EXECUTE other_vm_cleanup')), '>', 0,
	'L1 same-backend non-target terminal operation completes first');
is(injection_hits($session), $hits0,
	'L1 same-backend non-target terminal operation cannot consume the exact target arm');
assert_ledger_counts($session, \%ledger_zero,
	'L1 non-target VM leaves no deferred ledger evidence');
cmp_ok(int($session->query_safe('EXECUTE holder_cleanup')), '>', 0,
	'L1 holder cleanup defers exact VM holder without client result 13');
my $hits1 = injection_hits($session);
is($hits1, $hits0 + 1, 'L1 injection is consumed exactly once');
is(pcm_int($node0, 'pcm_x_queue_barrier_unwind_count'), $unwind0 + 1,
	'L1 cleanup refusal records one typed barrier unwind');
assert_ledger_counts(
	$session,
	{
		%ledger_zero,
		pcm_x_bufmgr_holder_live_count     => 1,
		pcm_x_bufmgr_holder_deferred_count => 1,
	},
	'L1 exact holder ledger is live and DEFERRED in the triggering backend');
my $holder_hit = pcm_value($session, 'pcm_x_bufmgr_barrier_hit');
my $holder_deferred = pcm_value($session, 'pcm_x_bufmgr_barrier_deferred');
is($holder_deferred, $holder_hit,
	'L1 holder HIT and DEFERRED snapshots name the same exact evidence');
like($holder_hit,
	qr/\Avalid=1 lane=holder .*rel=\Q$target_rel\E fork=2 blk=\Q$target_vm_block\E .*request=[1-9]\d* .*evidence_slot=\d+\/[1-9]\d*/,
	'L1 holder trace includes exact relation/block/request/holder-slot identity');
like(target_ltag($session, $target_holder_tag),
	qr/\bactive_holder=\d+\b/,
	'L1 exact target ltag retains its active holder member while DEFERRED');
cmp_ok(int(pcm_value($session,
			'pcm_x_bufmgr_barrier_non_target_ignored_count')),
	'>', 0,
	'L1 same-backend non-target terminal operation was ignored before dispatch');

# Leg 2: an update of the other heap block reaches the same VM holder prepare.
# It refuses, aborts the fresh ACQUIRING writer claim, and heapam owns
# unlock/warm/requalification.
set_barrier_target($session, $target_holder_tag,
	'L2 selector remains bound to the deferred holder evidence');
arm_once($session);
cmp_ok(int($session->query_safe('EXECUTE holder_reuse')), '>', 0,
	'L2 holder prepare/reuse refusal unwinds to heap caller and requalifies');
my $hits2 = injection_hits($session);
cmp_ok($hits2, '>=', $hits1 + 1,
	'L2 holder prepare/reuse reaches the armed refusal');
is(pcm_int($node0, 'pcm_x_queue_barrier_unwind_count'), $unwind0 + 2,
	'L2 holder prepare/reuse records exactly one additional unwind');
assert_ledger_counts($session, \%ledger_zero,
	'L2 holder prepare refusal requalifies and drains exact evidence to UNUSED');
is(pcm_value($session, 'pcm_x_bufmgr_barrier_unused'), $holder_hit,
	'L2 the same exact holder evidence transitions DEFERRED to UNUSED');
is(pcm_value($session, 'pcm_x_bufmgr_barrier_holder_live_high_water'), '1',
	'L2 target holder ledger high-water proves no instantaneous duplicate');

is($session->query_safe(
		"SELECT cluster_inject_fault('$point', 'none', 0)"),
	't', 'L2 disarms the completed holder assertion');
$session->quit;

# A full-pair cold restart clears the shared-buffer PCM-X cover without using
# the unsupported online-rejoin path.  The untouched writer relation therefore
# starts its independent exact VM lifecycle from N.
$pair->stop_pair;
$pair->start_pair;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 45),
	'L3 node0 sees node1 connected after the cold restart');
ok($pair->wait_for_peer_state(1, 0, 'connected', 45),
	'L3 node1 sees node0 connected after the cold restart');
for my $node_index (0 .. 1)
{
	my $member_node = $node_index == 0 ? $node0 : $node1;
	my ($membership_ready, $membership_count) =
	  wait_for_membership_count($member_node, 2, 60);
	ok($membership_ready,
		"L3 node$node_index sees two MEMBER nodes after the cold restart")
	  or diag("last node$node_index membership count=$membership_count");
}
($runtime_active, $runtime_state) =
  wait_for_pcm_value($node0, 'pcm_x_runtime_state', 1, 45);
ok($runtime_active, 'L3 PCM-X runtime becomes ACTIVE after the cold restart')
  or diag("last pcm_x_runtime_state=$runtime_state");

%baseline = map { $_ => pcm_int($node0, $_) } @gauges;
$unwind0 = pcm_int($node0, 'pcm_x_queue_barrier_unwind_count');
$session = $node0->background_psql(
	'postgres', on_error_die => 1, timeout => 30);
$session->set_query_timer_restart();
$session->query_safe(q{
	SET enable_seqscan = off;
	PREPARE writer_cleanup AS
		DELETE FROM t418_writer WHERE ctid = '(0,1)'::tid RETURNING id;
	PREPARE writer_reuse AS
		DELETE FROM t418_writer WHERE ctid = '(1,1)'::tid RETURNING id;
	PREPARE final_drain AS
		DELETE FROM t418_writer WHERE ctid = '(2,1)'::tid RETURNING id;
});
assert_ledger_counts($session, \%ledger_zero,
	'L3 writer leg starts in a fresh backend with both ledgers at exact zero');
my $writer_hits0 = injection_hits($session);

# Leg 3: an exclusive heap lifecycle reaches writer cleanup first.  Its exact
# claim remains DEFERRED during the bounded assertion-only observation window.
# A separate backend reads the live shared ltag before the triggering SQL is
# allowed to finish its normal prepare/reuse drain.
set_barrier_target($session, $target_writer_tag,
	'L3 selector binds the exact target heap writer relation and block');
$session->query_safe(
	'SET cluster.pcm_x_retry_barrier_writer_hold_ms = 6000');
is($session->query_safe(
		'SHOW cluster.pcm_x_retry_barrier_writer_hold_ms'),
	'6000', 'L3 enables the bounded cassert-only live writer window');
arm_once($session);
$session->query_until(
	qr/T418_WRITER_STARTED/,
	"\\echo T418_WRITER_STARTED\nEXECUTE writer_cleanup;\n");
my ($writer_ltag_ready, $writer_ltag_live) =
  wait_for_target_ltag($node0, $target_writer_tag, 5);
ok($writer_ltag_ready,
	'L3 exact target ltag is live while the selected writer is DEFERRED')
  or diag("last target ltag=[$writer_ltag_live]");
like($writer_ltag_live,
	qr/\Arel=\Q$writer_target_rel\E fork=2 blk=\Q$target_vm_block\E .*\bmembers=[1-9]\d*\b .*\bactive_writer=\d+\b/,
	'L3 live target ltag exposes member count and active-writer identity');
my ($live_writer_slot) = $writer_ltag_live =~ /\bactive_writer=(\d+)\b/;
my $writer_finish = $session->query_until(
	qr/T418_WRITER_DONE/, "\\echo T418_WRITER_DONE\n");
like($writer_finish, qr/(?:\A|\n)1(?:\r?\n|\z)/,
	'L3 writer cleanup finishes after the bounded observation window');
my $hits3 = injection_hits($session);
cmp_ok($hits3, '>=', $writer_hits0 + 1,
	'L3 writer cleanup reaches the armed refusal');
cmp_ok(pcm_int($node0, 'pcm_x_queue_barrier_unwind_count'), '>=', $unwind0 + 1,
	'L3 writer cleanup records a typed barrier unwind');
assert_ledger_counts($session, \%ledger_zero,
	'L3 triggering SQL drains the exact writer ledger after observation');
my $writer_hit = pcm_value($session, 'pcm_x_bufmgr_barrier_hit');
my $writer_deferred = pcm_value($session, 'pcm_x_bufmgr_barrier_deferred');
my $writer_unused = pcm_value($session, 'pcm_x_bufmgr_barrier_unused');
is($writer_deferred, $writer_hit,
	'L3 writer HIT and DEFERRED snapshots name the same exact claim');
is($writer_unused, $writer_hit,
	'L3 the same captured writer claim transitions DEFERRED to UNUSED');
like($writer_hit,
	qr/\Avalid=1 lane=writer .*rel=\Q$writer_target_rel\E fork=2 blk=\Q$target_vm_block\E .*request=[1-9]\d* .*evidence_slot=\d+\/[1-9]\d* claim=[1-9]\d* round=[1-9]\d*/,
	'L3 writer trace includes exact relation/block/request/active-writer identity');
my ($hit_writer_slot) = $writer_hit =~ /\bevidence_slot=(\d+)\//;
is($hit_writer_slot, $live_writer_slot,
	'L3 trace evidence slot is the live ltag active-writer identity');
is(pcm_value($session, 'pcm_x_bufmgr_barrier_writer_live_high_water'), '1',
	'L3 target writer ledger high-water proves no instantaneous duplicate claim');

# Leg 4: do not re-arm the injection and accidentally overwrite the first
# claim's trace with a later same-tag lifecycle.  A normal reuse must leave
# that exact completed identity and the one-entry high-water unchanged.
my $writer_unwind3 = pcm_int($node0, 'pcm_x_queue_barrier_unwind_count');
is($session->query_safe(
		"SELECT cluster_inject_fault('$point', 'none', 0)"),
	't', 'L4 opens the one-shot writer assertion barrier');
cmp_ok(int($session->query_safe('EXECUTE writer_reuse')), '>', 0,
	'L4 next same-buffer lifecycle succeeds without re-arming');
my $hits4 = injection_hits($session);
is($hits4, $hits3,
	'L4 next lifecycle cannot replace the captured injection identity');
is(pcm_int($node0, 'pcm_x_queue_barrier_unwind_count'), $writer_unwind3,
	'L4 unarmed writer reuse adds no barrier unwind');
assert_ledger_counts($session, \%ledger_zero,
	'L4 unarmed writer reuse leaves both ledgers at exact zero');
is(pcm_value($session, 'pcm_x_bufmgr_barrier_unused'), $writer_hit,
	'L4 completed trace still names the first exact writer claim');
is(pcm_value($session, 'pcm_x_bufmgr_barrier_writer_live_high_water'), '1',
	'L4 target writer high-water remains one after normal reuse');

# Leg 5: opening the assertion barrier lets the same backend consume its
# exact evidence and complete another same-buffer lifecycle.
cmp_ok(int($session->query_safe('EXECUTE final_drain')), '>', 0,
	'L5 another same-buffer lifecycle succeeds after exact drain');
assert_ledger_counts($session, \%ledger_zero,
	'L5 current-backend holder/writer live and deferred ledgers are exact zero');
$session->quit;

my ($gauges_ok, $final_gauges) = wait_for_gauges($node0, \%baseline, 30);
ok($gauges_ok, 'L5 queue/tag/ticket/slot gauges return to baseline')
  or diag(explain($final_gauges));
is(pcm_int($node0, 'pcm_x_runtime_state'), 1,
	'L5 runtime remains ACTIVE after positive drain');

my $log = substr(slurp_file($node0->logfile), $log_offset);
unlike($log, qr/result=13/,
	'client/server log has no untyped BARRIER_CLOSED result 13');
unlike($log, qr/cluster PCM-X (?:holder|writer) operation (?:is not ready|failed)/,
	'client/server log has no holder/writer operation failure');
unlike($log, qr/\b(?:PANIC|FATAL)\b/,
	'client/server log has no PANIC or FATAL');
unlike($log, qr/cannot wait for a cluster PCM-X holder gate while holding content authority/,
	'P0-30 lock-order guard is not tripped');

$pair->stop_pair;
done_testing();
