#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 244_wal_state_registry.pl
#    spec-4.2 -- ClusterWalState registry, single-node surface.
#
#      L1   offline init creates <root>/pgrac_wal_state (66048 bytes);
#           registry_ready=t; own slot reaches 'active' only after the
#           node serves (phase -> RUNNING publish)
#      L2   cluster_stats refresh advances registry_last_updated and
#           registry_highest_lsn while the node runs
#      L3   clean stop publishes STOPPED (verified via raw slot bytes
#           and via the dump key after restart)
#      L4   kill -9 leaves the slot ACTIVE (crash never writes STOPPED;
#           restart re-publishes ACTIVE with a new started_at)
#      L5   own-slot corruption -> startup FATAL 53RA2; restoring the
#           exact registry image recovers
#      L6   header corruption -> startup FATAL 53RA2 (never rebuilt
#           automatically); restoring the header recovers
#      L7   chmod-based publish failure -> startup FATAL 53RA2 (the
#           registered injection point cannot be armed before first
#           boot; same real-fault pattern as t/242 L11)
#      L8   corrupt NEIGHBOUR slot -> full-registry startup FATAL
#           53RA2; restoring the exact registry image recovers
#      L9   wal_threads_dir unset -> registry_ready=f, no file
#      L9b  startup failure before recovery does not publish ACTIVE: a
#           mis-linked pg_wal FATALs in the spec-4.1 claim validation
#           (pre-StartupXLOG) and the slot keeps its previous content.
#           NB: this does NOT exercise a mid-StartupXLOG failure; the
#           publish site (phase -> RUNNING) is after both.
#      L10  dump keys 10/10 under wal_thread; wait events 2/2
#      L11  own slot owned by a FOREIGN node_id (valid CRC) -> startup
#           FATAL 53RA2; the slot is never overwritten (round-2 P1)
#      L12  registry truncated to 512B -> startup FATAL 53RA2 (fixed
#           66048; never resized in place) (round-2 P1)
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.2-wal-thread-metadata-catalog.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use File::Path qw(make_path);
use PgracClusterNode;
use PgracWalState qw(crc32c read_file_raw write_file_raw read_slot_raw patch_byte
  forge_slot_node_id forge_slot_fpw_sticky);
use PostgreSQL::Test::Utils;
use Test::More;

sub dumpkey
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value FROM pg_cluster_state
		WHERE category='wal_thread' AND key='$key'});
}

sub checkpoint_redo_u64
{
	my ($node) = @_;

	return $node->safe_psql('postgres', q{
		SELECT pg_wal_lsn_diff(redo_lsn, '0/0')::bigint
		  FROM pg_control_checkpoint()});
}

sub cf_x_acquire_count
{
	my ($node) = @_;

	return $node->safe_psql('postgres', q{
		SELECT value::bigint FROM pg_cluster_state
		 WHERE category = 'cf' AND key = 'cf_x_acquire'});
}

sub w5_owned_bytes
{
	my ($regfile, $tid) = @_;
	my $slot = substr(read_file_raw($regfile), 512 + ($tid - 1) * 512, 512);

	return substr($slot, 56, 8) . substr($slot, 68, 4);
}

my $wroot = PostgreSQL::Test::Utils::tempdir();
my $regfile = "$wroot/pgrac_wal_state";
my $shared_root = PostgreSQL::Test::Utils::tempdir();
make_path("$shared_root/global");

my $node = PgracClusterNode->new('wal_state_a');
$node->init(extra => [
	'-X', "$wroot/thread_4", "--pgrac-wal-state-root=$wroot" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.shared_storage_backend = cluster_fs\n"
	  . "cluster.shared_data_dir = '$shared_root'\n"
	  . "cluster.smgr_user_relations = on\n"
	  . "cluster.controlfile_shared_authority = on\n"
	  . "cluster.cluster_stats_main_loop_interval = '500ms'\n"
	  . "autovacuum = off\n");
$node->start;

# ============================================================
# L1: registry created; ACTIVE published at the RUNNING transition.
# ============================================================
ok(-f $regfile, 'L1 registry file exists after first boot');
is(-s $regfile, 66048, 'L1 registry file is the fixed 66048 bytes');
is(dumpkey($node, 'registry_ready'), 't', 'L1 registry_ready');
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L1 own slot is ACTIVE once the node serves SQL');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{thread_id}, 4, 'L1 slot self-describes thread 4');
	is($slot->{node_id},   3, 'L1 slot records node_id 3');
	is($slot->{state},     1, 'L1 raw state == ACTIVE(1)');
}

# ============================================================
# L2: stats tick refreshes liveness stamp + watermarks.
# ============================================================
my $ts0  = dumpkey($node, 'registry_last_updated');
my $lsn0 = dumpkey($node, 'registry_highest_lsn');
$node->safe_psql('postgres',
	q{CREATE TABLE t244 AS SELECT g FROM generate_series(1, 2000) g});
my $deadline = time() + 15;
my ($ts1, $lsn1) = ($ts0, $lsn0);
while (time() < $deadline) {
	$ts1  = dumpkey($node, 'registry_last_updated');
	$lsn1 = dumpkey($node, 'registry_highest_lsn');
	last if $ts1 ne $ts0 && $lsn1 ne $lsn0;
	select(undef, undef, undef, 0.25);
}
cmp_ok($ts1, '>', $ts0, "L2 registry_last_updated advances ($ts0 -> $ts1)");
isnt($lsn1, $lsn0, 'L2 registry_highest_lsn advances with WAL volume');

# ============================================================
# W5: SIGHUP may request FPW-off but only a non-EOR checkpoint may
# persist the sticky and perform the safe off transition.  Both W5a
# and W5b borrow the checkpoint's already verified CF(X).
# ============================================================
{
	my $w5root = PostgreSQL::Test::Utils::tempdir();
	my $w5reg = "$w5root/pgrac_wal_state";
	my $w5shared = PostgreSQL::Test::Utils::tempdir();
	make_path("$w5shared/global");
	my $w5 = PgracClusterNode->new('wal_state_w5');
	$w5->init(extra => [
		'-X', "$w5root/thread_14", "--pgrac-wal-state-root=$w5root" ]);
	$w5->append_conf('postgresql.conf',
		    "cluster.enabled = on\n"
		  . "cluster.node_id = 13\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.wal_threads_dir = '$w5root'\n"
		  . "cluster.shared_storage_backend = cluster_fs\n"
		  . "cluster.shared_data_dir = '$w5shared'\n"
		  . "cluster.smgr_user_relations = on\n"
		  . "cluster.controlfile_shared_authority = on\n"
		  . "cluster.cluster_stats_main_loop_interval = '500ms'\n"
		  . "autovacuum = off\n");
	$w5->start;
	my $before = read_slot_raw($w5reg, 14);

	is($before->{fpw_was_off}, 0, 'W5 precondition: FPW-off sticky is clear');
	$w5->safe_psql('postgres', q{
		ALTER SYSTEM SET full_page_writes = 'off';
		SELECT pg_reload_conf();});
	ok($w5->poll_query_until('postgres',
		q{SELECT current_setting('full_page_writes') = 'off'}),
		'W5 desired full_page_writes=off is visible after SIGHUP');
	is(read_slot_raw($w5reg, 14)->{fpw_was_off}, 0,
		'W5 SIGHUP alone does not initialize the sticky');

	chmod(0444, $w5reg) or die "chmod: $!";
	$w5->safe_psql('postgres', 'CHECKPOINT');
	is($w5->safe_psql('postgres',
		q{SELECT full_page_writes FROM pg_control_checkpoint()}),
		't', 'W5 sticky failure keeps checkpoint FPW on and checkpoint completes');
	my $failed = read_slot_raw($w5reg, 14);
	is($failed->{fpw_was_off}, 0, 'W5 sticky failure emits no durable off evidence');
	is($failed->{checkpoint_redo_lsn}, $before->{checkpoint_redo_lsn},
		'W5 advert failure preserves the prior checkpoint redo');

	chmod(0644, $w5reg) or die "chmod: $!";
	$w5->safe_psql('postgres', 'CHECKPOINT');
	my $retry = read_slot_raw($w5reg, 14);
	is($retry->{fpw_was_off}, 1,
		'W5 next non-EOR checkpoint persists sticky before FPW-off');
	is($w5->safe_psql('postgres',
		q{SELECT full_page_writes FROM pg_control_checkpoint()}),
		'f', 'W5 successful sticky permits checkpoint FPW-off');
	is($retry->{checkpoint_redo_lsn}, checkpoint_redo_u64($w5),
		'W5 successful checkpoint advert equals durable control redo');

	$w5->safe_psql('postgres', q{
		ALTER SYSTEM SET full_page_writes = 'on';
		SELECT pg_reload_conf();});
	ok($w5->poll_query_until('postgres',
		q{SELECT current_setting('full_page_writes') = 'on'}),
		'W5 desired full_page_writes=on is visible after SIGHUP');

	$w5->safe_psql('postgres',
		q{CREATE TABLE w5_redo_advance AS SELECT generate_series(1, 1000)});
	my $advert_before_failure = read_slot_raw($w5reg, 14)->{checkpoint_redo_lsn};
	chmod(0444, $w5reg) or die "chmod: $!";
	$w5->safe_psql('postgres', 'CHECKPOINT');
	my $control_after_failure = checkpoint_redo_u64($w5);
	cmp_ok($control_after_failure, '>', $advert_before_failure,
		'W5 failed advert does not prevent durable checkpoint progress');
	is(read_slot_raw($w5reg, 14)->{checkpoint_redo_lsn}, $advert_before_failure,
		'W5 failed advert preserves old conservative redo');

	chmod(0644, $w5reg) or die "chmod: $!";
	$w5->safe_psql('postgres', 'CHECKPOINT');
	is(read_slot_raw($w5reg, 14)->{checkpoint_redo_lsn}, checkpoint_redo_u64($w5),
		'W5 next checkpoint retries and publishes its durable redo');

	# Leave replay's latest FPW state off, then restart with the shared-CF
	# authority disabled so the EOR test isolates W5's read-only evidence gate.
	$w5->safe_psql('postgres', q{
		ALTER SYSTEM SET full_page_writes = 'off';
		SELECT pg_reload_conf();});
	ok($w5->poll_query_until('postgres',
		q{SELECT current_setting('full_page_writes') = 'off'}),
		'W5 historical leg sees desired FPW-off');
	$w5->safe_psql('postgres', 'CHECKPOINT');
	$w5->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.controlfile_shared_authority = 'off'});
	$w5->stop('immediate');
	forge_slot_fpw_sticky($w5reg, 14, 0);
	my $historical_start = $w5->start(fail_ok => 1);
	is($historical_start, 0,
		'W5 historical FPW-off without sticky is Startup FATAL');
	$w5->stop('immediate') if $historical_start;
	$w5->adjust_conf('postgresql.auto.conf',
		'cluster.controlfile_shared_authority', 'on');
	forge_slot_fpw_sticky($w5reg, 14, 1);
	my $w5_before_eor = read_slot_raw($w5reg, 14);
	my $w5_bytes_before_eor = w5_owned_bytes($w5reg, 14);
	my $eor_log_off = -s $w5->logfile;
	my $eor_start = $w5->start(fail_ok => 1);
	is($eor_start, 1,
		'W5 historical FPW-off with valid own sticky restarts');
	if (!$eor_start)
	{
		my $eor_log = PostgreSQL::Test::Utils::slurp_file(
			$w5->logfile, $eor_log_off);

		like($eor_log,
			qr/could not acquire the cluster control-file lock for a checkpoint.*checkpoint request failed/s,
			'W5 RED reaches the Checkpointer CF(X) OWNER-to-EOR gap');
		done_testing();
		exit;
	}

	my $w5_after_eor = read_slot_raw($w5reg, 14);
	is(w5_owned_bytes($w5reg, 14), $w5_bytes_before_eor,
		'W5 EOR leaves the owned registry bytes unchanged');
	is($w5_after_eor->{checkpoint_redo_lsn},
		$w5_before_eor->{checkpoint_redo_lsn},
		'W5 EOR performs no checkpoint-redo advert write');
	is($w5_after_eor->{fpw_was_off}, $w5_before_eor->{fpw_was_off},
		'W5 EOR performs no FPW-sticky write');
	is(cf_x_acquire_count($w5), 0,
		'W5 OWNER EOR restart does not acquire CF(X)');
	my $cf_x_before = cf_x_acquire_count($w5);
	$w5->safe_psql('postgres', 'CHECKPOINT');
	cmp_ok(cf_x_acquire_count($w5), '>', $cf_x_before,
		'W5 next steady checkpoint acquires real CF(X)');
	$w5->stop;
}

# ============================================================
# L3: clean stop publishes STOPPED.
# ============================================================
$node->stop;    # fast = clean
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 2, 'L3 raw state == STOPPED(2) after clean stop');
}
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L3 restart republishes ACTIVE');

# ============================================================
# L4: crash leaves ACTIVE; restart re-publishes with new started_at.
# ============================================================
my $started_before = read_slot_raw($regfile, 4)->{started_at};
$node->stop('immediate');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 1, 'L4 immediate shutdown leaves the slot ACTIVE');
	is($slot->{started_at}, $started_before,
		'L4 crash did not rewrite the slot (same incarnation stamp)');
}
$node->start;
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 1, 'L4 restart publishes ACTIVE again');
	cmp_ok($slot->{started_at}, '>', $started_before,
		'L4 new incarnation has a newer started_at');
}

# ============================================================
# L5: own-slot corruption fails full-registry startup validation.
# ============================================================
$node->stop;
my $own_corrupt_image = read_file_raw($regfile);
# slot 4 starts at 512 + (4-1)*512 = 2048; flip one body byte -> bad CRC
patch_byte($regfile, 2048 + 41);
is($node->start(fail_ok => 1), 0,
	'L5 start refused on corrupt own slot');
write_file_raw($regfile, $own_corrupt_image);
$node->start;

# ============================================================
# L6: header corruption -> FATAL 53RA2, never auto-rebuilt.
# ============================================================
$node->stop;
my $hdr_orig;
{
	open my $fh, '<:raw', $regfile or die;
	sysread($fh, $hdr_orig, 512) == 512 or die;
	close $fh;
}
patch_byte($regfile, 0);    # magic
my $log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L6 start refused on corrupt registry header');
my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/WAL state registry .* failed validation/,
	'L6 FATAL names the registry validation failure (53RA2)');
{
	open my $fh, '+<:raw', $regfile or die;
	syswrite($fh, $hdr_orig, 512) == 512 or die;
	close $fh;
}
$node->start;
is(dumpkey($node, 'registry_ready'), 't', 'L6 restored header validates again');

# ============================================================
# L7: publish failure (read-only registry) -> FATAL 53RA2.
# ============================================================
$node->stop;
chmod(0444, $regfile) or die "chmod: $!";
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L7 start refused when ACTIVE publish cannot write');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/could not publish ACTIVE to the WAL state registry/,
	'L7 FATAL names the ACTIVE publish failure (53RA2)');
chmod(0644, $regfile) or die "chmod: $!";
$node->start;

# ============================================================
# L8: corrupt neighbour slot fails full-registry startup validation.
# ============================================================
$node->stop;
my $neighbour_corrupt_image = read_file_raw($regfile);
patch_byte($regfile, 512 + (9 - 1) * 512 + 4);    # slot 9 garbage
is($node->start(fail_ok => 1), 0,
	'L8 start refused on corrupt neighbour slot');
write_file_raw($regfile, $neighbour_corrupt_image);
$node->start;

# ============================================================
# L9: no wal_threads_dir -> no registry.
# ============================================================
my $flat = PgracClusterNode->new('wal_state_flat');
$flat->init;
$flat->append_conf('postgresql.conf',
	"cluster.enabled = on\ncluster.node_id = 5\ncluster.allow_single_node = on\n");
$flat->start;
is(dumpkey($flat, 'registry_ready'), 'f', 'L9 flat layout has no registry');
is(dumpkey($flat, 'registry_slot_state'), '-', 'L9 slot state placeholder');
$flat->stop;

# ============================================================
# L9b: pre-recovery startup failure does not publish ACTIVE.
# (The 4.1 claim validation FATALs before StartupXLOG; ACTIVE is
# published only at phase -> RUNNING, after recovery succeeded.)
# ============================================================
$node->stop;
my $stopped_before = read_slot_raw($regfile, 4);
is($stopped_before->{state}, 2, 'L9b precondition: clean STOPPED on disk');
make_path("$wroot/thread_9");
my $pg_wal = $node->data_dir . '/pg_wal';
unlink($pg_wal) or die "unlink: $!";
symlink("$wroot/thread_9", $pg_wal) or die "symlink: $!";
is($node->start(fail_ok => 1), 0, 'L9b mis-linked pg_wal still refused (4.1)');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 2,
		'L9b failed startup never published ACTIVE (slot keeps STOPPED)');
}
unlink($pg_wal) or die;
symlink("$wroot/thread_4", $pg_wal) or die;
$node->start;

# ============================================================
# L10: dump keys + wait events self-enumeration.
# ============================================================
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'wal_thread'}),
	'10', 'L10 wal_thread category has exactly 10 keys (spec-4.2 +5)');
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		  WHERE name IN ('ClusterWalStateRead', 'ClusterWalStateWrite')}),
	'2', 'L10 registry I/O wait events registered');

# ============================================================
# L11: valid own slot owned by a FOREIGN node_id -> startup refused;
# the slot is evidence and is never overwritten (round-2 P1).
# ============================================================
$node->stop;
my $full_image = read_file_raw($regfile);
is(length($full_image), 66048, 'L11 precondition: full registry image saved');
forge_slot_node_id($regfile, 4, 7);    # valid CRC, foreign owner
is(read_slot_raw($regfile, 4)->{node_id}, 7, 'L11 crafted slot says node 7');
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L11 start refused: own slot owned by another node');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/own slot 4 failed validation.*node_id mismatch.*expected 3, found 7/s,
	'L11 FATAL names the foreign owner (53RA2)');
is(read_slot_raw($regfile, 4)->{node_id}, 7,
	'L11 foreign slot left untouched (evidence preserved)');
write_file_raw($regfile, $full_image);
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L11 restored registry starts and republishes ACTIVE');

# ============================================================
# L12: truncated registry -> startup refused (fixed 66048 bytes,
# never resized in place) (round-2 P1).
# ============================================================
$node->stop;
$full_image = read_file_raw($regfile);
truncate($regfile, 512) or die "truncate: $!";
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L12 start refused on truncated registry');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/not a regular exact-size file.*size 512, expected 66048/s,
	'L12 FATAL names the size mismatch (53RA2)');
is(-s $regfile, 512, 'L12 registry never auto-resized');
{
	open my $fh, '>:raw', $regfile or die;
	syswrite($fh, $full_image) == 66048 or die;
	close $fh;
}
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L12 restored registry starts and republishes ACTIVE');

$node->stop;

done_testing();
