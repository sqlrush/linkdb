#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 417_cf_checkpoint_resowner_cleanup_2node.pl
#    A checkpoint ERROR after CF X acquisition must release the exact holder
#    through ResourceOwner cleanup.  The surviving checkpointer must then run
#    the next checkpoint under the same PID without a stale local/remote grant.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub state_counter
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql('postgres', qq{
		SELECT coalesce((
			SELECT value::bigint
			FROM pg_cluster_state
			WHERE category = '$category' AND key = '$key'), 0)});
	return $value // 0;
}

sub checkpointer_pid
{
	my ($node) = @_;
	return $node->safe_psql(
		'postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer'});
}

sub dirty_relation
{
	my ($node, $round) = @_;
	$node->safe_psql(
		'postgres',
		"UPDATE cf_cleanup_t SET v = 'r${round}-' || id WHERE id <= 50");
}

sub set_outbound_saturation
{
	my ($on, @nodes) = @_;
	my $sql = $on
	  ? q{ALTER SYSTEM SET cluster.injection_points =
			'cluster-cf-s6-outbound-double-full:warning'}
	  : q{ALTER SYSTEM RESET cluster.injection_points};

	for my $node (@nodes)
	{
		$node->safe_psql('postgres', $sql);
		$node->safe_psql('postgres', q{SELECT pg_reload_conf()});
	}

	# Give the checkpointer and remote GES processes a SIGHUP cycle.  The
	# saturation hook logs when the checkpointer actually consumes the arm.
	usleep(1_000_000);
}

sub set_cf_held_window
{
	my ($on, @nodes) = @_;
	my $sql = $on
	  ? q{ALTER SYSTEM SET cluster.injection_points =
			'cluster-cf-held-census-window:sleep:6000000'}
	  : q{ALTER SYSTEM RESET cluster.injection_points};

	for my $node (@nodes)
	{
		$node->safe_psql('postgres', $sql);
		$node->safe_psql('postgres', q{SELECT pg_reload_conf()});
	}
	usleep(1_000_000);
}

sub failed_release_request_id
{
	my ($log) = @_;
	my ($request_id) =
	  $log =~ /CF holder release was not confirmed:[^\r\n]*request_id=(\d+)[^\r\n]*exact holder retained pending/;
	return $request_id;
}

sub retried_release_request_id
{
	my ($log) = @_;
	my ($request_id) =
	  $log =~ /CF pending holder release retry confirmed:[^\r\n]*request_id=(\d+)/;
	return $request_id;
}

sub test_request_id_log_parsers
{
	my $serial_log =
	    "CF holder release was not confirmed: node=0 procno=7 epoch=8 "
	  . "request_id=101 result=8; exact holder retained pending\n"
	  . "CF pending holder release retry confirmed: node=0 procno=7 epoch=8 "
	  . "request_id=202\n"
	  . "unrelated producer request_id=909\n";
	my $split_failed_log =
	    "CF holder release was not confirmed: node=0 procno=7 epoch=8\n"
	  . "unrelated producer request_id=303 exact holder retained pending\n";

	is(failed_release_request_id($serial_log), '101',
		'failed-release request_id parser stays on its producer log line');
	ok(!defined failed_release_request_id($split_failed_log),
		'failed-release parser rejects a request_id and suffix from another line');
	is(retried_release_request_id($serial_log), '202',
		'retry request_id parser ignores a later unrelated request_id');
}

test_request_id_log_parsers();
if ($ENV{PGRAC_T417_LOG_PARSER_ONLY})
{
	done_testing();
	exit 0;
}

my @cf_live_numeric_keys = qw(
	cf_slot_snapshot_valid
	cf_x_held_count
	cf_s_held_count
	cf_x_release_pending_count
	cf_s_release_pending_count
	cf_pending_retry_count
	cf_slot_invalid_count
);

sub cf_live_value
{
	my ($node, $key) = @_;
	my $row = $node->safe_psql(
		'postgres',
		"SELECT count(*) || E'\\t' || coalesce(min(value), '') "
		  . "FROM pg_cluster_state WHERE category = 'cf' AND key = '$key'");
	my ($count, $value) = split(/\t/, $row, 2);
	die "cf.$key must exist exactly once: [$row]"
	  unless defined($count) && $count eq '1' && defined($value);
	die "cf.$key must be unsigned decimal: [$value]"
	  unless $value =~ /\A\d+\z/;
	return int($value);
}

sub cf_x_owner
{
	my ($node) = @_;
	my $row = $node->safe_psql(
		'postgres',
		q{SELECT count(*) || E'\t' || coalesce(min(value), '')
		  FROM pg_cluster_state
		  WHERE category = 'cf' AND key = 'cf_x_owner'});
	my ($count, $value) = split(/\t/, $row, 2);
	die "cf.cf_x_owner must exist exactly once: [$row]"
	  unless defined($count) && $count eq '1' && defined($value);
	die "invalid cf_x_owner grammar: [$value]"
	  unless $value =~
	  /\Astate=(?:EMPTY|HELD|RELEASE_PENDING|AMBIGUOUS|INVALID) pid=-?\d+ procno=\d+ start_us=-?\d+ node=-?\d+ epoch=\d+ request_id=\d+ coordinated=[01]\z/;
	return $value;
}

sub assert_cf_live_terminal
{
	my ($node, $name) = @_;
	my %got = map { $_ => cf_live_value($node, $_) } @cf_live_numeric_keys;
	my $owner = cf_x_owner($node);
	is_deeply(
		\%got,
		{
			cf_slot_snapshot_valid         => 1,
			cf_x_held_count                => 0,
			cf_s_held_count                => 0,
			cf_x_release_pending_count     => 0,
			cf_s_release_pending_count     => 0,
			cf_pending_retry_count         => 0,
			cf_slot_invalid_count          => 0,
		},
		$name);
	is($owner,
		'state=EMPTY pid=0 procno=0 start_us=0 node=0 epoch=0 request_id=0 coordinated=0',
		"$name exposes one exact EMPTY X-owner record");
	return { %got, cf_x_owner => $owner };
}

my $shared_root = PostgreSQL::Test::Utils::tempdir();
mkdir "$shared_root/global" or die "mkdir shared global: $!";

my $disk_dir = PostgreSQL::Test::Utils::tempdir();
my @disks;
for my $i (0 .. 2)
{
	my $path = "$disk_dir/disk$i";
	open(my $fh, '>', $path) or die "open $path: $!";
	binmode $fh;
	print $fh ("\0" x (128 * 512));
	close $fh;
	push @disks, $path;
}
my $disks_csv = join(',', @disks);

my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port1 = PostgreSQL::Test::Cluster::get_free_port();

my $node0 = PostgreSQL::Test::Cluster->new('cf_cleanup_node0');
$node0->init(allows_streaming => 1);
$node0->start;
$node0->backup('cf_cleanup_backup');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('cf_cleanup_node1');
$node1->init_from_backup($node0, 'cf_cleanup_backup');

# Seed the shared control-file authority in the single-node era.
$node0->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.node_id = 0
EOC
$node0->start;
$node0->stop;

my $common_conf = <<EOC;
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = on
cluster.lms_workers = 1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.grd_max_entries = 1024
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
checkpoint_timeout = 1h
max_wal_size = 4GB
autovacuum = off
log_error_verbosity = verbose
EOC
$node0->append_conf('postgresql.conf', $common_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node1->append_conf('postgresql.conf', $common_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = cf_cleanup

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data_port0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data_port1
EOC
PostgreSQL::Test::Utils::append_to_file(
	$node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file(
	$node1->data_dir . '/pgrac.conf', $pgrac_conf);

PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=cf_cleanup_node1', 'start');
$node0->start;
$node1->_update_pid(1);

my $node1_ready = 0;
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres', 'SELECT 1');
	if (defined $rc && $rc == 0)
	{
		$node1_ready = 1;
		last;
	}
	usleep(500_000);
}
ok($node1_ready, 'peer node is ready');
is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'checkpoint node is ready');

# Allow the LMS/GES paths to settle before taking the baseline.
usleep(5_000_000);

# The CF singleton has exactly one hash master.  Select the other node as the
# checkpoint requester so S6 necessarily traverses the remote GES dedup
# terminal producer; a local-master checkpoint cannot prove that path.
my $node0_remote_before =
  state_counter($node0, 'grd', 'grd_remote_master_lookup_count');
$node0->safe_psql('postgres', 'CHECKPOINT');
my $node0_cf_is_remote =
  state_counter($node0, 'grd', 'grd_remote_master_lookup_count')
  > $node0_remote_before;
my $checkpoint_node = $node0_cf_is_remote ? $node0 : $node1;
my $peer_node = $node0_cf_is_remote ? $node1 : $node0;

my $remote_before =
  state_counter($checkpoint_node, 'grd', 'grd_remote_master_lookup_count');
my $dedup_ack_before =
  state_counter($checkpoint_node, 'ges', 'ges_dedup_journal_ack_count');
$checkpoint_node->safe_psql('postgres', 'CHECKPOINT');
ok(
	state_counter(
		$checkpoint_node, 'grd', 'grd_remote_master_lookup_count') > $remote_before,
	'selected checkpointer reaches a remote CF master');
my $dedup_ready = 0;
for (1 .. 50)
{
	if (state_counter(
			$checkpoint_node, 'ges', 'ges_dedup_journal_ack_count') > $dedup_ack_before)
	{
		$dedup_ready = 1;
		last;
	}
	usleep(200_000);
}
ok($dedup_ready, 'remote CF baseline completes a GES dedup DONE/ACK lifecycle');
die "remote CF dedup lifecycle did not become ready"
  unless $dedup_ready;

my $pid_before = checkpointer_pid($checkpoint_node);
my $cf_x_before = state_counter($checkpoint_node, 'cf', 'cf_x_acquire');
my $s6_before =
  state_counter($checkpoint_node, 'cf', 'cf_s6_release_confirmed');
assert_cf_live_terminal($checkpoint_node,
	'live CF census starts valid and empty');

set_cf_held_window(1, $checkpoint_node, $peer_node);
my $held_checkpoint = $checkpoint_node->background_psql(
	'postgres', on_error_die => 1, timeout => 30);
$held_checkpoint->query_until(
	qr/T417_HELD_STARTED/,
	"\\echo T417_HELD_STARTED\nCHECKPOINT;\n\\echo T417_HELD_DONE\n");
my $held_visible = 0;
for (1 .. 50)
{
	if (cf_live_value($checkpoint_node, 'cf_x_held_count') == 1)
	{
		$held_visible = 1;
		last;
	}
	usleep(100_000);
}
ok($held_visible,
	'positive census window exposes one real checkpointer CF X holder');
is(cf_live_value($checkpoint_node, 'cf_x_release_pending_count'), 0,
	'HELD window has no premature RELEASE_PENDING slot');
like(
	cf_x_owner($checkpoint_node),
	qr/\Astate=HELD pid=\Q$pid_before\E .* coordinated=1\z/,
	'HELD owner record binds the real checkpointer PID');
$held_checkpoint->query_until(qr/T417_HELD_DONE/, q{});
$held_checkpoint->quit;
set_cf_held_window(0, $checkpoint_node, $peer_node);
assert_cf_live_terminal($checkpoint_node,
	'positive HELD window drains to an exact terminal census');

# Create the relation after the baseline checkpoint so the checkpointer has
# never opened its shared-storage file.  Removing write permission then makes
# its first CheckPointBuffers open fail after CF X acquisition.
$checkpoint_node->safe_psql(
	'postgres',
	q{CREATE TABLE cf_cleanup_t (id int, v text);
	  INSERT INTO cf_cleanup_t
	  SELECT g, 'base-' || g FROM generate_series(1, 200) g});
my $relpath = $checkpoint_node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('cf_cleanup_t')});
my @relfile_candidates = (
	"$shared_root/$relpath",
	$checkpoint_node->data_dir . "/$relpath");
my ($checkpoint_relfile) = grep { -f $_ } @relfile_candidates;
ok(defined $checkpoint_relfile, 'target relation file exists');
die "could not resolve relation file for $relpath"
  unless defined $checkpoint_relfile;
dirty_relation($checkpoint_node, 1);
chmod 0400, $checkpoint_relfile
  or die "chmod 0400 $checkpoint_relfile: $!";
my $log_off = -s $checkpoint_node->logfile;

# Product-bound cassert seam: WARNING leaves the master-side request path
# functional, while the checkpointer's real S6 nonthrow scope physically fills
# its main outbound ring + cleanup dirty list immediately before the real
# dedup-lifecycle producer attempts to stage its terminal frame.
set_outbound_saturation(1, $checkpoint_node, $peer_node);

# The checkpointer logs the storage ERROR and reports the checkpoint failed.
# The client result is not the assertion surface; holder cleanup and survival are.
$checkpoint_node->psql('postgres', 'CHECKPOINT', timeout => 60);
ok(
	$checkpoint_node->wait_for_log(
		qr/could not open (?:existing )?file .*Permission denied/, $log_off),
	'first checkpoint reaches a real mid-CheckPointGuts storage ERROR');
ok(
	$checkpoint_node->wait_for_log(
		qr/CF S6 test saturation producer=ges_dedup_lifecycle message=GES_DEDUP_DONE filled outbound ring and cleanup dirty list/,
		$log_off),
	'ResourceOwner callback reaches the GES dedup DONE terminal producer');

is(checkpointer_pid($checkpoint_node), $pid_before,
	'checkpointer PID survives storage ERROR plus outbound double-full');
is(state_counter($checkpoint_node, 'cf', 'cf_x_acquire') - $cf_x_before, 1,
	'failed checkpoint acquired CF X exactly once');
is(state_counter($checkpoint_node, 'cf', 'cf_s6_release_confirmed') - $s6_before, 0,
	'double-full ResourceOwner cleanup does not falsely confirm CF S6');

my $failed_log = substr(slurp_file($checkpoint_node->logfile), $log_off);
my $pending_request_id = failed_release_request_id($failed_log);
ok(defined $pending_request_id,
	'exact failed holder request_id is retained and observable as pending');
is(cf_live_value($checkpoint_node, 'cf_slot_snapshot_valid'), 1,
	'failed S6 retains a valid same-epoch CF census');
is(cf_live_value($checkpoint_node, 'cf_x_held_count'), 0,
	'failed S6 revokes HELD before remote cleanup');
is(cf_live_value($checkpoint_node, 'cf_x_release_pending_count'), 1,
	'failed S6 publishes one X RELEASE_PENDING slot');
is(cf_live_value($checkpoint_node, 'cf_pending_retry_count'), 1,
	'failed S6 publishes one exact pending retry');
like(
	cf_x_owner($checkpoint_node),
	qr/\Astate=RELEASE_PENDING pid=\Q$pid_before\E .* request_id=\Q$pending_request_id\E coordinated=1\z/,
	'pending owner record binds the surviving checkpointer and logged request');

set_outbound_saturation(0, $checkpoint_node, $peer_node);
ok(
	$checkpoint_node->poll_query_until(
		'postgres',
		q{SELECT
			(SELECT value::int = 0 FROM pg_cluster_state
			 WHERE category = 'grd' AND key = 'grd_outbound_ring_depth')
			AND
			(SELECT value::int = 0 FROM pg_cluster_state
			 WHERE category = 'grd' AND key = 'grd_outbound_cleanup_dirty_depth')},
		't'),
	'assertion-only saturation frames drain before exact retry');

chmod 0600, $checkpoint_relfile
  or die "chmod 0600 $checkpoint_relfile: $!";

dirty_relation($checkpoint_node, 2);
$checkpoint_node->safe_psql('postgres', 'CHECKPOINT');

is(checkpointer_pid($checkpoint_node), $pid_before,
	'next checkpoint succeeds under the same checkpointer PID');
is(state_counter($checkpoint_node, 'cf', 'cf_x_acquire') - $cf_x_before, 2,
	'two checkpoint attempts acquired CF X exactly twice');
is(state_counter($checkpoint_node, 'cf', 'cf_s6_release_confirmed') - $s6_before, 2,
	'next checkpoint confirms pending exact retry before its own CF S6');

my $run_log = slurp_file($checkpoint_node->logfile, $log_off);
my $retried_request_id = retried_release_request_id($run_log);
is($retried_request_id, $pending_request_id,
	'retry confirms the identical pending holder request_id');
my $terminal_first = assert_cf_live_terminal($checkpoint_node,
	'same-checkpointer retry clears the exact shared CF slot');
usleep(500_000);
my $terminal_second = assert_cf_live_terminal($checkpoint_node,
	'bounded post-drain settle keeps the shared CF slot terminal');
is_deeply($terminal_second, $terminal_first,
	'consecutive final CF census samples are byte-exact stable');
unlike(
	$run_log,
	qr/TRAP:\s+FailedAssertion|Assert failed|SIGABRT|PANIC|FATAL/,
	'cleanup and same-PID retry emit no assertion, abort, FATAL, or PANIC');

$node0->stop;
$node1->stop;
done_testing();
