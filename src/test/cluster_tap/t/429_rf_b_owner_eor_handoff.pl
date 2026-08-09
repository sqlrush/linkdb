#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 429_rf_b_owner_eor_handoff.pl
#    Focused real-process proof for the single-node OWNER-to-EOR handoff.
#
# The observation process watches one restart-log cursor.  It snapshots only
# after the end-of-recovery checkpoint has completed and abandons the dynamic
# zero-write claim if the first phase-4 checkpoint has already started.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use File::Path qw(make_path);
use Fcntl qw(F_GETFL F_SETFL O_NONBLOCK);
use PgracClusterNode;
use PgracWalState qw(read_file_raw read_slot_raw);
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub cf_x_acquire_count
{
	my ($node) = @_;

	return $node->safe_psql('postgres', q{
		SELECT value::bigint FROM pg_cluster_state
		 WHERE category = 'cf' AND key = 'cf_x_acquire'});
}

sub owned_w5_bytes
{
	my ($regfile, $thread_id) = @_;
	my $slot = substr(read_file_raw($regfile),
		512 + ($thread_id - 1) * 512, 512);

	return substr($slot, 56, 8) . substr($slot, 68, 4);
}

sub lsn_to_u64
{
	my ($lsn) = @_;
	my ($hi, $lo) = split m{/}, $lsn, 2;

	return hex($hi) * 4294967296 + hex($lo);
}

sub checkpoint_redo_pair
{
	my ($log) = @_;
	my ($eor_started, $eor_redo, $phase4_started, $phase4_redo);

	for my $line (split /\n/, $log)
	{
		if ($line =~ /checkpoint starting: end-of-recovery\b/)
		{
			$eor_started = 1;
			next;
		}
		if ($eor_started && !defined $eor_redo
			&& $line =~ /checkpoint complete: .*\bredo lsn=([0-9A-F]+\/[0-9A-F]+)\b/i)
		{
			$eor_redo = uc($1);
			next;
		}
		if (defined $eor_redo && !$phase4_started
			&& $line =~ /checkpoint starting: (?!end-of-recovery\b)/)
		{
			$phase4_started = 1;
			next;
		}
		if ($phase4_started && !defined $phase4_redo
			&& $line =~ /checkpoint complete: .*\bredo lsn=([0-9A-F]+\/[0-9A-F]+)\b/i)
		{
			$phase4_redo = uc($1);
			last;
		}
	}

	return ($eor_redo, $phase4_redo);
}

sub publish_observation
{
	my ($path, $record) = @_;

	open(my $fh, '>', $path) or die "open $path: $!";
	print {$fh} "$record\n" or die "write $path: $!";
	close($fh) or die "close $path: $!";
	return;
}

sub observe_eor_boundary
{
	my ($logfile, $cursor, $regfile, $thread_id, $control, $result_path) = @_;
	my $flags = fcntl($control, F_GETFL, 0);
	my $log = '';
	my $eor_started = 0;

	defined $flags or die "fcntl(F_GETFL): $!";
	fcntl($control, F_SETFL, $flags | O_NONBLOCK)
	  or die "fcntl(F_SETFL): $!";

	while (1)
	{
		if (-f $logfile)
		{
			open(my $lfh, '<', $logfile) or die "open $logfile: $!";
			binmode($lfh);
			seek($lfh, $cursor, 0) or die "seek $logfile: $!";
			local $/;
			$log = <$lfh> // '';
			close($lfh) or die "close $logfile: $!";

			$eor_started = 1
			  if $log =~ /checkpoint starting: end-of-recovery\b/;
			if ($eor_started
				&& $log =~ /checkpoint starting: end-of-recovery\b.*?checkpoint complete: .*?\bredo lsn=([0-9A-F]+\/[0-9A-F]+)\b/is)
			{
				my $eor_redo = uc($1);
				my $after_eor = substr($log, $+[0]);

				if ($after_eor =~ /checkpoint starting: (?!end-of-recovery\b)/)
				{
					publish_observation($result_path,
						join("\t", 'BOUNDARY_UNREACHABLE',
							'phase4-start-visible-with-eor-complete'));
					exit 0;
				}

				my $snapshot = owned_w5_bytes($regfile, $thread_id);

				open(my $postfh, '<', $logfile) or die "open $logfile: $!";
				binmode($postfh);
				seek($postfh, $cursor, 0) or die "seek $logfile: $!";
				local $/;
				my $post_snapshot_log = <$postfh> // '';
				close($postfh) or die "close $logfile: $!";
				my $post_eor = $post_snapshot_log;
				$post_eor =~ s/^.*?checkpoint starting: end-of-recovery\b.*?checkpoint complete: .*?\bredo lsn=[0-9A-F]+\/[0-9A-F]+\b//is;
				if ($post_eor =~ /checkpoint starting: (?!end-of-recovery\b)/)
				{
					publish_observation($result_path,
						join("\t", 'BOUNDARY_UNREACHABLE',
							'phase4-start-raced-boundary-snapshot'));
					exit 0;
				}

				publish_observation($result_path,
					join("\t", 'SNAPSHOT', $eor_redo, unpack('H*', $snapshot)));
				exit 0;
			}
		}

		my $control_record = '';
		my $nread = sysread($control, $control_record, 256);
		if (defined $nread && $nread > 0)
		{
			if ($log =~ /could not acquire the cluster control-file lock for a checkpoint.*checkpoint request failed/s)
			{
				publish_observation($result_path,
					join("\t", 'EOR_FAILED',
						'checkpointer-cf-x-owner-handoff-missing'));
			}
			else
			{
				publish_observation($result_path,
					join("\t", 'BOUNDARY_UNREACHABLE',
						'startup-finished-without-isolated-eor-snapshot'));
			}
			exit 0;
		}

		usleep(1_000);
	}
}

my $wal_root = PostgreSQL::Test::Utils::tempdir();
my $regfile = "$wal_root/pgrac_wal_state";
my $shared_root = PostgreSQL::Test::Utils::tempdir();
my $result_path = "$wal_root/eor-boundary.result";
my $thread_id = 14;

make_path("$shared_root/global");

my $node = PgracClusterNode->new('rf_b_owner_eor');
$node->init(extra => [
	'-X', "$wal_root/thread_$thread_id",
	"--pgrac-wal-state-root=$wal_root" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 13\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wal_root'\n"
	  . "cluster.shared_storage_backend = cluster_fs\n"
	  . "cluster.shared_data_dir = '$shared_root'\n"
	  . "cluster.smgr_user_relations = on\n"
	  . "cluster.controlfile_shared_authority = on\n"
	  . "cluster.cluster_stats_main_loop_interval = '500ms'\n"
	  . "autovacuum = off\n");
$node->start;

$node->safe_psql('postgres', q{
	ALTER SYSTEM SET full_page_writes = 'off';
	SELECT pg_reload_conf();});
ok($node->poll_query_until('postgres',
	q{SELECT current_setting('full_page_writes') = 'off'}),
	'precondition: crash history has full_page_writes=off');
$node->safe_psql('postgres', 'CHECKPOINT');
is(read_slot_raw($regfile, $thread_id)->{fpw_was_off}, 1,
	'precondition: own W5 slot contains valid FPW-off evidence');

$node->stop('immediate');
my $preimage = owned_w5_bytes($regfile, $thread_id);
my $restart_cursor = -s $node->logfile;
pipe(my $control_read, my $control_write) or die "pipe: $!";
my $observer_pid = fork();

defined $observer_pid or die "fork: $!";
if ($observer_pid == 0)
{
	close($control_write);
	observe_eor_boundary($node->logfile, $restart_cursor, $regfile,
		$thread_id, $control_read, $result_path);
	exit 2;
}

close($control_read);
my $restart_ok = $node->start(fail_ok => 1);
{
	local $SIG{PIPE} = 'IGNORE';
	syswrite($control_write, "START_DONE\t$restart_ok\n");
}
close($control_write);
waitpid($observer_pid, 0);
is($? >> 8, 0, 'EOR boundary observer exits cleanly');

my $restart_log = PostgreSQL::Test::Utils::slurp_file(
	$node->logfile, $restart_cursor);
is($restart_ok, 1,
	'real sticky=1 crash restart completes the OWNER-to-EOR handoff');

if (!$restart_ok)
{
	my $failure_observation = PostgreSQL::Test::Utils::slurp_file($result_path);
	chomp($failure_observation);
	is($failure_observation,
		join("\t", 'EOR_FAILED', 'checkpointer-cf-x-owner-handoff-missing'),
		'RED observer binds failure to the real EOR Checkpointer path');
	like($restart_log,
		qr/could not acquire the cluster control-file lock for a checkpoint.*checkpoint request failed/s,
		'RED: real Checkpointer exposes the missing OWNER-to-EOR handoff');
	done_testing();
	exit;
}

my $observation = PostgreSQL::Test::Utils::slurp_file($result_path);
chomp($observation);
my ($kind, $observed_eor, $observed_bytes) = split /\t/, $observation, 3;

if ($kind eq 'SNAPSHOT')
{
	pass('real EOR-complete/pre-phase4 snapshot boundary is reachable');
	is($observed_bytes, unpack('H*', $preimage),
		'EOR_W5_ZERO_WRITE=PASS: exact bytes 56..63/68..71 match preimage');
}
else
{
	fail('real EOR-complete/pre-phase4 snapshot boundary is reachable');
	diag("BOUNDARY_UNREACHABLE: $observation");
}

my ($eor_redo, $phase4_redo) = checkpoint_redo_pair($restart_log);
is($eor_redo, $observed_eor,
	'snapshot is bound to the completed EOR redo from the same log cursor')
	if $kind eq 'SNAPSHOT';
ok(defined $phase4_redo, 'same log cursor records the following phase4 checkpoint redo');
cmp_ok(lsn_to_u64($eor_redo), '<', lsn_to_u64($phase4_redo),
	'dynamic checkpoint redo order is E < P')
	if defined $eor_redo && defined $phase4_redo;

my $cf_x_before = cf_x_acquire_count($node);
$node->safe_psql('postgres', 'CHECKPOINT');
cmp_ok(cf_x_acquire_count($node), '>', $cf_x_before,
	'following steady SQL CHECKPOINT acquires real CF(X)');
$node->stop;

done_testing();
