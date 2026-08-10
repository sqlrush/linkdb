#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 8_4_005_epoch_restart_4n.pl
#    Stage 8 R4 dormant-profile four-node epoch/restart contract.
#
# The current profile deliberately proves only the deploy-disabled shape:
# a fresh formation starts at epoch zero, role-node restarts invalidate their
# old boot/control identities, ordinary source SQL remains usable, and no R4
# target record or traffic becomes reachable.  Positive activation remains a
# hard-unavailable profile until its separately governed prerequisites exist.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/8_4_005_epoch_restart_4n.pl
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Digest::SHA qw(sha256_hex);
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my $profile = $ENV{PGRAC_R4_TEST_PROFILE} // 'dormant';

if ($profile eq 'activation')
{
	BAIL_OUT('CONDITION_NOT_YET_MET: PGRAC_R4_TEST_PROFILE=activation '
		. 'is DEFERRED_BY_USER_RF_DECISION');
}
BAIL_OUT("unsupported PGRAC_R4_TEST_PROFILE=$profile")
	unless $profile eq 'dormant';

my $SQL_TIMEOUT_S = 10;
my $WAIT_TIMEOUT_S = 90;
my $VOTING_FILE_BYTES_MIN = (5 * 128 + 1) * 512;

sub sql_once
{
	my ($node, $sql, $timeout_s) = @_;
	$timeout_s //= $SQL_TIMEOUT_S;

	my $started = time();
	my $timed_out = 0;
	my ($rc, $stdout, $stderr) =
	  $node->psql('postgres', $sql, timeout => $timeout_s,
		timed_out => \$timed_out);
	return (undef, $stdout // '', $stderr // '', time() - $started, 1)
	  if $timed_out;
	return ($rc, $stdout // '', $stderr // '', time() - $started, 0);
}

sub scalar_once
{
	my ($node, $sql) = @_;
	my ($rc, $stdout, $stderr) = sql_once($node, $sql);
	return (undef, $stderr) unless defined($rc) && $rc == 0;
	return ($stdout, '');
}

sub epoch_of
{
	my ($node) = @_;
	my ($value, $error) = scalar_once($node,
		'SELECT coalesce(max(new_epoch), 0) FROM pg_cluster_reconfig_state');
	return undef unless defined($value) && $value =~ /\A\d+\z/;
	return $value + 0;
}

sub peer_state
{
	my ($observer, $peer_id) = @_;
	my ($value, $error) = scalar_once($observer,
		"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $peer_id");
	return $value;
}

sub reconnect_generation
{
	my ($observer, $peer_id) = @_;
	my ($value, $error) = scalar_once($observer,
		"SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = $peer_id");
	return undef unless defined($value) && $value =~ /\A\d+\z/;
	return $value + 0;
}

sub membership_identity
{
	my ($observer, $peer_id) = @_;
	my ($value, $error) = scalar_once($observer,
		"SELECT state || '|' || presented_incarnation || '|' || "
		. "last_admitted_incarnation || '|' || admitted_epoch "
		. "FROM pg_cluster_membership WHERE node_id = $peer_id");
	return undef unless defined $value;
	my ($state, $presented, $admitted, $epoch) = split(/\|/, $value, 4);
	return undef
	  unless defined($epoch)
	  && $presented =~ /\A\d+\z/
	  && $admitted =~ /\A\d+\z/
	  && $epoch =~ /\A\d+\z/;
	return {
		state => $state,
		presented => $presented + 0,
		admitted => $admitted + 0,
		epoch => $epoch + 0,
	};
}

sub aux_pids
{
	my ($node) = @_;
	my ($value, $error) = scalar_once($node,
		q{SELECT backend_type || '|' || min(pid)::text
		  FROM pg_stat_activity
		  WHERE backend_type IN ('lmon', 'lms')
		  GROUP BY backend_type
		  ORDER BY backend_type});
	return undef unless defined $value;

	my %pids;
	for my $row (grep { $_ ne '' } split(/\n/, $value))
	{
		my ($type, $pid) = split(/\|/, $row, 2);
		return undef
		  unless defined($type)
		  && defined($pid)
		  && $pid =~ /\A\d+\z/
		  && ($type eq 'lmon' || $type eq 'lms');
		$pids{$type} = $pid + 0;
	}
	return undef unless exists($pids{lmon}) && exists($pids{lms});
	return \%pids;
}

sub formation_ready
{
	my ($quad) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;

	while (time() < $deadline)
	{
		my $ready = 1;
		for my $i (0 .. 3)
		{
			my ($value, $error) = scalar_once($quad->node($i),
				"SELECT (SELECT in_quorum::int FROM pg_cluster_quorum_state) "
				. "|| '|' || (SELECT count(*) FROM pg_cluster_ic_peers "
				. "WHERE node_id <> $i AND state = 'connected') "
				. "|| '|' || (SELECT count(*) FROM pg_cluster_membership "
				. "WHERE state = 'member')");
			if (!defined($value) || $value ne '1|3|4')
			{
				$ready = 0;
				last;
			}
		}
		return 1 if $ready;
		usleep(250_000);
	}
	return 0;
}

sub max_epoch_of
{
	my ($quad, @node_ids) = @_;
	my $maximum;

	for my $node_id (@node_ids)
	{
		my $epoch = epoch_of($quad->node($node_id));
		$maximum = $epoch
		  if defined($epoch) && (!defined($maximum) || $epoch > $maximum);
	}
	return $maximum;
}

sub wait_epoch_advance
{
	my ($quad, $before, @node_ids) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;
	while (time() < $deadline)
	{
		my $epoch = max_epoch_of($quad, @node_ids);
		return $epoch if defined($epoch) && $epoch > $before;
		usleep(250_000);
	}
	return undef;
}

sub wait_aux_pids
{
	my ($node) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;
	while (time() < $deadline)
	{
		my $pids = aux_pids($node);
		return $pids if defined $pids;
		usleep(250_000);
	}
	return undef;
}

sub wait_reconnect_advance
{
	my ($observer, $peer_id, $before) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;
	while (time() < $deadline)
	{
		my $generation = reconnect_generation($observer, $peer_id);
		return $generation
		  if defined($generation) && $generation > $before;
		usleep(250_000);
	}
	return undef;
}

sub wait_membership_identity_advance
{
	my ($observer, $peer_id, $before) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;
	while (time() < $deadline)
	{
		my $identity = membership_identity($observer, $peer_id);
		return $identity
		  if defined($identity)
		  && $identity->{state} eq 'member'
		  && $identity->{presented} > $before->{presented}
		  && $identity->{admitted} > $before->{admitted};
		usleep(250_000);
	}
	return undef;
}

sub source_sql_status
{
	my ($quad, @node_ids) = @_;
	for my $i (@node_ids)
	{
		my ($rc, $stdout, $stderr, $elapsed, $timed_out) =
		  sql_once($quad->node($i), q{
			SELECT 42, current_setting('transaction_isolation'), txid_current() > 0;
		});
		unless (defined($rc) && $rc == 0 && $stdout eq '42|read committed|t')
		{
			return (0, "node$i source SQL failed: rc="
				. (defined($rc) ? $rc : '<undef>')
				. " timed_out=$timed_out elapsed=$elapsed "
				. "stdout=[$stdout] stderr=[$stderr]", $timed_out);
		}
	}
	return (1, '', 0);
}

sub source_sql_usable
{
	my ($quad, @node_ids) = @_;
	my ($usable, $evidence) = source_sql_status($quad, @node_ids);
	diag($evidence) unless $usable;
	return $usable;
}

sub wait_source_sql_usable
{
	my ($quad, @node_ids) = @_;
	my $deadline = time() + $WAIT_TIMEOUT_S;
	my $last_evidence = 'source SQL was not attempted';

	while (time() < $deadline)
	{
		my ($usable, $evidence, $timed_out) =
		  source_sql_status($quad, @node_ids);
		return 1 if $usable;
		$last_evidence = $evidence;
		diag("bounded source readiness retry: $evidence") if $timed_out;
		usleep(250_000);
	}
	diag("bounded source readiness deadline expired: $last_evidence");
	return 0;
}

sub provision_voting_file_minimum
{
	my ($disk_paths, $minimum) = @_;
	for my $path (@$disk_paths)
	{
		my $size = -s $path;
		return 0 unless defined($size) && $size <= $minimum;
		open(my $fh, '+<', $path) or die "open $path: $!";
		binmode($fh);
		truncate($fh, $minimum) or die "truncate $path to $minimum: $!";
		close($fh) or die "close $path: $!";
		return 0 unless (-s $path) == $minimum;
	}
	return 1;
}

sub file_tail_sha256
{
	my ($path, $offset) = @_;
	open(my $fh, '<', $path) or die "open $path: $!";
	binmode($fh);
	seek($fh, $offset, 0) or die "seek $path to $offset: $!";

	my $digest = Digest::SHA->new(256);
	while (1)
	{
		my $buffer = '';
		my $read = read($fh, $buffer, 8192);
		die "read $path: $!" unless defined $read;
		last if $read == 0;
		$digest->add($buffer);
	}
	close($fh) or die "close $path: $!";
	return $digest->hexdigest;
}

sub semantic_tail_snapshot
{
	my ($disk_paths, $offset) = @_;
	my @rows;
	for my $i (0 .. $#$disk_paths)
	{
		my $path = $disk_paths->[$i];
		my $size = -s $path;
		return "disk$i:MISSING" unless defined $size;
		return "disk$i:SHORT:$size" if $size < $offset;
		push @rows, join(':', "disk$i", $size,
			file_tail_sha256($path, $offset));
	}
	return join(';', @rows);
}

sub legacy_tail_is_empty
{
	my ($disk_paths, $offset) = @_;
	my $empty_sha = sha256_hex('');
	my $snapshot = semantic_tail_snapshot($disk_paths, $offset);
	my $expected = join(';', map { "disk$_:$offset:$empty_sha" }
		(0 .. $#$disk_paths));
	return ($snapshot eq $expected, $snapshot, $expected);
}

sub wait_legacy_tail_empty
{
	my ($disk_paths, $offset) = @_;
	my $deadline = time() + 30;
	my ($ok, $actual, $expected);
	while (time() < $deadline)
	{
		($ok, $actual, $expected) = legacy_tail_is_empty($disk_paths, $offset);
		return ($ok, $actual, $expected) if $ok;
		return ($ok, $actual, $expected) if $actual =~ /:(?:LONG|\d+):/ && $actual !~ /:$offset:/;
		usleep(100_000);
	}
	return ($ok, $actual, $expected);
}

sub check_empty_tail
{
	my ($disk_paths, $offset, $label) = @_;
	my ($ok, $actual, $expected) = legacy_tail_is_empty($disk_paths, $offset);
	ok($ok, $label) or diag("expected [$expected], observed [$actual]");
}

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'r4_epoch_restart',
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		'cluster.lms_enabled = on',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 500',
		'cluster.cssd_dead_deadband_factor = 6',
	]);

my @voting_disks = $quad->voting_disk_paths;
BAIL_OUT('ClusterQuad did not create the exact three voting-disk fixtures')
	unless scalar(@voting_disks) == 3;
BAIL_OUT('could not provision the existing voting-file minimum')
	unless provision_voting_file_minimum(\@voting_disks,
		$VOTING_FILE_BYTES_MIN);

$quad->start_quad;
ok(formation_ready($quad),
	'L1 real four-node strict-quorum formation is fully admitted and connected');

my @fresh_epochs = map { epoch_of($quad->node($_)) } (0 .. 3);
ok(!(grep { !defined($_) || $_ != 0 } @fresh_epochs),
	'L2 fresh formation epoch is exactly 0 on all four nodes')
	  or diag('fresh epochs: ' . join(',', map { defined($_) ? $_ : '<undef>' }
		@fresh_epochs));

my @initial_aux = map { aux_pids($quad->node($_)) } (0 .. 3);
ok(!(grep { !defined($_) } @initial_aux),
	'L3 every node exposes live LMON and LMS process checkpoints');

is(scalar(@voting_disks), 3, 'L4 strict formation owns exactly three voting disks');

my ($voting_bytes, $voting_error) = scalar_once($quad->node3,
	q{SELECT setting FROM pg_settings WHERE name = 'cluster.voting_disk_size_bytes'});
is($voting_bytes, "$VOTING_FILE_BYTES_MIN",
	'L4 existing startup GUC equals the compiled old voting-file minimum');
$voting_bytes = $VOTING_FILE_BYTES_MIN;

my ($tail_ready, $tail_actual, $tail_expected) =
	wait_legacy_tail_empty(\@voting_disks, $voting_bytes);
ok($tail_ready,
	'L4 fresh dormant formation has no PGSA bytes beyond the old minimum')
	  or diag("expected [$tail_expected], observed [$tail_actual]");

ok(source_sql_usable($quad, 0 .. 3),
	'L5 ordinary source transaction SQL is usable on all four nodes');
ok(source_sql_usable($quad, 0 .. 3),
	'L5 ordinary source transaction SQL remains usable before any restart');

# Node 3 stays up throughout and supplies only existing control/formation
# observations.  reconnect_count is the existing connection-generation
# witness; the membership view supplies boot/admission identity.  There is no
# test-only ACK reader: non-consumption is proved at the authority outputs
# below (no PGSA tail, no target traffic and a typed pre-PREPARE refusal).
my @roles = (
	[ requester => 0 ],
	[ holder => 1 ],
	[ origin => 2 ],
);

for my $role (@roles)
{
	my ($role_name, $node_id) = @$role;
	my $node = $quad->node($node_id);
	my $observer = $quad->node3;

	ok(source_sql_usable($quad, 0 .. 3),
		"$role_name checkpoint: source SQL usable before restart");
	check_empty_tail(\@voting_disks, $voting_bytes,
		"$role_name checkpoint: target stays closed before restart");

	my $old_epoch = max_epoch_of($quad, 0 .. 3);
	my $old_generation = reconnect_generation($observer, $node_id);
	my $old_identity = membership_identity($observer, $node_id);
	my $old_pids = aux_pids($node);
	my @survivors = grep { $_ != $node_id } (0 .. 3);
	ok(defined($old_epoch) && defined($old_generation)
		&& defined($old_identity) && defined($old_pids),
		"$role_name checkpoint: old epoch/boot/CONTROL/LMON/LMS tuple captured");

	my $stop_ok = $node->stop('fast', fail_ok => 1);
	ok($stop_ok, "$role_name restart: role node stopped cleanly");

	my $down_epoch = defined($old_epoch)
	  ? wait_epoch_advance($quad, $old_epoch, @survivors)
	  : undef;
	ok(defined($down_epoch) && $down_epoch > $old_epoch,
		"$role_name restart: surviving formation advances epoch while role is unavailable");

	my $down_state = peer_state($observer, $node_id);
	ok(defined($down_state) && $down_state ne 'connected',
		"$role_name restart: observer no longer treats the old CONTROL connection as live");

	ok(wait_source_sql_usable($quad, @survivors),
		"$role_name unavailable: survivor source SQL remains usable");
	check_empty_tail(\@voting_disks, $voting_bytes,
		"$role_name unavailable: no stale semantic record or ACK opens target");

	my $start_ok = $node->start(fail_ok => 1);
	ok($start_ok, "$role_name restart: role node starts with a fresh boot");

	my $ready = formation_ready($quad);
	ok($ready, "$role_name restart: fresh role boot is re-admitted to all four nodes");

	my $rejoined_epoch = max_epoch_of($quad, 0 .. 3);
	ok(defined($rejoined_epoch) && defined($down_epoch)
		&& $rejoined_epoch >= $down_epoch,
		"$role_name restart: current formation retains the advanced epoch");

	my $new_pids = wait_aux_pids($node);
	ok(defined($new_pids) && defined($old_pids)
		&& $new_pids->{lmon} != $old_pids->{lmon},
		"$role_name restart: LMON process checkpoint has a fresh pid");
	ok(defined($new_pids) && defined($old_pids)
		&& $new_pids->{lms} != $old_pids->{lms},
		"$role_name restart: LMS process checkpoint has a fresh pid");

	my $new_generation = defined($old_generation)
	  ? wait_reconnect_advance($observer, $node_id, $old_generation)
	  : undef;
	ok(defined($new_generation) && $new_generation > $old_generation,
		"$role_name restart: CONTROL connection generation advances; old ACK tuple is stale");

	my $new_identity = defined($old_identity)
	  ? wait_membership_identity_advance($observer, $node_id, $old_identity)
	  : undef;
	ok(defined($new_identity),
		"$role_name restart: boot/admitted incarnation advances; old ACK tuple cannot match");

	ok(source_sql_usable($quad, 0 .. 3),
		"$role_name checkpoint: source SQL usable after re-admission");
	check_empty_tail(\@voting_disks, $voting_bytes,
		"$role_name checkpoint: target remains closed after re-admission");
}

check_empty_tail(\@voting_disks, $voting_bytes,
	'L6 pre-ENABLE PGSA tail is empty at the old voting-file minimum');
ok(source_sql_usable($quad, 0 .. 3),
	'L6 source admission is open immediately before ENABLE refusal');

my $pgsa_before = semantic_tail_snapshot(\@voting_disks, $voting_bytes);
my ($enable_rc, $enable_stdout, $enable_stderr, $enable_elapsed) =
	sql_once($quad->node3,
		'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL;', 10);
my $enable_evidence = join("\n", $enable_stdout, $enable_stderr);

isnt($enable_rc, 0, 'L6 one dormant ENABLE attempt is refused');
cmp_ok($enable_elapsed, '<', 10,
	'L6 dormant ENABLE refusal is bounded and does not hang');
like($enable_evidence, qr/\bRF_DEFERRED\b/,
	'L6 dormant ENABLE refusal carries typed RF_DEFERRED');
like($enable_evidence, qr/\bCONDITION_NOT_YET_MET\b/,
	'L6 dormant ENABLE refusal carries literal CONDITION_NOT_YET_MET');
unlike($enable_evidence, qr/\b(?:PREPARE|COMMIT|TARGET_OPEN)\b/,
	'L6 refusal occurs before PREPARE or any target-open result');

my $pgsa_after = semantic_tail_snapshot(\@voting_disks, $voting_bytes);
is($pgsa_after, $pgsa_before,
	'L6 refused ENABLE changes neither PGSA size nor PGSA tail hash');
ok(source_sql_usable($quad, 0 .. 3),
	'L6 refused ENABLE does not close ordinary source admission');

my $target_wire = qr/
	(?:R4_(?:CR_BUILD|TX_RESOLVE|MULTI_RESOLVE).*(?:send|recv|wire))
	|
	(?:(?:send|recv|wire).*R4_(?:CR_BUILD|TX_RESOLVE|MULTI_RESOLVE))
	|
	(?:SEMANTIC_ACTIVATION_(?:PREPARE|ACK|COMMIT|OPEN).*(?:send|recv|wire))
	|
	(?:(?:send|recv|wire).*SEMANTIC_ACTIVATION_(?:PREPARE|ACK|COMMIT|OPEN))
/ix;

for my $i (0 .. 3)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($quad->node($i)->logfile);
	unlike($log, $target_wire,
		"L7 node$i log has no R4 target or activation-control wire");
}

for my $i (0 .. 3)
{
	my $stopped = $quad->node($i)->stop('fast', fail_ok => 1);
	ok($stopped, "L8 node$i shuts down cleanly without forced cancel");
}
ok(!(grep { -e $quad->node($_)->data_dir . '/postmaster.pid' } (0 .. 3)),
	'L8 all four postmasters are down after clean shutdown');

done_testing();
