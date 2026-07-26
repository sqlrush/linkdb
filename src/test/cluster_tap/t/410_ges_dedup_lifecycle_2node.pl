#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 410_ges_dedup_lifecycle_2node.pl
#    S3-P0-10 -- reliable GES retransmit-dedup completion lifecycle.
#
#    This first live gate proves the normal terminal paths on one immutable
#    binary:
#      L1  both nodes expose the receiver-dedup and requester-journal rows;
#      L2  a remote-master GRANT produces a lifecycle ACK;
#      L3  a remote-master NOWAIT REJECT produces a lifecycle ACK;
#      L4  after each terminal, receiver entries and requester journal rows
#          converge to zero on both nodes, with no capacity rejection.
#
#    A key has exactly one GES master.  The test acquires it from each node
#    until the requester-side ACK identifies the remote-master direction.
#    It then holds the key on that master and drives the conflicting NOWAIT
#    request from the other node, making the REJECT remote deterministically.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/410_ges_dedup_lifecycle_2node.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(time usleep);

my ($pair, $n0, $n1);
my $stopped_lmon_pid = 0;
my $stopped_backend_pid = 0;

END
{
	kill 'CONT', $stopped_lmon_pid
	  if defined($stopped_lmon_pid) && $stopped_lmon_pid > 0;
	kill 'CONT', $stopped_backend_pid
	  if defined($stopped_backend_pid) && $stopped_backend_pid > 0;
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

sub lifecycle_occupancy
{
	my $total = 0;

	for my $node ($n0, $n1)
	{
		$total += ges_int($node, 'ges_dedup_entry_count');
		$total += ges_int($node, 'ges_dedup_journal_count');
	}
	return $total;
}

sub wait_lifecycle_empty
{
	return poll_until(sub { lifecycle_occupancy() == 0 }, 15);
}

sub wait_backend_event
{
	my ($node, $pid, $event, $seconds) = @_;
	return poll_until(
		sub {
			my $seen = $node->safe_psql(
				'postgres', qq{
				SELECT coalesce(wait_event, '') FROM pg_stat_activity
				WHERE pid = $pid AND state = 'active'});
			defined($seen) && $seen eq $event;
		},
		$seconds);
}

sub state_int_both
{
	my ($category, $key) = @_;
	my $total = 0;

	for my $node ($n0, $n1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = '$category' AND key = '$key'});
		$total += defined($v) && $v ne '' ? int($v) : 0;
	}
	return $total;
}

sub relation_grd_int
{
	my ($node, $database_oid, $relation_oid, $column) = @_;

	die "invalid GRD aggregate column $column"
	  unless $column eq 'ngranted' || $column eq 'nconverts';
	my $v = $node->safe_psql(
		'postgres', qq{
		SELECT coalesce(sum($column), 0) FROM pg_cluster_grd_entries
		WHERE field1 = $database_oid AND field2 = $relation_oid
		  AND field3 = 0 AND field4 = 0
		  AND type = 0 AND lockmethodid = 1});
	return defined($v) && $v ne '' ? int($v) : 0;
}

sub relation_grd_snapshot
{
	my ($node, $database_oid, $relation_oid) = @_;
	my $v = $node->safe_psql(
		'postgres', qq{
		SELECT coalesce(sum(ngranted), 0) || ',' ||
			   coalesce(sum(nconverts), 0)
		FROM pg_cluster_grd_entries
		WHERE field1 = $database_oid AND field2 = $relation_oid
		  AND field3 = 0 AND field4 = 0
		  AND type = 0 AND lockmethodid = 1});
	return defined($v) && $v ne '' ? $v : '0,0';
}

sub release_and_close
{
	my ($session, $key) = @_;

	is($session->query_safe("SELECT pg_advisory_unlock($key)"),
		't', "released probe key $key");
	$session->quit;
}

$pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ges_dedup_lifecycle',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_dedup_max_entries = 256',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.ges_convert_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'restart_after_crash = on',
	]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

# L9 needs one deterministic native-probe result without weakening the
# production path.  On the initial, unmodified startup, create the ordinary
# same-OID fixture and discover its relation-lock master from an actual remote
# lifecycle ACK.  Then stop both postmasters normally, arm the assertion-build
# one-shot on that exact master + relation LOCKTAG, and restart.  The token is
# still untouched by L1-L8 because those legs use advisory resources.
my $convert_relation = 't410_convert_fixture';
my ($convert_database_oid, $convert_relation_oid);
my ($tm_requester, $tm_master, $tm_requester_idx, $tm_master_idx);
unless ($ENV{PGRAC_T410_L1_L8_ONLY})
{
	BAIL_OUT('L9 bootstrap node0 cannot see node1 connected')
	  unless $pair->wait_for_peer_state(0, 1, 'connected', 30);
	BAIL_OUT('L9 bootstrap node1 cannot see node0 connected')
	  unless $pair->wait_for_peer_state(1, 0, 'connected', 30);

	$n0->safe_psql(
		'postgres', "CREATE TABLE $convert_relation (id integer)");
	$n1->safe_psql(
		'postgres', "CREATE TABLE $convert_relation (id integer)");

	my $database_oid0 = int($n0->safe_psql(
			'postgres',
			q{SELECT oid FROM pg_database WHERE datname = current_database()}));
	my $database_oid1 = int($n1->safe_psql(
			'postgres',
			q{SELECT oid FROM pg_database WHERE datname = current_database()}));
	my $relation_oid0 = int($n0->safe_psql(
			'postgres', "SELECT '$convert_relation'::regclass::oid"));
	my $relation_oid1 = int($n1->safe_psql(
			'postgres', "SELECT '$convert_relation'::regclass::oid"));
	BAIL_OUT(
		"L9 fixture database OID mismatch: node0=$database_oid0 node1=$database_oid1")
	  unless $database_oid0 == $database_oid1;
	BAIL_OUT(
		"L9 fixture relation OID mismatch: node0=$relation_oid0 node1=$relation_oid1")
	  unless $relation_oid0 == $relation_oid1;
	$convert_database_oid = $database_oid0;
	$convert_relation_oid = $relation_oid0;

	BAIL_OUT('L9 fixture setup left lifecycle rows before master discovery')
	  unless wait_lifecycle_empty();
	my @probe_remote = (0, 0);
	for my $idx (0, 1)
	{
		my $node = $idx == 0 ? $n0 : $n1;
		my $ack_before =
		  ges_int($node, 'ges_dedup_journal_ack_count');
		my $probe = $node->background_psql(
			'postgres', on_error_die => 1);

		$probe->query_safe('BEGIN');
		$probe->query_safe(
			"LOCK TABLE $convert_relation IN SHARE MODE");
		$probe_remote[$idx] = poll_until(
			sub {
				ges_int($node, 'ges_dedup_journal_ack_count')
				  > $ack_before;
			},
			10) ? 1 : 0;
		$probe->query_safe('ROLLBACK');
		$probe->quit;
		BAIL_OUT("L9 node$idx master probe left lifecycle rows")
		  unless wait_lifecycle_empty();
	}
	BAIL_OUT(
		"L9 relation master discovery was not unique "
		  . "(node0_remote=$probe_remote[0] node1_remote=$probe_remote[1])")
	  unless $probe_remote[0] + $probe_remote[1] == 1;

	if ($probe_remote[0])
	{
		$tm_requester = $n0;
		$tm_master = $n1;
		$tm_requester_idx = 0;
		$tm_master_idx = 1;
	}
	else
	{
		$tm_requester = $n1;
		$tm_master = $n0;
		$tm_requester_idx = 1;
		$tm_master_idx = 0;
	}

	$pair->stop_pair;
	$tm_master->append_conf(
		'postgresql.conf',
		"cluster.unsafe_test_native_probe_force_clear_once = 1\n"
		  . "cluster.unsafe_test_native_probe_force_clear_node_id = $tm_master_idx\n"
		  . "cluster.unsafe_test_native_probe_force_clear_database_oid = $convert_database_oid\n"
		  . "cluster.unsafe_test_native_probe_force_clear_relation_oid = $convert_relation_oid\n");
	$pair->start_pair;
	usleep(3_000_000);
}

ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my @lifecycle_keys = qw(
  ges_dedup_entry_count
  ges_dedup_hit_cached_count
  ges_dedup_in_flight_dup_count
  ges_dedup_stale_reprocess_count
  ges_dedup_full_reject_count
  ges_dedup_journal_count
  ges_dedup_journal_full_count
  ges_dedup_journal_ack_count);
for my $idx (0, 1)
{
	my $node = $idx == 0 ? $n0 : $n1;
	my $list = join(',', map { "'$_'" } @lifecycle_keys);
	is($node->safe_psql(
			'postgres', qq{
			SELECT count(*) FROM pg_cluster_state
			WHERE category = 'ges' AND key IN ($list)}),
		scalar(@lifecycle_keys),
		"L1 node$idx exposes all lifecycle counters");
}
ok(wait_lifecycle_empty(),
	'L1 receiver dedup and requester journal start empty on both nodes');

my $full_before =
  ges_int($n0, 'ges_dedup_full_reject_count')
  + ges_int($n1, 'ges_dedup_full_reject_count')
  + ges_int($n0, 'ges_dedup_journal_full_count')
  + ges_int($n1, 'ges_dedup_journal_full_count');

my ($master, $requester, $master_idx, $requester_idx, $chosen_key);
my $remote_grant_seen = 0;

KEY:
for my $key (41001 .. 41016)
{
	for my $idx (0, 1)
	{
		my $node = $idx == 0 ? $n0 : $n1;
		my $peer = $idx == 0 ? $n1 : $n0;
		my $ack_before = ges_int($node, 'ges_dedup_journal_ack_count');
		my $session = $node->background_psql('postgres', on_error_die => 1);

		$session->query_safe("SELECT pg_advisory_lock($key)");
		if (poll_until(
				sub {
					ges_int($node, 'ges_dedup_journal_ack_count')
					  > $ack_before;
				},
				5))
		{
			$requester = $node;
			$master = $peer;
			$requester_idx = $idx;
			$master_idx = 1 - $idx;
			$chosen_key = $key;
			$remote_grant_seen = 1;
			release_and_close($session, $key);
			last KEY;
		}
		release_and_close($session, $key);
		ok(wait_lifecycle_empty(),
			"L2 local-master probe key $key on node$idx leaves no lifecycle rows");
	}
}

ok($remote_grant_seen,
	'L2 found and completed a remote-master GRANT with a lifecycle ACK');
ok(wait_lifecycle_empty(),
	'L2 remote GRANT receiver entry + requester journal converge to zero');

SKIP:
{
	skip 'no remote-master direction found', 4 unless $remote_grant_seen;

	my $holder = $master->background_psql('postgres', on_error_die => 1);
	$holder->query_safe("SELECT pg_advisory_lock($chosen_key)");

	my $ack_before =
	  ges_int($requester, 'ges_dedup_journal_ack_count');
	my $try = $requester->background_psql('postgres', on_error_die => 1);
	is($try->query_safe("SELECT pg_try_advisory_lock($chosen_key)"),
		'f',
		"L3 node$requester_idx remote NOWAIT conflicts with node$master_idx holder");
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_ack_count')
				  > $ack_before;
			},
			10),
		'L3 remote-master REJECT produced a lifecycle ACK');
	$try->quit;

	is($holder->query_safe("SELECT pg_advisory_unlock($chosen_key)"),
		't', 'L3 master-side holder released after the REJECT');
	$holder->quit;
	ok(wait_lifecycle_empty(),
		'L3 remote REJECT receiver entry + requester journal converge to zero');
}

my $full_after =
  ges_int($n0, 'ges_dedup_full_reject_count')
  + ges_int($n1, 'ges_dedup_full_reject_count')
  + ges_int($n0, 'ges_dedup_journal_full_count')
  + ges_int($n1, 'ges_dedup_journal_full_count');
is($full_after, $full_before,
	'L4 normal GRANT/REJECT caused no receiver or requester capacity rejection');

# L5: freeze the master LMON, stage a remote request under the old CONTROL
# capability generation, let the requester detect the broken link, then resume
# the peer.  Every request/retransmit reserved by this backend remains bound to
# the old generation and therefore must never execute on the replacement link.
# The query fails closed, while the reliable lifecycle retry (which is allowed
# to rebind the DONE frame to the current generation) drains both tables.
SKIP:
{
	skip 'no remote-master direction found', 7 unless $remote_grant_seen;

	$stopped_lmon_pid = int($master->safe_psql(
		'postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon'}));
	ok($stopped_lmon_pid > 0,
		"L5 located node$master_idx LMON pid $stopped_lmon_pid");

	my $reconnect_before = int($requester->safe_psql(
		'postgres',
		"SELECT reconnect_count FROM pg_cluster_ic_peers "
		  . "WHERE node_id = $master_idx"));
	kill 'STOP', $stopped_lmon_pid
	  or die "L5 SIGSTOP $stopped_lmon_pid failed: $!";

	my $old_key = $chosen_key + 1000;
	my $old = $requester->background_psql(
		'postgres', on_error_stop => 0, timeout => 40);
	$old->query_until(qr/PGRAC_OLD_FIRED/,
		"SET cluster.ges_request_timeout_ms = '8s';\n"
		  . "\\echo PGRAC_OLD_FIRED\n"
		  . "SELECT pg_advisory_lock($old_key);\n"
		  . "SELECT pg_advisory_unlock($old_key);\n"
		  . "\\echo PGRAC_OLD_DONE\n");
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_count') > 0;
			},
			5),
		'L5 old-generation remote request reserved a requester journal row');
	ok(poll_until(
			sub {
				int($requester->safe_psql(
						'postgres',
						"SELECT reconnect_count FROM pg_cluster_ic_peers "
						  . "WHERE node_id = $master_idx"))
				  > $reconnect_before;
			},
			8),
		'L5 requester detected the frozen peer and advanced reconnect_count');

	kill 'CONT', $stopped_lmon_pid
	  or die "L5 SIGCONT $stopped_lmon_pid failed: $!";
	$stopped_lmon_pid = 0;
	ok($pair->wait_for_peer_state($requester_idx, $master_idx, 'connected', 30),
		'L5 requester reconnected to the resumed master');
	ok($pair->wait_for_peer_state($master_idx, $requester_idx, 'connected', 30),
		'L5 resumed master reconnected to the requester');

	my $old_tail = $old->query_until(qr/PGRAC_OLD_DONE/, "");
	like($old_tail, qr/(?:^|\n)f(?:\n|$)/,
		'L5 old-generation request never executed on the replacement link');
	eval { $old->quit; };
	ok(wait_lifecycle_empty(),
		'L5 reconnect lifecycle retry drains receiver entry and requester journal');
}

# L6: SIGKILL a requester backend while its remote request is in flight.
# PostgreSQL treats an unexpected backend SIGKILL as a shared-memory crash and
# restarts every child under the same postmaster.  The fresh ProcArray begins
# allocating backend slots again, so the first post-recovery client exercises
# the procno-reuse boundary.  The peer must discard the old receiver row, and
# the fresh request (with a new boot/generation/request id) must not be covered
# by the old completion frontier.
SKIP:
{
	skip 'no remote-master direction found', 13 unless $remote_grant_seen;

	# Reuse the key whose master direction L2 proved.  Advisory resource
	# hashing is key-dependent, so adding an arbitrary offset would no longer
	# guarantee that $master is the actual GES master.
	my $kill_key = $chosen_key;
	my $holder = $master->background_psql('postgres', on_error_die => 1);
	$holder->query_safe("SELECT pg_advisory_lock($kill_key)");

	my $crashed = $requester->background_psql(
		'postgres', on_error_stop => 0, timeout => 60);
	my $old_backend_pid = int($crashed->query_safe('SELECT pg_backend_pid()'));
	ok($old_backend_pid > 0,
		"L6 captured requester backend pid $old_backend_pid before SIGKILL");
	$crashed->query_until(qr/PGRAC_KILL_FIRED/,
		"SET cluster.ges_request_timeout_ms = '30s';\n"
		  . "\\echo PGRAC_KILL_FIRED\n"
		  . "SELECT pg_advisory_lock($kill_key);\n"
		  . "\\echo PGRAC_KILL_UNEXPECTED_RETURN\n");

	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_count') > 0;
			},
			5),
		'L6 doomed backend reserved a requester journal row');
	ok(poll_until(
			sub {
				ges_int($master, 'ges_dedup_entry_count') > 0;
			},
			5),
		'L6 master registered the doomed backend request in receiver dedup');

	kill 'KILL', $old_backend_pid
	  or die "L6 SIGKILL backend $old_backend_pid failed: $!";
	ok(1, 'L6 requester backend SIGKILL delivered');

	ok(poll_until(
			sub {
				my ($rc, $out) = $requester->psql('postgres', 'SELECT 1');
				defined($rc) && $rc == 0 && defined($out) && $out eq '1';
			},
			45),
		'L6 requester postmaster completed crash recovery and accepts SQL');
	ok($pair->wait_for_peer_state($requester_idx, $master_idx, 'connected', 60),
		'L6 recovered requester reconnected to the master');
	ok($pair->wait_for_peer_state($master_idx, $requester_idx, 'connected', 60),
		'L6 master reconnected to the recovered requester');

	eval { $crashed->quit; };

	my $ack_before =
	  ges_int($requester, 'ges_dedup_journal_ack_count');
	my $fresh = $requester->background_psql('postgres', on_error_die => 1);
	my $new_backend_pid = int($fresh->query_safe('SELECT pg_backend_pid()'));
	isnt($new_backend_pid, $old_backend_pid,
		'L6 post-recovery backend has a fresh process identity');
	is($fresh->query_safe("SELECT pg_try_advisory_lock($kill_key)"),
		'f',
		'L6 fresh backend request is processed (not stale-frontier suppressed)');
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_ack_count')
				  > $ack_before;
			},
			10),
		'L6 fresh post-recovery request receives its own lifecycle ACK');
	$fresh->quit;

	is($holder->query_safe("SELECT pg_advisory_unlock($kill_key)"),
		't', 'L6 master holder survived requester crash and releases cleanly');
	$holder->quit;
	is($requester->safe_psql(
			'postgres', "SELECT pg_try_advisory_lock($kill_key)"),
		't',
		'L6 superseded-boot waiter cannot become an orphan holder after release');
	ok(wait_lifecycle_empty(),
		'L6 SIGKILL/reuse leaves receiver dedup and requester journal empty');
}

# L7: cancel a blocking remote REQUEST.  The interrupt boundary must remove
# the exact master-side waiter, terminalize its completion intent, and leave no
# dedup row or journal row behind.
SKIP:
{
	skip 'no remote-master direction found', 9 unless $remote_grant_seen;

	my $holder = $master->background_psql('postgres', on_error_die => 1);
	$holder->query_safe("SELECT pg_advisory_lock($chosen_key)");
	my $waiter = $requester->background_psql(
		'postgres', on_error_stop => 0, timeout => 45);
	my $waiter_pid = int($waiter->query_safe('SELECT pg_backend_pid()'));
	ok($waiter_pid > 0, "L7 captured REQUEST waiter pid $waiter_pid");
	my $ack_before =
	  ges_int($requester, 'ges_dedup_journal_ack_count');
	$waiter->query_until(qr/PGRAC_REQUEST_FIRED/,
		"\\echo PGRAC_REQUEST_FIRED\n"
		  . "SELECT pg_advisory_lock($chosen_key);\n"
		  . "\\echo PGRAC_REQUEST_DONE\n");
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_count') > 0;
			},
			5),
		'L7 blocking REQUEST reserved a requester journal row');
	ok(poll_until(
			sub {
				ges_int($master, 'ges_dedup_entry_count') > 0;
			},
			5),
		'L7 blocking REQUEST registered a receiver dedup row');
	is($requester->safe_psql(
			'postgres', "SELECT pg_cancel_backend($waiter_pid)"),
		't', 'L7 query cancel delivered to the blocking REQUEST');
	$waiter->query_until(qr/PGRAC_REQUEST_DONE/, "");
	pass('L7 canceled REQUEST returned control to its session');
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_ack_count')
				  > $ack_before;
			},
			10),
		'L7 canceled REQUEST completion receives a lifecycle ACK');
	ok(wait_lifecycle_empty(),
		'L7 canceled REQUEST drains receiver dedup and requester journal');
	eval { $waiter->quit; };
	is($holder->query_safe("SELECT pg_advisory_unlock($chosen_key)"),
		't', 'L7 master holder releases after waiter cancellation');
	$holder->quit;
	is($requester->safe_psql(
			'postgres', "SELECT pg_try_advisory_lock($chosen_key)"),
		't', 'L7 canceled waiter left no orphan holder or queue entry');
}

# L8: cancel while a remote RELEASE is waiting for its ACK.  Freezing the
# master LMON makes this normally short window deterministic without changing
# product behavior.  The catch path must stage a dedup-bypassed cleanup RELEASE
# before publishing DONE; after the peer resumes, the lock is acquirable.
SKIP:
{
	skip 'no remote-master direction found', 11 unless $remote_grant_seen;

	my $releaser = $requester->background_psql(
		'postgres', on_error_stop => 0, timeout => 45);
	$releaser->query_safe("SELECT pg_advisory_lock($chosen_key)");
	ok(wait_lifecycle_empty(),
		'L8 remote holder GRANT lifecycle is empty before RELEASE cancellation');
	my $releaser_pid =
	  int($releaser->query_safe('SELECT pg_backend_pid()'));
	ok($releaser_pid > 0, "L8 captured RELEASE backend pid $releaser_pid");
	$stopped_lmon_pid = int($master->safe_psql(
		'postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon'}));
	ok($stopped_lmon_pid > 0,
		"L8 located node$master_idx LMON pid $stopped_lmon_pid");
	my $ack_before =
	  ges_int($requester, 'ges_dedup_journal_ack_count');
	kill 'STOP', $stopped_lmon_pid
	  or die "L8 SIGSTOP $stopped_lmon_pid failed: $!";
	$releaser->query_until(qr/PGRAC_RELEASE_FIRED/,
		"\\echo PGRAC_RELEASE_FIRED\n"
		  . "SELECT pg_advisory_unlock($chosen_key);\n"
		  . "\\echo PGRAC_RELEASE_DONE\n");
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_count') > 0;
			},
			5),
		'L8 remote RELEASE reserved a journal row while ACK is frozen');
	is($requester->safe_psql(
			'postgres', "SELECT pg_cancel_backend($releaser_pid)"),
		't', 'L8 query cancel delivered inside RELEASE ACK wait');
	kill 'CONT', $stopped_lmon_pid
	  or die "L8 SIGCONT $stopped_lmon_pid failed: $!";
	$stopped_lmon_pid = 0;
	ok($pair->wait_for_peer_state($requester_idx, $master_idx, 'connected', 30),
		'L8 requester remains/reconnects to resumed master');
	ok($pair->wait_for_peer_state($master_idx, $requester_idx, 'connected', 30),
		'L8 resumed master remains/reconnects to requester');
	$releaser->query_until(qr/PGRAC_RELEASE_DONE/, "");
	pass('L8 canceled RELEASE returned control to its session');
	eval { $releaser->quit; };
	ok(poll_until(
			sub {
				ges_int($requester, 'ges_dedup_journal_ack_count')
				  > $ack_before;
			},
			10),
		'L8 canceled RELEASE completion receives a lifecycle ACK');
	ok(wait_lifecycle_empty(),
		'L8 cleanup RELEASE + DONE drain both lifecycle tables');
	is($requester->safe_psql(
			'postgres', "SELECT pg_try_advisory_lock($chosen_key)"),
		't', 'L8 cleanup RELEASE removed the authoritative remote holder');
}

if ($ENV{PGRAC_T410_L1_L8_ONLY})
{
	$pair->stop_pair;
	done_testing();
	exit 0;
}

# L9: live CONVERT cancellation and rollback.  The bootstrap phase above
# already proved the ordinary relation's exact remote-master direction and
# armed one assertion-build native-probe CLEAR on that master.  The seam lets
# the real remote CONVERT reach the GRD conflict queue even though the peer's
# native SHARE lock would normally stop it at the conservative probe layer.
my $blocker = $tm_master->background_psql('postgres', on_error_die => 1);
$blocker->query_safe('BEGIN');
$blocker->query_safe(
	"LOCK TABLE $convert_relation IN SHARE MODE");

my $converter = $tm_requester->background_psql(
	'postgres', on_error_stop => 0, timeout => 60);
$converter->query_safe('BEGIN');
$converter->query_safe(
	"LOCK TABLE $convert_relation IN SHARE MODE");
$converter->query_safe('SAVEPOINT l9_convert_upgrade');
my $converter_pid =
  int($converter->query_safe('SELECT pg_backend_pid()'));
ok($converter_pid > 0,
	"L9 captured remote CONVERT backend pid $converter_pid");
my $enqueued_before =
  state_int_both('grd', 'grd_convert_enqueued_count');
my $ack_before =
  ges_int($tm_requester, 'ges_dedup_journal_ack_count');
$converter->query_until(qr/PGRAC_CONVERT_FIRED/,
	"\\echo PGRAC_CONVERT_FIRED\n"
	  . "LOCK TABLE $convert_relation IN ACCESS EXCLUSIVE MODE;\n");
ok(wait_backend_event(
		$tm_requester, $converter_pid, 'GesConvertWait', 15),
	'L9 remote upgrade blocks in the real GesConvertWait path');
ok(poll_until(
		sub {
			ges_int($tm_requester, 'ges_dedup_journal_count') > 0;
		},
		5),
	'L9 remote CONVERT reserved a requester journal row');
ok(poll_until(
		sub {
			ges_int($tm_master, 'ges_dedup_entry_count') > 0;
		},
		5),
	'L9 remote CONVERT registered a receiver dedup row');
ok(poll_until(
		sub {
			state_int_both('grd', 'grd_convert_enqueued_count')
			  > $enqueued_before;
		},
		10),
	'L9 master enqueued the actual CONVERT before cancellation');
is($tm_requester->safe_psql(
		'postgres', "SELECT pg_cancel_backend($converter_pid)"),
	't', 'L9 query cancel delivered to GesConvertWait');
ok(poll_until(
		sub {
			my $state = $tm_requester->safe_psql(
				'postgres', qq{
				SELECT coalesce(state, '') FROM pg_stat_activity
				WHERE pid = $converter_pid});
			defined($state) && $state eq 'idle in transaction (aborted)';
		},
		10),
	'L9 canceled CONVERT returned with its transaction still open');
ok(poll_until(
		sub {
			ges_int($tm_requester, 'ges_dedup_journal_ack_count')
			  > $ack_before;
		},
		10),
	'L9 canceled CONVERT completion receives a lifecycle ACK');
ok(wait_lifecycle_empty(),
	'L9 canceled CONVERT drains both lifecycle tables before transaction end');

$converter->query_until(qr/PGRAC_SAVEPOINT_ROLLED_BACK/,
	"ROLLBACK TO SAVEPOINT l9_convert_upgrade;\n"
	  . "\\echo PGRAC_SAVEPOINT_ROLLED_BACK\n");
pass('L9 canceled upgrade rolled back to its savepoint');
my $both_share_holders_retained = poll_until(
	sub {
		relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'nconverts') == 0
		  && relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'ngranted') == 2;
	},
	10);
diag('L9 master GRD after savepoint rollback: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid))
  unless $both_share_holders_retained;
ok($both_share_holders_retained,
	'L9 savepoint rollback removes the CONVERT and retains both SHARE holders');

$blocker->query_safe('COMMIT');
$blocker->quit;
my $old_holder_retained = poll_until(
		sub {
			relation_grd_int(
				$tm_master, $convert_database_oid,
				$convert_relation_oid, 'nconverts') == 0
			  && relation_grd_int(
				$tm_master, $convert_database_oid,
				$convert_relation_oid, 'ngranted') == 1;
		},
		10);
diag('L9 master GRD after blocker COMMIT: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid))
  unless $old_holder_retained;
ok($old_holder_retained,
	'L9 CONVERT_ROLLBACK removes the queue entry but retains old SHARE holder');

$converter->query_until(qr/PGRAC_CONVERT_DONE/,
	"ROLLBACK;\n\\echo PGRAC_CONVERT_DONE\n");
pass('L9 canceled CONVERT transaction rolled back and returned control');
eval { $converter->quit; };
ok(wait_lifecycle_empty(),
	'L9 explicit rollback leaves both lifecycle tables empty');
my $parent_share_released = poll_until(
	sub {
		relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'nconverts') == 0
		  && relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'ngranted') == 0;
	},
	10);
diag('L9 master GRD after parent transaction rollback: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid))
  unless $parent_share_released;
ok($parent_share_released,
	'L9 parent rollback releases the retained SHARE holder');
is( int($n0->safe_psql(
			'postgres',
			q{SELECT coalesce(sum(nconverts), 0) FROM pg_cluster_grd_entries}))
	  + int($n1->safe_psql(
			'postgres',
			q{SELECT coalesce(sum(nconverts), 0) FROM pg_cluster_grd_entries})),
	0, 'L9 no queued CONVERT survives cancellation');
my ($fresh_rc, $fresh_out, $fresh_err) = $tm_requester->psql(
	'postgres', qq{
	BEGIN;
	LOCK TABLE $convert_relation IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
is($fresh_rc, 0,
	'L9 CONVERT_ROLLBACK leaves the relation fully acquirable')
  or diag("L9 fresh AccessExclusive err=$fresh_err");

# L10: force the opposite cancellation race on the same immutable product
# binary.  Restarting normally re-arms the exact assertion-build native-probe
# one-shot.  Once the CONVERT is queued, SIGSTOP only its backend (LMON remains
# live), release the blocker, and wait until the master has granted the
# CONVERT and the requester LMON has delivered the GRANT into shared reply-wait
# state.  ges_dispatch_grant_identity records the compile-time-locked 52-byte
# GesReplyPayload before that wire reply can be observed on the requester, so
# the simultaneous receiver row proves the cached GRANT is present.
#
# A pending SIGINT is then delivered when the backend resumes.  Its exact
# CANCEL_WAIT must lose with NOT_FOUND, must not overwrite the cached 52-byte
# GRANT with terminal CACHED0, and CONVERT_ROLLBACK must restore the parent
# transaction's old SHARE holder.  The master-side stale-rejected counter is
# the direct NOT_FOUND witness.
$pair->stop_pair;
$pair->start_pair;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L10 node0 sees node1 connected after exact-seam rearm');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L10 node1 sees node0 connected after exact-seam rearm');
ok(wait_lifecycle_empty(),
	'L10 restarted pair begins with empty lifecycle state');

my $race_blocker =
  $tm_master->background_psql('postgres', on_error_die => 1);
$race_blocker->query_safe('BEGIN');
$race_blocker->query_safe(
	"LOCK TABLE $convert_relation IN SHARE MODE");

my $race_converter = $tm_requester->background_psql(
	'postgres', on_error_stop => 0, timeout => 60);
$race_converter->query_safe('BEGIN');
$race_converter->query_safe(
	"LOCK TABLE $convert_relation IN SHARE MODE");
$race_converter->query_safe('SAVEPOINT l10_grant_wins');
ok(wait_lifecycle_empty(),
	'L10 parent SHARE grants completed before the race');
my $race_converter_pid =
  int($race_converter->query_safe('SELECT pg_backend_pid()'));
ok($race_converter_pid > 0,
	"L10 captured grant-wins CONVERT backend pid $race_converter_pid");

my $race_enqueued_before =
  state_int_both('grd', 'grd_convert_enqueued_count');
my $race_reply_before =
  ges_int($tm_requester, 'ges_reply_defer_count');
my $race_ack_before =
  ges_int($tm_requester, 'ges_dedup_journal_ack_count');
my $race_cancel_not_found_before = int($tm_master->safe_psql(
	'postgres', q{
	SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
	WHERE category = 'lmd'
	  AND key = 'cancel_wait_stale_rejected_count'}));
$race_converter->query_until(qr/PGRAC_GRANT_WINS_FIRED/,
	"\\echo PGRAC_GRANT_WINS_FIRED\n"
	  . "LOCK TABLE $convert_relation IN ACCESS EXCLUSIVE MODE;\n");
ok(wait_backend_event(
		$tm_requester, $race_converter_pid, 'GesConvertWait', 15),
	'L10 CONVERT reached GesConvertWait before the forced race');
ok(poll_until(
		sub {
			state_int_both('grd', 'grd_convert_enqueued_count')
			  > $race_enqueued_before;
		},
		10),
	'L10 master enqueued the CONVERT before grant-wins');

$stopped_backend_pid = $race_converter_pid;
ok(kill('STOP', $stopped_backend_pid) == 1,
	'L10 stopped only the converter backend while LMON stayed live');
$race_blocker->query_safe('COMMIT');
$race_blocker->quit;

my $cached_grant_present = poll_until(
	sub {
		relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'nconverts') == 0
		  && relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'ngranted') == 1
		  && ges_int($tm_requester, 'ges_reply_defer_count')
			   > $race_reply_before
		  && ges_int($tm_master, 'ges_dedup_entry_count') > 0;
	},
	10);
diag('L10 grant-wins master GRD/cache: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid)
	  . ',dedup=' . ges_int($tm_master, 'ges_dedup_entry_count')
	  . ',reply_delta='
	  . (ges_int($tm_requester, 'ges_reply_defer_count')
		 - $race_reply_before))
  unless $cached_grant_present;
ok($cached_grant_present,
	'L10 master granted the CONVERT and retained its 52-byte cached GRANT');

is($tm_requester->safe_psql(
		'postgres', "SELECT pg_cancel_backend($race_converter_pid)"),
	't', 'L10 queued query cancel after the master GRANT won');
ok(kill('CONT', $stopped_backend_pid) == 1,
	'L10 resumed the converter backend with SIGINT pending');
$stopped_backend_pid = 0;
ok(poll_until(
		sub {
			my $state = $tm_requester->safe_psql(
				'postgres', qq{
				SELECT coalesce(state, '') FROM pg_stat_activity
				WHERE pid = $race_converter_pid});
			defined($state) && $state eq 'idle in transaction (aborted)';
		},
		10),
	'L10 pending cancel interrupted the already-delivered CONVERT GRANT');
ok(poll_until(
		sub {
			int($tm_master->safe_psql(
				'postgres', q{
				SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
				WHERE category = 'lmd'
				  AND key = 'cancel_wait_stale_rejected_count'}))
			  > $race_cancel_not_found_before;
		},
		10),
	'L10 wait-seq exact CANCEL_WAIT observed NOT_FOUND after grant-wins');
ok(poll_until(
		sub {
			ges_int($tm_requester, 'ges_dedup_journal_ack_count')
			  > $race_ack_before;
		},
		10),
	'L10 grant-wins cancellation receives its lifecycle ACK');
ok(wait_lifecycle_empty(),
	'L10 NOT_FOUND leaves no receiver cache or requester journal row');

$race_converter->query_until(qr/PGRAC_GRANT_WINS_SAVEPOINT_DONE/,
	"ROLLBACK TO SAVEPOINT l10_grant_wins;\n"
	  . "\\echo PGRAC_GRANT_WINS_SAVEPOINT_DONE\n");
my $race_parent_share_retained = poll_until(
	sub {
		relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'nconverts') == 0
		  && relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'ngranted') == 1;
	},
	10);
diag('L10 GRD after grant-wins savepoint rollback: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid))
  unless $race_parent_share_retained;
ok($race_parent_share_retained,
	'L10 NOT_FOUND did not erase the real parent SHARE holder');

$race_converter->query_until(qr/PGRAC_GRANT_WINS_DONE/,
	"ROLLBACK;\n\\echo PGRAC_GRANT_WINS_DONE\n");
eval { $race_converter->quit; };
my $race_final_empty = poll_until(
	sub {
		relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'nconverts') == 0
		  && relation_grd_int(
			$tm_master, $convert_database_oid,
			$convert_relation_oid, 'ngranted') == 0;
	},
	10);
diag('L10 final master GRD: '
	  . relation_grd_snapshot(
		  $tm_master, $convert_database_oid, $convert_relation_oid))
  unless $race_final_empty;
ok($race_final_empty,
	'L10 real parent release clears the final holder and convert queue');
ok(wait_lifecycle_empty(),
	'L10 final release leaves lifecycle state empty');

my ($race_fresh_rc, $race_fresh_out, $race_fresh_err) =
  $tm_requester->psql(
	'postgres', qq{
	BEGIN;
	LOCK TABLE $convert_relation IN ACCESS EXCLUSIVE MODE;
	COMMIT;});
is($race_fresh_rc, 0,
	'L10 grant-wins cleanup leaves the relation fully acquirable')
  or diag("L10 fresh AccessExclusive err=$race_fresh_err");

$pair->stop_pair;
done_testing();
