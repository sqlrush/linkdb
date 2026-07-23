#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 409_cluster_current_mx_capacity.pl
#	  Legal 256-member current-MultiXact positive and 257th-member limit.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/409_cluster_current_mx_capacity.pl
#
# NOTES
#	  pgrac-original file.
#	  Spec: spec-3.6b-multixact-current-dml.md
#
#	  This is a real transaction-lifecycle fixture, not wire injection.
#	  Each node keeps 128 compatible FOR SHARE transactions ACTIVE.  A pinned
#	  REPEATABLE READ snapshot plus one committed filler per current TT
#	  segment creates legitimate retention pressure, allowing the fixed
#	  48-slot allocator to roll over while every old ACTIVE member remains
#	  authority-provable.  The combined row reaches 256 real members; the
#	  257th real locker must hit the documented protocol limit exactly.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use File::Temp qw(tempdir);
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use IPC::Run;

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);


sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		     FROM pg_cluster_state
		    WHERE category = '$category' AND key = '$key'});
	return int($value // 0);
}


sub wait_for
{
	my ($predicate, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		my $matched = eval { $predicate->() };
		return 1 if $matched;
		usleep(200_000);
	}
	my $matched = eval { $predicate->() };
	return $matched ? 1 : 0;
}


sub write_retry
{
	my ($node, $sql) = @_;
	for (1 .. 10)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	return 0;
}


sub mirrored_coincident_create
{
	my ($node0, $node1, $name, $ddl) = @_;

	for (1 .. 8)
	{
		return 0 unless write_retry($node0, $ddl);
		return 0 unless write_retry($node1, $ddl);
		my $p0 = $node0->safe_psql(
			'postgres', "SELECT pg_relation_filepath('$name')");
		my $p1 = $node1->safe_psql(
			'postgres', "SELECT pg_relation_filepath('$name')");
		return 1 if $p0 eq $p1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn)
			= $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		return 0
		  unless write_retry(
			$lag,
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'current_mx_capacity',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'max_connections = 150',
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 60000',
		'cluster.gcs_reply_timeout_ms = 60000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 20',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.tt_status_overlay_ttl_ms = 600000',
		'cluster.undo_segments_max_per_instance = 256',
	]);
$pair->start_pair;
usleep(2_000_000);
my ($node0, $node1) = ($pair->node0, $pair->node1);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 peers connected');
ok(wait_for(
		sub {
			my $f0 = $node0->safe_psql(
				'postgres',
				q{SELECT coalesce(max(value::bigint), 0)
				    FROM cluster_dump_state()
				   WHERE key = 'mxid_stripe_activated_floor'});
			my $f1 = $node1->safe_psql(
				'postgres',
				q{SELECT coalesce(max(value::bigint), 0)
				    FROM cluster_dump_state()
				   WHERE key = 'mxid_stripe_activated_floor'});
			return $f0 > 0 && $f1 > 0 && $f0 == $f1;
		},
		60),
	'L1 common MultiXact activation floor is ready')
  or BAIL_OUT('mxid activation floor did not converge');

ok(write_retry($node0, 'CREATE EXTENSION IF NOT EXISTS pageinspect')
	  && write_retry($node1, 'CREATE EXTENSION IF NOT EXISTS pageinspect'),
	'L1 pageinspect fixture support is writable on both catalogs')
  or BAIL_OUT('could not install pageinspect');
ok(mirrored_coincident_create(
		$node0, $node1, 'cmxcap_t',
		'CREATE TABLE cmxcap_t (id int, v int)'),
	'L2 target relation identity coincides')
  or BAIL_OUT('could not create target coincident relation');
ok(mirrored_coincident_create(
		$node0, $node1, 'cmxcap_fill',
		'CREATE TABLE cmxcap_fill (node_id int, seq int)'),
	'L2 filler relation identity coincides')
  or BAIL_OUT('could not create filler coincident relation');
$node0->safe_psql('postgres', 'INSERT INTO cmxcap_t VALUES (1, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');

# Pin each node's local retention horizon below every filler commit.
my $pin0 = $node0->background_psql('postgres', on_error_die => 1);
my $pin1 = $node1->background_psql('postgres', on_error_die => 1);
for my $pin ($pin0, $pin1)
{
	$pin->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$pin->query_safe('SELECT count(*) FROM pg_class');
}

my @filler_seq = (0, 0);
my @benches;
my $single_b;
my $bench_dir = tempdir(CLEANUP => 1);
my $bench_script = "$bench_dir/hold.sql";
{
	open(my $fh, '>', $bench_script)
	  or die "cannot write $bench_script: $!";
	print $fh <<'SQL';
SELECT pg_sleep(:client_id * 0.5);
BEGIN;
SELECT v FROM cmxcap_t WHERE id = 1 FOR SHARE;
SELECT 1 /* cmxcap_ready */;
SELECT pg_sleep(1800);
COMMIT;
SQL
	close($fh);
}

sub retained_filler
{
	my ($node, $node_id) = @_;
	$filler_seq[$node_id]++;
	$node->safe_psql(
		'postgres',
		"INSERT INTO cmxcap_fill VALUES ($node_id, $filler_seq[$node_id])");
}

sub start_locker_batch
{
	my ($node, $app, $clients) = @_;
	my @cmd = (
		$node->installed_command('pgbench'),
		'-n', '-c', $clients, '-j', 4, '-t', 1,
		'-f', $bench_script,
		'-h', $node->host, '-p', $node->port, 'postgres');
	my ($out, $err) = ('', '');
	my $handle;

	{
		local $ENV{PGAPPNAME} = $app;
		$handle = IPC::Run::start(
			\@cmd, '>', \$out, '2>', \$err);
	}
	push @benches, {
		handle => $handle,
		out => \$out,
		err => \$err,
		app => $app,
		clients => $clients,
	};

	return wait_for(
		sub {
			return $node->safe_psql(
				'postgres',
				qq{SELECT count(*)
				     FROM pg_stat_activity
				    WHERE application_name = '$app'
				      AND state = 'active'
				      AND wait_event = 'PgSleep'
				      AND query LIKE 'SELECT pg_sleep(1800)%'})
			  == $clients;
		},
		120);
}

sub stop_locker_batches
{
	for my $bench (reverse @benches)
	{
		eval { $bench->{handle}->kill_kill };
		eval { $bench->{handle}->finish };
		diag(${ $bench->{err} })
		  if ${ $bench->{err} } ne ''
		  && ${ $bench->{err} } !~ /connection to server was lost/;
	}
	@benches = ();
}

sub start_retry_locker
{
	my ($node) = @_;
	my $last_error = '';

	for (1 .. 10)
	{
		my $locker = $node->background_psql(
			'postgres', on_error_die => 1, timeout => 120);
		$locker->set_query_timer_restart();
		my $ok = eval {
			$locker->query_safe('BEGIN');
			$locker->query_safe(
				'SELECT v FROM cmxcap_t WHERE id = 1 FOR SHARE');
			1;
		};
		return $locker if $ok;

		$last_error = $@;
		eval { $locker->query_safe('ROLLBACK') };
		eval { $locker->quit };
		usleep(500_000);
	}
	diag("persistent locker retries exhausted: $last_error");
	return undef;
}

END
{
	stop_locker_batches();
	if (defined($single_b))
	{
		eval { $single_b->query_safe('ROLLBACK') };
		eval { $single_b->quit };
		$single_b = undef;
	}
}

my $roll0_before
	= state_int($node0, 'undo', 'tt_retention_rollover_count');
my $single_a = $node0->background_psql(
	'postgres', on_error_die => 1, timeout => 120);
$single_a->set_query_timer_restart();
$single_a->query_safe('BEGIN');
$single_a->query_safe(
	'SELECT v FROM cmxcap_t WHERE id = 1 FOR SHARE');
ok(start_locker_batch($node0, 'cmxcap_n0_b1', 46),
	'L3 node0 first segment reaches 47 ACTIVE lockers')
  or BAIL_OUT('node0 first locker batch did not become ready');
retained_filler($node0, 0);
ok(start_locker_batch($node0, 'cmxcap_n0_b2', 47),
	'L3 node0 second 47-locker batch is ACTIVE')
  or BAIL_OUT('node0 second locker batch did not become ready');
retained_filler($node0, 0);
ok(start_locker_batch($node0, 'cmxcap_n0_b3', 34),
	'L3 node0 final 34-locker batch is ACTIVE')
  or BAIL_OUT('node0 final locker batch did not become ready');

# Add the replacement before retiring A so active membership stays at 128.
# A is the only page ITL LOCK_ONLY_ACTIVE slot; retiring it removes the
# spec-5.2 page-X transfer boundary before node1 enters.
my $replacement = $node0->background_psql(
	'postgres', on_error_die => 1, timeout => 120);
$replacement->set_query_timer_restart();
$replacement->query_safe('BEGIN');
$replacement->query_safe(
	'SELECT v FROM cmxcap_t WHERE id = 1 FOR SHARE');
$single_a->query_safe('COMMIT');
$single_a->quit;
cmp_ok(
	state_int($node0, 'undo', 'tt_retention_rollover_count')
	  - $roll0_before,
	'>=', 2,
	'L3 node0 legally retains 128 ACTIVE lockers across TT rollovers');

my $roll1_before
	= state_int($node1, 'undo', 'tt_retention_rollover_count');
$single_b = start_retry_locker($node1);
ok(defined($single_b),
	'L3 node1 first persistent locker survives transient remaster')
  or BAIL_OUT('node1 persistent locker did not become ready');
ok(start_locker_batch($node1, 'cmxcap_n1_b1', 46),
	'L3 node1 first segment reaches 47 ACTIVE lockers')
  or BAIL_OUT('node1 first locker batch did not become ready');
retained_filler($node1, 1);
ok(start_locker_batch($node1, 'cmxcap_n1_b2', 47),
	'L3 node1 second 47-locker batch is ACTIVE')
  or BAIL_OUT('node1 second locker batch did not become ready');
retained_filler($node1, 1);
ok(start_locker_batch($node1, 'cmxcap_n1_b3', 34),
	'L3 node1 final 34-locker batch is ACTIVE')
  or BAIL_OUT('node1 final locker batch did not become ready');
cmp_ok(
	state_int($node1, 'undo', 'tt_retention_rollover_count')
	  - $roll1_before,
	'>=', 2,
	'L3 node1 legally retains 128 ACTIVE lockers across TT rollovers');

is($node0->safe_psql(
		'postgres',
		q{SELECT count(*)
		    FROM pg_stat_activity
		   WHERE application_name LIKE 'cmxcap_n0_%'
		     AND state = 'active'
		     AND wait_event = 'PgSleep'
		     AND query LIKE 'SELECT pg_sleep(1800)%'}),
	'127',
	'L4 node0 holds 127 pgbench lockers plus one replacement');
is($node1->safe_psql(
		'postgres',
		q{SELECT count(*)
		    FROM pg_stat_activity
		   WHERE application_name LIKE 'cmxcap_n1_%'
		     AND state = 'active'
		     AND wait_event = 'PgSleep'
		     AND query LIKE 'SELECT pg_sleep(1800)%'}),
	'127',
	'L4 node1 holds 127 pgbench lockers plus one persistent locker');

my $raw_mx = $node1->safe_psql(
	'postgres',
	q{SELECT t_xmax::text
	    FROM heap_page_items(get_raw_page('cmxcap_t', 0))
	   WHERE lp_flags = 1
	   ORDER BY lp
	   LIMIT 1});
like($raw_mx, qr/^\d+$/, 'L4 tuple exposes a real raw MultiXactId');
is($raw_mx % 16, 1,
	'L4 value-derived MXID origin is node1, the final requester');
is($node1->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_get_multixact_members('$raw_mx'::xid)"),
	'256',
	'L4 origin SLRU contains all 256 real ACTIVE members');

my $limit_before
	= state_int($node0, 'multixact_current',
	'recompose_failclosed_count');
my $describe_before
	= state_int($node0, 'multixact_current', 'describe_remote_ask_count');

$node0->safe_psql(
	'postgres',
	q{
CREATE OR REPLACE FUNCTION cmxcap_try_257()
RETURNS TABLE(state text, message text, detail text, hint text)
LANGUAGE plpgsql
AS $func$
DECLARE
  caught_state text;
  caught_message text;
  caught_detail text;
  caught_hint text;
BEGIN
  BEGIN
    PERFORM v FROM cmxcap_t WHERE id = 1 FOR SHARE;
    RETURN QUERY SELECT '00000', 'no error', '', '';
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      caught_state = RETURNED_SQLSTATE,
      caught_message = MESSAGE_TEXT,
      caught_detail = PG_EXCEPTION_DETAIL,
      caught_hint = PG_EXCEPTION_HINT;
    RETURN QUERY
      SELECT caught_state, caught_message, caught_detail, caught_hint;
  END;
END
$func$});

my ($state, $message, $detail, $hint);
for (1 .. 10)
{
	my $limit_record = $node0->safe_psql(
		'postgres',
		q{SELECT state || chr(30) || message || chr(30) ||
		          detail || chr(30) || hint
		    FROM cmxcap_try_257()});
	($state, $message, $detail, $hint)
		= split(/\x1e/, $limit_record, -1);
	last if $state ne '53R9I';
	usleep(500_000);
}
is($state, '0A000', 'L5 257th locker returns SQLSTATE 0A000');
is($message,
	'cross-node current-DML does not support MultiXact with more than 256 members',
	'L5 257th locker returns exact supported-limit message');
is($detail,
	"MultiXact $raw_mx has 257 members; PostgreSQL permits larger member sets, but this pgrac protocol version supports at most 256.",
	'L5 257th locker returns exact supported-limit detail');
is($hint,
	'Reduce concurrent row lockers or retry after lockers finish; upgrade when chunked member-list support is available.',
	'L5 257th locker returns exact supported-limit hint');
cmp_ok(
	state_int(
		$node0, 'multixact_current',
		'recompose_failclosed_count'),
	'>', $limit_before,
	'L5 257th real locker increments the supported-limit counter');
cmp_ok(
	state_int(
		$node0, 'multixact_current', 'describe_remote_ask_count'),
	'>', $describe_before,
	'L5 257th locker describes the foreign 256-member authority');

my $raw_after = $node1->safe_psql(
	'postgres',
	q{SELECT t_xmax::text
	    FROM heap_page_items(get_raw_page('cmxcap_t', 0))
	   WHERE lp_flags = 1
	   ORDER BY lp
	   LIMIT 1});
is($raw_after, $raw_mx,
	'L6 rejected 257th locker leaves tuple raw xmax unchanged');
is($node1->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_get_multixact_members('$raw_after'::xid)"),
	'256',
	'L6 rejected 257th locker leaves the authority at exactly 256 members');

eval { $replacement->query_safe('ROLLBACK') };
eval { $replacement->quit };
stop_locker_batches();
eval { $single_b->query_safe('ROLLBACK') };
eval { $single_b->quit };
$single_b = undef;
for my $pin ($pin1, $pin0)
{
	eval { $pin->query_safe('ROLLBACK') };
	eval { $pin->quit };
}

$pair->stop_pair;
done_testing();
