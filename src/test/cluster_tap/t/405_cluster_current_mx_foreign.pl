#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 405_cluster_current_mx_foreign.pl
#	  Foreign-origin MultiXact current-DML authority gate.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/405_cluster_current_mx_foreign.pl
#
# NOTES
#	  pgrac-original file.
#	  Spec: spec-3.6b-multixact-current-dml.md
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use Test::More;
use Time::HiRes qw(usleep);


sub crc32c
{
	my ($data) = @_;
	my $crc = 0xFFFFFFFF;

	for my $byte (unpack('C*', $data))
	{
		$crc ^= $byte;
		for (1 .. 8)
		{
			$crc = ($crc >> 1) ^ (($crc & 1) ? 0x82F63B78 : 0);
		}
	}
	return $crc ^ 0xFFFFFFFF;
}


sub format_stage8_voting_file
{
	my ($path, $disk_index) = @_;
	open(my $fh, '+<', $path) or die "open $path: $!";
	binmode($fh);
	truncate($fh, 0) or die "truncate $path: $!";

	for my $node_id (0 .. 3)
	{
		my $slot = "\0" x 512;
		substr($slot, 0, 12, pack('V3', 0x51564F54, 1, $node_id));
		substr($slot, 48, 4, pack('V', $disk_index));
		substr($slot, 508, 4, pack('V', crc32c(substr($slot, 0, 508))));
		seek($fh, $node_id * 512, 0) or die "seek $path: $!";
		print {$fh} $slot or die "write $path: $!";
	}
	truncate($fh, 394240) or die "size $path: $!";
	close($fh) or die "close $path: $!";
}


sub state_int
{
	my ($node, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		   FROM pg_cluster_state
		  WHERE category = 'multixact_current' AND key = '$key'});
	return int($value // 0);
}


sub state_key_exists
{
	my ($node, $key) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT EXISTS (
		     SELECT 1
		       FROM pg_cluster_state
		      WHERE category = 'multixact_current' AND key = '$key')}) eq 't';
}


sub tuple_has_multixact
{
	my ($node, $relation) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(bool_or(
		     'HEAP_XMAX_IS_MULTI' = ANY(f.raw_flags)), false)
		   FROM heap_page_items(get_raw_page('$relation', 0)) AS h
		   CROSS JOIN LATERAL
		     heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f}) eq 't';
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


sub has_current_mx_capability
{
	my ($node, $peer) = @_;
	my $value = $node->safe_psql(
		'postgres',
		q{SELECT value
		    FROM pg_cluster_state
		   WHERE category = 'ic' AND key = 'peer_capabilities'});
	return 0
	  unless defined($value)
	  && $value =~ /\bn$peer:bits=0x([0-9A-Fa-f]+),gen=(\d+),v=1\b/;
	return (hex($1) & 0x00010000) != 0;
}


sub wait_for_any_wait
{
	my ($node, $query_like, $seconds) = @_;
	return wait_for(
		sub {
			my $event = $node->safe_psql(
				'postgres',
				qq{SELECT coalesce(wait_event, '')
				     FROM pg_stat_activity
				    WHERE query LIKE '$query_like'
				      AND pid <> pg_backend_pid()
				      AND state = 'active'
				    LIMIT 1});
			return $event eq 'GcsMultixactDescribeWait'
			  || $event eq 'GcsMultixactMemberProofWait'
			  || $event eq 'GesTxEnqueueWait';
		},
		$seconds);
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
		my $p0 = $node0->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		my $p1 = $node1->safe_psql('postgres', "SELECT pg_relation_filepath('$name')");
		return 1 if $p0 eq $p1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		return 0
		  unless write_retry($lag,
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}


sub start_blocking
{
	my ($handle, $sql) = @_;
	$handle->query_until(qr/CURRENT_MX_FIRED/, "\\echo CURRENT_MX_FIRED\n$sql;\n");
}


my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'current_mx_foreign',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 5000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
	]);
my @vote_paths = $quad->voting_disk_paths;
for my $disk_index (0 .. $#vote_paths)
{
	format_stage8_voting_file($vote_paths[$disk_index], $disk_index);
}
$quad->start_quad;
usleep(3_000_000);
my ($node0, $node1) = ($quad->node0, $quad->node1);

my $full_mesh = 1;
for my $from (0 .. 3)
{
	for my $to (0 .. 3)
	{
		next if $from == $to;
		$full_mesh &&= $quad->wait_for_peer_state(
			$from, $to, 'connected', 45);
	}
}
ok($full_mesh, 'L1 four-node peer mesh connected');

ok(wait_for(
		sub {
			return has_current_mx_capability($node0, 1)
			  && has_current_mx_capability($node1, 0);
		},
		30),
	'L1 current-MultiXact capability is valid in both request directions')
	  or do {
		diag('node0 capabilities: ' . $node0->safe_psql(
			'postgres', q{SELECT value FROM pg_cluster_state
			              WHERE category = 'ic' AND key = 'peer_capabilities'}));
		diag('node1 capabilities: ' . $node1->safe_psql(
			'postgres', q{SELECT value FROM pg_cluster_state
			              WHERE category = 'ic' AND key = 'peer_capabilities'}));
		BAIL_OUT('current-MultiXact capability did not converge');
	};

ok(wait_for(
		sub {
			for my $node_id (0 .. 3)
			{
				return 0
				  unless $quad->node($node_id)->safe_psql(
					'postgres',
					q{SELECT count(*) = 4
					    FROM pg_cluster_membership
					   WHERE state = 'member'}) eq 't';
			}
			return 1;
		},
		60),
	'L1 all four membership write gates are open')
	  or BAIL_OUT('four-node membership did not converge');

ok(wait_for(
		sub {
			my @floors = map {
				$quad->node($_)->safe_psql(
					'postgres',
					q{SELECT coalesce(max(value::bigint), 0)
					    FROM cluster_dump_state()
					   WHERE key = 'mxid_stripe_activated_floor'})
			} (0 .. 3);
			return $floors[0] > 0
			  && !grep { $_ != $floors[0] } @floors;
		},
		60),
	'L1 four-node MultiXact activation floor is ready')
	  or BAIL_OUT('mxid activation floor did not converge');

ok(write_retry($node0, 'CREATE EXTENSION IF NOT EXISTS pageinspect'),
	'L1 pageinspect fixture support is writable') or BAIL_OUT('could not install pageinspect');

ok(mirrored_coincident_create(
		$node0, $node1, 'cmxf_t',
		'CREATE TABLE cmxf_t (id int, v int)'),
	'L2 relation identity coincides') or BAIL_OUT('could not create a coincident relation');

$node0->safe_psql('postgres', 'INSERT INTO cmxf_t VALUES (1, 0), (2, 0), (3, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');
my $fixture_xmin = $node0->safe_psql(
	'postgres',
	q{SELECT xmin::text::int FROM cmxf_t WHERE id = 1});
my $fixture_epoch = $node1->safe_psql(
	'postgres',
	q{SELECT new_epoch FROM pg_cluster_reconfig_state});
ok(wait_for(
		sub {
			my $installed = eval {
				$node1->safe_psql(
					'postgres',
					qq{SELECT cluster_test_inject_visibility_tt_ref(
					    '$fixture_xmin'::xid, 0, 1, 1,
					    $fixture_epoch, 1::int8, false)})
			};
			return defined($installed) && $installed eq 't';
		},
		30),
	'fixture installs only the committed foreign xmin prerequisite')
	  or BAIL_OUT('could not isolate the foreign xmin prerequisite');
$node1->safe_psql(
	'postgres',
	q{ALTER SYSTEM SET cluster_test_force_visibility_cluster_path = on});
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);

ok(wait_for(
		sub {
			my $row = eval {
				$node1->safe_psql(
					'postgres',
					q{SELECT id::text || '|' || v::text
					    FROM cmxf_t WHERE id = 1})
			};
			return defined($row) && $row eq '1|0';
		},
		30),
	'fixture proves the foreign xmin visible before composing the MultiXact')
	  or BAIL_OUT('foreign xmin authority did not converge');

my $locker1 = $node0->background_psql('postgres', on_error_die => 1);
my $locker2 = $node0->background_psql('postgres', on_error_die => 1);
$locker1->query_safe('BEGIN');
$locker1->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');
$locker2->query_safe('BEGIN');
$locker2->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');

$locker1->query_safe('COMMIT');
ok(tuple_has_multixact($node0, 'cmxf_t'),
	'RED-F fixture retains a real foreign-origin MultiXact with one ACTIVE member');
ok(state_key_exists($node1, 'describe_remote_ask_count')
	  && state_key_exists($node1, 'member_proof_ask_count')
	  && state_key_exists($node1, 'wait_count')
	  && state_key_exists($node1, 'foreign_slru_guard_count'),
	'current-MultiXact observability keys are exposed');
my $describe_before = state_int($node1, 'describe_remote_ask_count');
my $proof_before = state_int($node1, 'member_proof_ask_count');
my $wait_before = state_int($node1, 'wait_count');
my $guard_before = state_int($node1, 'foreign_slru_guard_count');

my $writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($writer, 'UPDATE cmxf_t SET v = v + 1 WHERE id = 1');
my %observed_waits;
my $entered_current_wait = wait_for(
	sub {
		my $event = $node1->safe_psql(
			'postgres',
			q{SELECT coalesce(wait_event, '')
			     FROM pg_stat_activity
			    WHERE query LIKE '%UPDATE cmxf_t SET v = v + 1 WHERE id = 1%'
			      AND pid <> pg_backend_pid()
			      AND state = 'active'
			    LIMIT 1});
		$observed_waits{$event} = 1 if $event ne '';
		return state_int($node1, 'wait_count') > $wait_before;
	},
	15);
diag('observed writer waits: ' . join(', ', sort keys %observed_waits))
	unless $entered_current_wait;
unless ($entered_current_wait)
{
	diag('node0 describe_local_count=' . state_int($node0, 'describe_local_count'));
	diag('node1 describe_remote_timeout_count='
	  . state_int($node1, 'describe_remote_timeout_count'));
	for my $node_diag ([node0 => $node0], [node1 => $node1])
	{
		my ($name, $node) = @$node_diag;
		diag($name . ' transport counters: ' . $node->safe_psql(
			'postgres',
			q{SELECT string_agg(category || '.' || key || '=' || value, ', '
			                  ORDER BY category, key)
			    FROM pg_cluster_state
			   WHERE (category = 'lms' AND key IN
			          ('lms_data_dispatch_count',
			           'lms_outbound_not_admitted_count',
			           'lms_outbound_cap_guard_drop_count'))
			      OR (category = 'gcs' AND key = 'block_forward_received_count')}));
	}
}
ok($entered_current_wait,
	'RED-F entered authoritative foreign-MX wait path');

$locker2->query_safe('COMMIT');
ok(wait_for(
		sub { $node1->safe_psql('postgres', 'SELECT v FROM cmxf_t WHERE id = 1') eq '1' },
		20),
	'foreign lock-only MultiXact rejudges and UPDATE succeeds');
eval { $writer->quit };
eval { $locker1->quit };
eval { $locker2->quit };

cmp_ok(state_int($node1, 'describe_remote_ask_count'), '>', $describe_before,
	'foreign descriptor RPC counter advanced');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>', $proof_before,
	'foreign member-proof RPC counter advanced');
is(state_int($node1, 'foreign_slru_guard_count'), $guard_before,
	'positive path never attempted requester-local foreign SLRU decode');

$quad->stop_quad;
done_testing();
