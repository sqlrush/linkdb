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
use Time::HiRes qw(time usleep);

my $pgrd_voting_file_bytes = (8 * 128 + 3) * 512;


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
	truncate($fh, $pgrd_voting_file_bytes) or die "size $path: $!";
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

sub ctrc_int
{
	my ($node, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		   FROM pg_cluster_state
		  WHERE category = 'ctrc' AND key = '$key'});
	return int($value // 0);
}


sub cluster_state_int
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value::bigint), 0)
		   FROM pg_cluster_state
		  WHERE category = '$category' AND key = '$key'});
	return int($value // 0);
}


sub ctrc_text
{
	my ($node, $key) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT coalesce(max(value), '')
		   FROM pg_cluster_state
		  WHERE category = 'ctrc' AND key = '$key'});
}


sub ctrc_key_exists
{
	my ($node, $key) = @_;
	return $node->safe_psql(
		'postgres',
		qq{SELECT EXISTS (
		     SELECT 1
		       FROM pg_cluster_state
		      WHERE category = 'ctrc' AND key = '$key')}) eq 't';
}


sub ctrc_barrier
{
	my ($node, $phase, $armed) = @_;
	my $type = $armed ? 'skip' : 'none';
	my $param = $armed ? $phase : 0;
	return $node->safe_psql(
		'postgres',
		qq{SELECT cluster_inject_fault(
		     'cluster-ctrc-stage-barrier', '$type', $param)}) eq 't';
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


sub activate_semantic_round
{
	my ($node, $label) = @_;
	my $deadline = time() + 60;
	my ($last_rc, $last_out, $last_err);

	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql(
			'postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 45);
		($last_rc, $last_out, $last_err) = ($rc, $out, $err);
		return if defined($rc) && $rc == 0;
		die "$label activation result is unknown after SQL timeout: "
			. ($err // '<undef>')
			unless defined($rc);
		die "$label activation failed outside the retry contract: "
			. ($err // '<undef>')
			unless defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
		usleep(100_000);
	}

	die "$label activation did not reach OPEN_APPLIED: rc="
		. (defined($last_rc) ? $last_rc : '<undef>')
		. ' stdout=[' . ($last_out // '') . '] stderr=['
		. ($last_err // '') . ']';
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
	shared_system_identifier => 1,
	shared_system_identifier_seed_sql => q{
		CREATE EXTENSION IF NOT EXISTS pageinspect;
		CREATE TABLE cmxf_t (id int, v int);
		CREATE TABLE cmxf_cross_churn (id int PRIMARY KEY, v int)
		    WITH (fillfactor = 50);
		CREATE TABLE cmxf_cross (id int PRIMARY KEY, v int, pad text)
		    WITH (fillfactor = 50);
	},
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.read_scache = on',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 5000',
		'cluster.undo_cleaner_interval_ms = 200',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
	]);
my @vote_paths = $quad->voting_disk_paths;
for my $disk_index (0 .. $#vote_paths)
{
	format_stage8_voting_file($vote_paths[$disk_index], $disk_index);
}
for my $node ($quad->nodes)
{
	$node->append_conf('postgresql.conf',
		"cluster.voting_disk_size_bytes = $pgrd_voting_file_bytes\n");
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

ok($node0->safe_psql(
		'postgres',
		q{SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pageinspect')}) eq 't'
	&& $node1->safe_psql(
		'postgres',
		q{SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pageinspect')}) eq 't',
	'L1 pageinspect fixture support is inherited from the native baseline')
	  or BAIL_OUT('pageinspect baseline fixture is unavailable');

is($node0->safe_psql('postgres', q{SELECT pg_relation_filepath('cmxf_t')}),
	$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('cmxf_t')}),
	'L2 relation identity coincides');

my $pgrd_root = $quad->shared_data_root . '/pg_undo';
my $pgrd_mirror = "$pgrd_root/pgrac_undo_root.control";
mkdir $pgrd_root or die "mkdir $pgrd_root: $!";
for my $node ($quad->nodes)
{
	$node->poll_query_until(
		'postgres', q{SELECT in_quorum FROM pg_cluster_quorum_state}, 't')
	  or BAIL_OUT('pre-OPEN voting-disk majority did not become current');
	my ($rc, $out, $err);
	my $deadline = time() + 15;
	while (time() < $deadline)
	{
		($rc, $out, $err) = $node->psql(
			'postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 30);
		last if defined($rc) && $rc != 0 && defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
		usleep(100_000);
	}
	BAIL_OUT('pre-OPEN PGRD setup did not remain deferred: '
		. ($err // '<undef>'))
	  unless defined($rc) && $rc != 0 && defined($err)
	  && $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
}
ok(-f $pgrd_mirror, 'L1 pre-OPEN PGRD mirror is present');

activate_semantic_round($node0, 'R4 bit0');
activate_semantic_round($node0, 'Resource-X bit10');
for my $node_id (0 .. 3)
{
	is($quad->node($node_id)->safe_psql(
			'postgres',
			q{SELECT value FROM pg_cluster_state
			    WHERE category = 'pcm' AND key = 'resource_x_gate_phase'}),
		'open', "L1 node $node_id Resource-X gate is OPEN_APPLIED");
}

$node0->safe_psql('postgres', 'INSERT INTO cmxf_t VALUES (1, 0), (2, 0), (3, 0)');
$node0->safe_psql('postgres', 'CHECKPOINT');
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
	'R4 target resolves the real foreign xmin before composing the MultiXact')
	  or BAIL_OUT('R4 target foreign xmin authority did not converge');

my $locker1 = $node0->background_psql('postgres', on_error_die => 1);
my $locker2 = $node0->background_psql('postgres', on_error_die => 1);
$locker1->query_safe('BEGIN');
$locker1->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');
$locker2->query_safe('BEGIN');
$locker2->query_safe('SELECT v FROM cmxf_t WHERE id = 1 FOR SHARE');
pass('compatible SHARE composition proceeds without an unnecessary wait');

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
my $origin_barrier_before = ctrc_int($node0, 'test_barrier_hit_count');
my $ordinary_publish_before
	= ctrc_int($node1, 'ordinary_publication_after_apply_count');
my $current_publish_before
	= ctrc_int($node1, 'current_mx_publication_after_apply_count');
my $certificate_before
	= ctrc_int($node0, 'certificate_applied_count')
	  + ctrc_int($node0, 'certificate_replayed_count');
my $l11_before = ctrc_int($node0, 'l11_release_sample_count');
my $l12_before = ctrc_int($node0, 'l12_recycle_count');
my $transport_before = {};
for my $node_diag ([node0 => $node0], [node1 => $node1])
{
	my ($name, $node) = @$node_diag;
	$transport_before->{$name} = $node->safe_psql(
		'postgres',
		q{SELECT string_agg(category || '.' || key || '=' || value, ', '
		                  ORDER BY category, key)
		    FROM pg_cluster_state
		   WHERE (category = 'lms' AND key IN
		          ('lms_data_dispatch_count_w0',
		           'lms_data_dispatch_count_w1',
		           'lms_direct_reply_count_w0',
		           'lms_direct_reply_count_w1',
		           'lms_inline_serve_count_w0',
		           'lms_inline_serve_count_w1'))
		      OR (category = 'gcs' AND key IN
		          ('block_reply_count', 'stale_reply_drop_count'))
		      OR (category = 'ic' AND key IN
		          ('tier1_fifo_admitted_data',
		           'tier1_fifo_promoted_data',
		           'tier1_fifo_dropped_close_data'))});
}

ok(ctrc_barrier($node0, 1, 1),
	'origin ACTIVE-proof scheduling barrier armed');
my $writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($writer, 'UPDATE cmxf_t SET v = v + 1 WHERE id = 1');
ok(wait_for(
		sub {
			return ctrc_int($node0, 'test_barrier_hit_count')
			  > $origin_barrier_before;
		},
		20),
	'origin touched-node record precedes consumable ACTIVE proof');
cmp_ok(ctrc_int($node0, 'origin_open'), '>', 0,
	'ACTIVE proof barrier retains an exact OPEN origin row');
is(ctrc_int($node1, 'current_mx_publication_after_apply_count'),
	$current_publish_before,
	'dependent tuple is not published while ACTIVE proof is held');
ok(ctrc_barrier($node0, 0, 0),
	'origin ACTIVE-proof scheduling barrier released');
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
		diag($name . ' transport baseline: ' . $transport_before->{$name});
		diag($name . ' transport counters: ' . $node->safe_psql(
			'postgres',
			q{SELECT string_agg(category || '.' || key || '=' || value, ', '
			                  ORDER BY category, key)
			    FROM pg_cluster_state
			   WHERE (category = 'lms' AND key IN
			          ('lms_data_dispatch_count',
			           'lms_data_dispatch_count_w0',
			           'lms_data_dispatch_count_w1',
			           'lms_direct_reply_count_w0',
			           'lms_direct_reply_count_w1',
			           'lms_inline_serve_count_w0',
			           'lms_inline_serve_count_w1',
			           'lms_outbound_not_admitted_count',
			           'lms_outbound_cap_guard_drop_count'))
			      OR (category = 'gcs' AND key IN
			          ('block_forward_received_count',
			           'block_reply_count', 'stale_reply_drop_count'))
			      OR (category = 'ic' AND key IN
			          ('tier1_fifo_admitted_data',
			           'tier1_fifo_promoted_data',
			           'tier1_fifo_dropped_close_data'))}));
	}
}
ok($entered_current_wait,
	'RED-F entered authoritative foreign-MX wait path');

my $ack_barrier_before = ctrc_int($node1, 'test_barrier_hit_count');
my $certificate_barrier_before = ctrc_int($node0, 'test_barrier_hit_count');
ok(ctrc_barrier($node1, 2, 1),
	'participant durable-ACK scheduling barrier armed');
ok(ctrc_barrier($node0, 3, 1),
	'origin certificate scheduling barrier armed');
$locker2->query_safe('COMMIT');
ok(wait_for(
		sub { $node1->safe_psql('postgres', 'SELECT v FROM cmxf_t WHERE id = 1') eq '1' },
		20),
	'foreign lock-only MultiXact rejudges and UPDATE succeeds');
my $ack_barrier_reached = wait_for(
		sub {
			return ctrc_int($node1, 'test_barrier_hit_count')
			  > $ack_barrier_before;
		},
		30);
unless ($ack_barrier_reached)
{
	for my $node_diag ([node0 => $node0], [node1 => $node1])
	{
		my ($name, $node) = @$node_diag;
		diag($name . ' CTRC timeout state: ' . $node->safe_psql(
			'postgres',
			q{SELECT string_agg(key || '=' || value, ', ' ORDER BY key)
			    FROM pg_cluster_state WHERE category = 'ctrc'}));
		diag($name . ' current-MX timeout state: ' . $node->safe_psql(
			'postgres',
			q{SELECT string_agg(key || '=' || value, ', ' ORDER BY key)
			    FROM pg_cluster_state
			   WHERE category = 'multixact_current'}));
	}
}
ok($ack_barrier_reached,
	'participant reaches ACK barrier only after exact cleanup durability');
is(ctrc_text($node1, 'cleaner_reason'), 'PARTICIPANT_ACK',
	'participant cleaner reports the durable ACK boundary');
cmp_ok(ctrc_int($node1, 'receipt_ack_frozen')
	  + ctrc_int($node1, 'receipt_cleaned'), '>', 0,
	'participant has no live APPLIED target at the ACK boundary');
ok(ctrc_barrier($node1, 0, 0),
	'participant durable-ACK scheduling barrier released');
ok(wait_for(
		sub {
			return ctrc_int($node0, 'test_barrier_hit_count')
			  > $certificate_barrier_before;
		},
		30),
	'origin reaches certificate barrier with the complete ACK set');
is(ctrc_text($node0, 'cleaner_reason'), 'BLOCK0_CERTIFICATE',
	'origin cleaner reports the exact block-0 certificate boundary');
cmp_ok(ctrc_int($node1, 'participant_ack_frozen'), '>', 0,
	'participant ACK is frozen before block-0 release publication');
ok(ctrc_barrier($node0, 0, 0),
	'origin certificate scheduling barrier released');
ok(wait_for(
		sub {
			return ctrc_int($node0, 'certificate_applied_count')
				   + ctrc_int($node0, 'certificate_replayed_count')
			  > $certificate_before;
		},
		30),
	'exact 0xA0 release certificate becomes durable');
ok(wait_for(
		sub {
			return ctrc_int($node0, 'l11_release_sample_count') > $l11_before
			  && ctrc_int($node0, 'l12_recycle_count') > $l12_before;
		},
		60),
	'L11 release sample precedes eventual L12 slot recycle');
eval { $writer->quit };
eval { $locker1->quit };
eval { $locker2->quit };

cmp_ok(state_int($node1, 'describe_remote_ask_count'), '>', $describe_before,
	'foreign descriptor RPC counter advanced');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>', $proof_before,
	'foreign member-proof RPC counter advanced');
is(state_int($node1, 'foreign_slru_guard_count'), $guard_before,
	'positive path never attempted requester-local foreign SLRU decode');
cmp_ok(ctrc_int($node1, 'ordinary_publication_after_apply_count'), '>',
	$ordinary_publish_before,
	'ordinary ITL/UBA receipt is APPLIED before page publication');
cmp_ok(ctrc_int($node1, 'current_mx_publication_after_apply_count'), '>',
	$current_publish_before,
	'remote-member receipt is APPLIED before current-MX tuple publication');

# Adjustment 15: force a real DATA-record-segment != canonical-TT-segment
# successor using only production lifecycle.  The empty fixture relations are
# part of the native four-node baseline so this authority test does not depend
# on an unrelated mid-run catalog-invalidation round.  The held snapshot
# retains every completed TT slot; at most 64 short commits therefore cross
# the 48-slot current TT segment while the tiny DATA workload remains in its
# original 64-MiB segment.
$node0->safe_psql(
	'postgres',
	q{INSERT INTO cmxf_cross_churn VALUES (1, 0);
	  INSERT INTO cmxf_cross VALUES
	    (1, 0, repeat('a', 64)),
	    (2, 0, repeat('b', 64)),
	    (3, 0, repeat('c', 64));});
ok(wait_for(
		sub {
			my $count = eval {
				$node1->safe_psql('postgres', 'SELECT count(*) FROM cmxf_cross')
			};
			return defined($count) && $count eq '3';
		},
		30),
	'cross-segment fixture is visible on the remote requester')
	  or BAIL_OUT('cross-segment fixture did not converge');

my $cross_pin = $node0->background_psql('postgres', on_error_die => 1);
$cross_pin->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$cross_pin->query_safe('SELECT count(*) FROM cmxf_cross_churn');
my $cross_rollover_before
	= cluster_state_int($node0, 'undo', 'tt_retention_rollover_count');
my $cross_rollover_seen = 0;
my $cross_churn_commits = 0;
for (1 .. 64)
{
	$node0->safe_psql(
		'postgres',
		'UPDATE cmxf_cross_churn SET v = v + 1 WHERE id = 1');
	$cross_churn_commits++;
	if (cluster_state_int($node0, 'undo', 'tt_retention_rollover_count')
		> $cross_rollover_before)
	{
		$cross_rollover_seen = 1;
		last;
	}
}
ok($cross_rollover_seen,
	'held snapshot forces a real TT retention rollover within 64 commits');
BAIL_OUT('bounded cross-segment TT rollover fixture did not form')
	unless $cross_rollover_seen;
diag("cross-segment TT rollover formed after $cross_churn_commits commits");

my $cross_locker = $node0->background_psql('postgres', on_error_die => 1);
$cross_locker->query_safe('BEGIN');
$cross_locker->query_safe(
	'SELECT v FROM cmxf_cross WHERE id = 1 FOR KEY SHARE');
is($node0->safe_psql(
		'postgres',
		'UPDATE cmxf_cross SET v = v + 1 WHERE id = 1 RETURNING v'),
	'1',
	'new-TT-segment HOT updater commits while compatible KEY SHARE remains');
ok(tuple_has_multixact($node0, 'cmxf_cross'),
	'old HOT root retains the updater-bearing native MultiXact');
ok(state_key_exists($node0,
		'updater_provenance_cross_segment_match_count'),
	'cross-segment updater provenance counter is exposed');
my $cross_match_before = state_int(
	$node0, 'updater_provenance_cross_segment_match_count');
my $cross_proof_before = state_int($node1, 'member_proof_ask_count');
is($node1->safe_psql(
		'postgres',
		'UPDATE cmxf_cross SET v = v + 1 WHERE id = 1 RETURNING v'),
	'2',
	'remote native UPDATE follows the committed HOT successor exactly once');
is($node1->safe_psql(
		'postgres',
		'UPDATE cmxf_cross SET v = v + 1 WHERE id = 3 RETURNING v'),
	'1',
	'drift actor is refreshed on the current page holder after retention rollover');
ok(wait_for(
		sub {
			return state_int(
				$node0, 'updater_provenance_cross_segment_match_count')
			  > $cross_match_before;
		},
		20),
	'DATA != TT reaches MATCH only through the exact provenance chain');
cmp_ok(state_int($node1, 'member_proof_ask_count'), '>', $cross_proof_before,
	'remote requester consumed the origin member proof for the cross-segment tuple');
is($node0->safe_psql('postgres', 'SELECT v FROM cmxf_cross WHERE id = 1'),
	'2', 'origin observes the exact native cross-segment tuple result');
$cross_locker->query_safe('COMMIT');
eval { $cross_locker->quit };

# Re-form the same legal updater shape on row 2, then change unrelated row 3
# on the requester/Resource-X-holder node while the origin proof is paused.
# Both rows are asserted on one heap page; the saved page witness must restart
# before mutation, without any verdict-changing fault.
my $drift_locker = $node0->background_psql('postgres', on_error_die => 1);
$drift_locker->query_safe('BEGIN');
$drift_locker->query_safe(
	'SELECT v FROM cmxf_cross WHERE id = 2 FOR KEY SHARE');
is($node0->safe_psql(
		'postgres',
		'UPDATE cmxf_cross SET v = v + 1 WHERE id = 2 RETURNING v'),
	'1', 'drift fixture commits its compatible HOT updater');
is($node1->safe_psql(
		'postgres',
		q{SELECT count(DISTINCT
		     split_part(trim(both '()' from ctid::text), ',', 1)) = 1
		   FROM cmxf_cross WHERE id IN (2, 3)}),
	't', 'drift target and unrelated actor tuple occupy the same heap page');
my $drift_barrier_before = ctrc_int($node1, 'test_barrier_hit_count');
my $aba_before = state_int($node1, 'aba_restart_count');
ok(ctrc_barrier($node1, 4, 1),
	'requester requalification scheduling barrier armed for page drift');
my $drift_writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($drift_writer,
	'UPDATE cmxf_cross SET v = v + 1 WHERE id = 2');
ok(wait_for(
		sub {
			return ctrc_int($node1, 'test_barrier_hit_count')
			  > $drift_barrier_before;
		},
		20),
	'requester pauses after proof return and before page requalification');
is($node1->safe_psql(
		'postgres',
		'UPDATE cmxf_cross SET v = v + 1 WHERE id = 3 RETURNING v'),
	'2', 'same-page unrelated actor changes the saved requester witness');
ok(ctrc_barrier($node1, 0, 0),
	'requester requalification scheduling barrier released');
ok(wait_for(
		sub {
			return $node1->safe_psql(
				'postgres', 'SELECT v FROM cmxf_cross WHERE id = 2') eq '2';
		},
		30),
	'page-witness drift restarts and then completes the native UPDATE');
cmp_ok(state_int($node1, 'aba_restart_count'), '>', $aba_before,
	'changed page witness is rejected before mutation');
$drift_locker->query_safe('COMMIT');
eval { $drift_writer->quit };
eval { $drift_locker->quit };
$cross_pin->query_safe('ROLLBACK');
eval { $cross_pin->quit };

# Hold a second positive proof after touch, commit its owner, and make the
# final send-side OPEN/grant recheck reject the stale proof.  No sleeps decide
# the outcome: phase 1 is the same shared scheduling-only seam used above.
my $stale_locker1 = $node0->background_psql('postgres', on_error_die => 1);
my $stale_locker2 = $node0->background_psql('postgres', on_error_die => 1);
$stale_locker1->query_safe('BEGIN');
$stale_locker1->query_safe('SELECT v FROM cmxf_t WHERE id = 2 FOR SHARE');
$stale_locker2->query_safe('BEGIN');
$stale_locker2->query_safe('SELECT v FROM cmxf_t WHERE id = 2 FOR SHARE');
$stale_locker1->query_safe('COMMIT');
my $stale_barrier_before = ctrc_int($node0, 'test_barrier_hit_count');
my $seal_before = ctrc_int($node0, 'seal_started_count');
my $refused_before = ctrc_int($node0, 'grant_refused_count');
my $unknown_before = state_int($node1, 'member_proof_unknown_count');
my $stale_publish_before
	= ctrc_int($node1, 'current_mx_publication_after_apply_count');
ok(ctrc_barrier($node0, 1, 1),
	'delayed old-grant scheduling barrier armed');
my $stale_writer = $node1->background_psql('postgres', on_error_die => 1);
start_blocking($stale_writer, 'UPDATE cmxf_t SET v = v + 1 WHERE id = 2');
ok(wait_for(
		sub {
			return ctrc_int($node0, 'test_barrier_hit_count')
			  > $stale_barrier_before;
		},
		20),
	'second ACTIVE proof is held after touched-node registration');
$stale_locker2->query_safe('COMMIT');
ok(wait_for(
		sub { return ctrc_int($node0, 'seal_started_count') > $seal_before },
		30),
	'terminal CLOSE enters SEALING while the old grant is delayed');
is(ctrc_int($node1, 'current_mx_publication_after_apply_count'),
	$stale_publish_before,
	'delayed old grant publishes no partial tuple change');
ok(ctrc_barrier($node0, 0, 0),
	'delayed old-grant scheduling barrier released');
ok(wait_for(
		sub { $node1->safe_psql('postgres', 'SELECT v FROM cmxf_t WHERE id = 2') eq '1' },
		30),
	'stale proof is refused and the full tuple operation restarts');
cmp_ok(ctrc_int($node0, 'grant_refused_count'), '>', $refused_before,
	'terminal close refuses the delayed old grant');
my $unknown_after = state_int($node1, 'member_proof_unknown_count');
unless ($unknown_after > $unknown_before)
{
	diag('requester proof-result counters: ' . $node1->safe_psql(
		'postgres',
		q{SELECT string_agg(key || '=' || value, ', ' ORDER BY key)
		    FROM pg_cluster_state
		   WHERE category = 'multixact_current'
		     AND key IN ('member_proof_unknown_count',
		                 'member_proof_denied_count',
		                 'member_proof_timeout_count',
		                 'member_proof_invalid_reply_count')}));
}
cmp_ok($unknown_after, '>', $unknown_before,
	'one UNKNOWN member rejects the original proof batch');
eval { $stale_writer->quit };
eval { $stale_locker1->quit };
eval { $stale_locker2->quit };

my $aborter = $node0->background_psql('postgres', on_error_die => 1);
$aborter->query_safe('BEGIN');
$aborter->query_safe('UPDATE cmxf_t SET v = 99 WHERE id = 3');
$aborter->query_safe('ROLLBACK');
is($node0->safe_psql('postgres', 'SELECT v FROM cmxf_t WHERE id = 3'), '0',
	'ordinary ABORT retains no visible tuple change');
eval { $aborter->quit };

for my $node_id (0 .. 3)
{
	my $node = $quad->node($node_id);
	ok(ctrc_key_exists($node, 'publication_order_violation_count'),
		"node $node_id participates with CTRC observability");
	is(ctrc_int($node, 'publication_order_violation_count'), 0,
		"node $node_id reports zero publication-order violation");
	is(ctrc_int($node, 'test_barrier_phase'), 0,
		"node $node_id has no armed CTRC barrier");
}
ok(wait_for(
		sub {
			for my $node ($node0, $node1)
			{
				my $stuck = $node->safe_psql(
					'postgres',
					q{SELECT count(*)
					    FROM pg_stat_activity
					   WHERE wait_event IN
					     ('GcsMultixactDescribeWait',
					      'GcsMultixactMemberProofWait',
					      'GesTxEnqueueWait')});
				return 0 if $stuck != 0;
			}
			return 1;
		},
		20),
	'no current-MX wait edge remains stuck');

$quad->stop_quad;
done_testing();
