#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 415_cluster_update_rows1_4node.pl
#    S3-P0-26 RED/GREEN fixture: four nodes update one HOT-eligible row
#    concurrently.  Every no-retry UPDATE must affect one row, and the
#    resulting physical chain must contain all four HOT update edges.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use IPC::Run qw(start finish timeout);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub write_file
{
	my ($path, $contents) = @_;
	open(my $fh, '>', $path) or die "open $path: $!";
	print {$fh} $contents;
	close($fh) or die "close $path: $!";
}

sub wait_for_positive
{
	my ($node, $sql, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		my ($rc, $out, $err) =
		  $node->psql('postgres', $sql, timeout => 10);
		return 1 if $rc == 0 && defined($out) && $out =~ /\A[1-9]\d*\z/;
		usleep(200_000);
	}
	return 0;
}

sub write_retry
{
	my ($node, $sql, $seconds) = @_;
	my $deadline = time() + $seconds;

	while (time() < $deadline)
	{
		my ($rc, $out, $err) =
		  $node->psql('postgres', $sql, timeout => 30);
		return 1 if $rc == 0;
		usleep(500_000);
	}
	return 0;
}

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'p026_rows1',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.read_scache = on',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.crossnode_write_write = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 5000',
		'cluster.gcs_block_retransmit_max_retries = 8',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);

$quad->start_quad;
usleep(3_000_000);

for my $from (0 .. 3)
{
	is($quad->node($from)->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$from is alive");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 45),
			"L1 node$from sees node$to connected");
	}
}

for my $i (0 .. 3)
{
	ok($quad->wait_for_membership_count($i, 4, 60),
		"L2 node$i sees all four nodes as MEMBER")
	  or BAIL_OUT("node$i membership did not converge before fixture DDL");
	ok(wait_for_positive(
			$quad->node($i),
			q{SELECT coalesce(max(value::bigint), 0)
			    FROM cluster_dump_state()
			   WHERE key = 'xid_stripe_activated_floor'},
			60),
		"L2 node$i sees a positive xid stripe activation floor")
	  or BAIL_OUT("node$i xid stripe activation did not converge before fixture DDL");

	my ($rc, $out, $err) = $quad->node($i)->psql(
		'postgres',
		q{CREATE TABLE p026_hot (
			id integer NOT NULL,
			payload varchar(100) NOT NULL
		) WITH (fillfactor = 90)},
		timeout => 30);
	is($rc, 0, "L2 node$i created its local catalog mapping exactly once")
	  or BAIL_OUT("node$i fixture DDL failed: out=[$out] err=[$err]");
}

my @heap_paths = map {
	$quad->node($_)->safe_psql(
		'postgres', q{SELECT pg_relation_filepath('p026_hot')})
} (0 .. 3);
is(scalar(grep { $_ eq $heap_paths[0] } @heap_paths), 4,
	'L2 all nodes map the heap to one shared relation file')
  or BAIL_OUT('heap relfilepath identity did not coincide');

ok(write_retry(
		$quad->node0,
		'CREATE EXTENSION IF NOT EXISTS pageinspect',
		60),
	'L2 node0 installed pageinspect');
ok(write_retry(
		$quad->node0,
		q{INSERT INTO p026_hot VALUES (1, '')},
		60),
	'L2 one collision seed row inserted');
ok(write_retry(
		$quad->node0,
		'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) p026_hot',
		60),
	'L2 seed row frozen');
ok(write_retry($quad->node0, 'CHECKPOINT', 60),
	'L2 seed checkpointed');

for my $i (0 .. 3)
{
	is($quad->node($i)->safe_psql(
			'postgres', q{SELECT payload FROM p026_hot WHERE id = 1}),
		'',
		"L3 node$i pre-read established S");
}

my $start_at = $quad->node0->safe_psql(
	'postgres',
	q{SELECT (clock_timestamp() + interval '5 seconds')::text});
my $script_dir = PostgreSQL::Test::Utils::tempdir();
my @tokens = qw(A B C D);
my @runs;

for my $i (0 .. 3)
{
	my $script = "$script_dir/node${i}.sql";
	write_file(
		$script,
		"\\set ON_ERROR_STOP on\n"
		  . "SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
		  . "(TIMESTAMPTZ '$start_at' - clock_timestamp()))));\n"
		  . "UPDATE p026_hot SET payload = payload || '$tokens[$i]' "
		  . "WHERE id = 1;\n"
		  . "\\echo P026_ROWCOUNT=:ROW_COUNT\n");

	my %run = (stdout => '', stderr => '', timed_out => 0);
	my @cmd = (
		$quad->node($i)->installed_command('psql'),
		'-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
		'-f', $script,
		'-h', $quad->node($i)->host,
		'-p', $quad->node($i)->port,
		'postgres');
	$run{handle} = start(
		\@cmd, '<', \undef, '>', \$run{stdout}, '2>', \$run{stderr},
		timeout(60));
	push @runs, \%run;
}

for my $i (0 .. 3)
{
	my $run = $runs[$i];
	my $finished = eval { finish($run->{handle}); 1 };
	unless ($finished)
	{
		$run->{timed_out} = 1;
		eval { $run->{handle}->kill_kill; };
	}
	$run->{result} = eval { $run->{handle}->result(0) };
	$run->{result} = -1 unless defined($run->{result});
	my ($rowcount) =
	  $run->{stdout} =~ /(?:\A|\n)P026_ROWCOUNT=(\d+)(?:\n|\z)/;
	$rowcount = '' unless defined($rowcount);

	diag("L3 node$i rc=$run->{result} timed_out=$run->{timed_out} "
		. "stdout=[$run->{stdout}] stderr=[$run->{stderr}]");
	ok(!$run->{timed_out}, "L3 node$i completed before deadline");
	unlike(
		$run->{stderr},
		qr/cluster TT|cluster PCM-X|retransmit budget|current MultiXact authority/i,
		"L3 node$i is not masked by another fail-closed family");
	is($run->{result}, 0, "L3 node$i UPDATE completed successfully");
	is($rowcount, '1', "L3 node$i UPDATE affected exactly one row");
}

my @legal_payloads = qw(
  ABCD ABDC ACBD ACDB ADBC ADCB
  BACD BADC BCAD BCDA BDAC BDCA
  CABD CADB CBAD CBDA CDAB CDBA
  DABC DACB DBAC DBCA DCAB DCBA
);
my %legal_payloads = map { $_ => 1 } @legal_payloads;
my @payloads = map {
	$quad->node($_)->safe_psql(
		'postgres', q{SELECT payload FROM p026_hot WHERE id = 1})
} (0 .. 3);

for my $i (0 .. 3)
{
	ok(exists($legal_payloads{$payloads[$i]}),
		"L4 node$i sees one legal four-update serial order");
}
is($payloads[1], $payloads[0], 'L4 node1 agrees with node0');
is($payloads[2], $payloads[0], 'L4 node2 agrees with node0');
is($payloads[3], $payloads[0], 'L4 node3 agrees with node0');

my $chain_proof = $quad->node0->safe_psql(
	'postgres',
	q{
		WITH items AS (
			SELECT 0 AS blkno, h.lp, h.t_ctid, f.raw_flags
			FROM heap_page_items(get_raw_page('p026_hot', 0)) AS h
			CROSS JOIN LATERAL
				heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f
		),
		edges AS (
			SELECT
				'HEAP_HOT_UPDATED' = ANY(p.raw_flags) AS predecessor_hot,
				'HEAP_ONLY_TUPLE' = ANY(s.raw_flags) AS successor_heap_only,
				p.blkno = s.blkno AS same_block
			FROM items p
			JOIN items s
			  ON p.t_ctid = format('(%s,%s)', s.blkno, s.lp)::tid
			WHERE p.t_ctid <> format('(%s,%s)', p.blkno, p.lp)::tid
		)
		SELECT count(*),
		       coalesce(bool_and(predecessor_hot), false),
		       coalesce(bool_and(successor_heap_only), false),
		       coalesce(bool_and(same_block), false)
		FROM edges
	});
my ($edge_count, $predecessors_hot, $successors_heap_only, $same_block) =
  split(/\|/, $chain_proof, -1);
is($edge_count, '4', 'L5 pageinspect finds all four HOT update edges');
is($predecessors_hot, 't', 'L5 every predecessor is HEAP_HOT_UPDATED');
is($successors_heap_only, 't', 'L5 every successor is HEAP_ONLY_TUPLE');
is($same_block, 't', 'L5 every update edge stays on one heap block');

$quad->stop_quad;
done_testing();
