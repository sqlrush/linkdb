#-------------------------------------------------------------------------
#
# 422_recovery_serialization.pl
#    RF-ROOT P3 / STOP03 recovery-serialization acceptance.
#
#    This first cutover leg proves that the ten former SQL mutation/probe
#    surfaces are absent from the generated catalog and cannot be called or
#    recreated through LANGUAGE internal.  The surviving slot observer is
#    read-only.  Positive holder/recovery/crash legs are added when the ordered
#    RF-ROOT P4 NeedSet/AdmissionSet producer makes them reachable; this file
#    must not manufacture positive authority before then.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my @forbidden = qw(
  cluster_thread_replay_test
  cluster_thread_drive_test
  cluster_thread_replay_one_test
  cluster_thread_replay_one_auto_test
  cluster_thread_replay_slot_test
  cluster_thread_recovery_worker_run_test
  cluster_thread_recovery_launch_test
  cluster_reconfig_inject_dead_node_test
  cluster_ir_acquire_probe
  cluster_ir_release_probe
);

my $node = PgracClusterNode->new('recovery_serialization');
$node->init;
$node->append_conf('postgresql.conf',
		"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n");
$node->start;

my $quoted = join ',', map { "'$_'" } @forbidden;
is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_proc WHERE proname IN ($quoted) OR prosrc IN ($quoted)"),
	'0', 'T23 catalog contains no forbidden recovery mutator/probe name or prosrc');

my $slot_before = $node->safe_psql('postgres',
	'SELECT cluster_thread_replay_slot_state_test(1)');

for my $name (@forbidden)
{
	my ($call_rc, $call_out, $call_err) = $node->psql(
		'postgres', "\\set VERBOSITY verbose\nSELECT $name();");
	isnt($call_rc, 0, "T23 direct call rejected: $name");
	like($call_err, qr/42883/, "T23 direct call is undefined_function: $name");

	(my $suffix = $name) =~ s/^cluster_//;
	my ($create_rc, $create_out, $create_err) = $node->psql(
		'postgres',
		"\\set VERBOSITY verbose\n"
		  . "CREATE FUNCTION public.pgrac_forbidden_$suffix() RETURNS bool "
		  . "AS '$name' LANGUAGE internal;");
	isnt($create_rc, 0, "T23 LANGUAGE internal recreation rejected: $name");
	like($create_err, qr/42883/,
		"T23 LANGUAGE internal recreation is undefined_function: $name");
}

my $slot_after = $node->safe_psql('postgres',
	'SELECT cluster_thread_replay_slot_state_test(1)');
is($slot_after, $slot_before,
	'T23 read-only slot observer leaves the scheduler state unchanged');

$node->stop;
done_testing();
