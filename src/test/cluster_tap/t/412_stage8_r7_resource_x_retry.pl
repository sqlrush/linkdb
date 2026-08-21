#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 412_stage8_r7_resource_x_retry.pl
#    Focused Stage-8 Resource-X retry/terminal contract checks.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use Test::More;

my $root = abs_path("$FindBin::RealBin/../../../..");

sub slurp
{
	my ($relative) = @_;
	my $path = "$root/$relative";

	open(my $fh, '<', $path) or die "open $path: $!";
	local $/;
	my $text = <$fh>;
	close($fh) or die "close $path: $!";
	return $text;
}

my $gcs = slurp('src/backend/cluster/cluster_gcs_block.c');
my $convert = slurp('src/backend/cluster/cluster_pcm_x_convert.c');
my $retry = slurp('src/backend/cluster/cluster_resource_x_retry.c');
my $retry_header = slurp('src/include/cluster/cluster_resource_x_retry.h');
my $identity_header = slurp('src/include/cluster/cluster_resource_x_identity.h');
my $convert_header = slurp('src/include/cluster/cluster_pcm_x_convert.h');
my $unit = slurp('src/test/cluster_unit/test_cluster_resource_x_retry.c');
my $runtime_unit = slurp('src/test/cluster_unit/test_cluster_pcm_x_convert.c');
my $observation_tap = slurp('src/test/cluster_tap/t/418_stage8_r1_observability_2node.pl');

my @legacy_retry_defs =
  $convert =~ /cluster_pcm_x_local_reliable_retry_exact\s*\(/g;
is(scalar(@legacy_retry_defs), 1,
	'legacy retry primitive has only its definition in the queue engine');
unlike($gcs, qr/cluster_pcm_x_local_reliable_retry_exact\s*\(/,
	'production driver does not call the legacy retry primitive');
like($gcs,
	qr/cluster_gcs_block_pcm_x_formation_tick[\s\S]*?gcs_block_pcm_x_resource_retry_tick\(bindings_before\)/,
	'formation tick owns the Resource-X retry producer');
like($gcs,
	qr/gcs_block_pcm_x_resource_retry_tick[\s\S]*?cluster_pcm_x_retry_work_next\(/,
	'retry producer consumes the bounded resumable work cursor');
like($gcs,
	qr/RESOURCE_X_RETRY_NOT_DUE\)\s*return;\s*cluster_pcm_x_stats_note_retry_due\(\)/,
	'not-due observations do not enter the due counter');
like($gcs,
	qr/admitted = cluster_gcs_pcm_x_stage_frame[\s\S]*?if \(!admitted\)\s*return;\s*cluster_pcm_x_stats_note_retry_wire_attempt\(\)/,
	'wire-attempt count follows outbound admission');
like($gcs,
	qr/cluster_pcm_x_local_retry_recovery_blocked_exact\(/,
	'recovery-blocked classification is committed by an exact queue transition');
like($convert_header,
	qr/cluster_pcm_x_local_retry_recovery_blocked_exact\s*\(/,
	'exact recovery-blocked transition is a queue-engine API');
like($runtime_unit,
	qr/test_resource_x_retry_recovery_blocked_is_exact_and_idempotent/,
	'recovery-blocked transition has focused idempotency and conservation coverage');
like($retry,
	qr/now_mono_us >= state->terminal_deadline_mono_us[\s\S]*?RESOURCE_X_RETRY_TERMINAL_EXHAUSTED[\s\S]*?RESOURCE_X_RETRY_ROLL_FORWARD/,
	'absolute deadline distinguishes pre-return exhaustion from roll-forward');
like($retry_header,
	qr/RESOURCE_X_RETRY_MAX_RETRIES 8[\s\S]*?RESOURCE_X_RETRY_MAX_BACKOFF_MS 5000/,
	'retry state remains bounded by compile-time validation limits');
like($unit,
	qr/RESOURCE_X_TERMINAL_REASON_LEGACY_CANCEL[\s\S]*?RESOURCE_X_TERMINAL_REASON_RETRY_EXHAUSTED[\s\S]*?RESOURCE_X_TERMINAL_REASON_INVALIDATE_TIMEOUT[\s\S]*?RESOURCE_X_TERMINAL_REASON_LOST_WRITE[\s\S]*?RESOURCE_X_TERMINAL_REASON_INVALID/,
	'type-60 reason codec has zero compatibility and a closed nonzero domain');
unlike($identity_header, qr/\breason\b/,
	'terminal reason is absent from Resource-X identity and equality inputs');
like($convert_header, qr/StaticAssertDecl\(sizeof\(PcmXPhasePayload\) == 96/,
	'type-60 payload ABI remains 96 bytes');
like($observation_tap, qr/L17 real Resource-X success terminal/,
	'focused production success and conservation observation is present');
like($observation_tap, qr/L18 Resource-X retry observations reset/,
	'postmaster-incarnation reset coverage is present');

done_testing();
