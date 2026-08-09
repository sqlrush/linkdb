# Copyright (c) 2026, pgrac contributors

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Spec;
use FindBin;
use Test::More;

my $root = abs_path(File::Spec->catdir($FindBin::RealBin, '..', '..', '..', '..'));
my $provider = File::Spec->catfile(
	$root, 'src', 'backend', 'cluster', 'cluster_runtime_visibility.c');

open(my $fh, '<', $provider) or die "open $provider: $!";
local $/;
my $source = <$fh>;
close($fh) or die "close $provider: $!";

my ($sample_body) = $source =~
	/(typedef struct ClusterRuntimeSubtransSample \{.*?\}\s*ClusterRuntimeSubtransSample;)/s;
ok(defined($sample_body), 'R4 subtransaction sample structure exists');
like($sample_body, qr/TransactionId\s+xids\[CLUSTER_R4_SUBTRANS_MAX_DEPTH\]/,
	'subtransaction sample retains one xid chain');
unlike($sample_body, qr/TransactionId\s+parents\[/,
	'subtransaction sample does not retain a duplicate parent chain');
like($source,
	qr/expected_parent\s*=\s*i\s*\+\s*1\s*<\s*sample->count\s*\?\s*sample->xids\[i\s*\+\s*1\]\s*:\s*InvalidTransactionId/s,
	'recheck derives each expected parent from the adjacent sampled xid');
unlike($source, qr/for\s*\(i\s*=\s*0;\s*i\s*<=\s*depth;\s*i\+\+\)/,
	'sampling has no nested prior-xid scan');
like($source, qr/parent_distance\s*<=\s*previous_distance/,
	'sampling rejects non-monotonic parent distance in constant work per edge');

done_testing();
