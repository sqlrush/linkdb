# Copyright (c) 2026, pgrac contributors

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Find;
use File::Spec;
use FindBin;
use Test::More;

my $root = abs_path(File::Spec->catdir($FindBin::RealBin, '..', '..', '..', '..'));
my $backend = File::Spec->catdir($root, 'src', 'backend');
my $provider = File::Spec->catfile($backend, 'cluster', 'cluster_tx_enqueue.c');
my %allowed_callsite = map { $_ => 1 } (
	File::Spec->catfile($backend, 'access', 'heap', 'heapam.c'),
	File::Spec->catfile($backend, 'access', 'heap', 'heapam_visibility.c'),
);

open(my $provider_fh, '<', $provider) or die "open $provider: $!";
local $/;
my $provider_source = <$provider_fh>;
close($provider_fh) or die "close $provider: $!";

like(
	$provider_source,
	qr/cluster_tx_enqueue_wait_exact\s*\([^\{]+\{.{0,3000}Assert\(AmRegularBackendProcess\(\)\);/s,
	'TARGET exact-wait entry is restricted to an ordinary backend');

my @unexpected;
find(
	{
		wanted => sub {
			return unless -f $_ && /\.c\z/;
			my $path = abs_path($File::Find::name);
			return if $path eq $provider;
			open(my $fh, '<', $path) or die "open $path: $!";
			local $/;
			my $source = <$fh>;
			close($fh) or die "close $path: $!";
			if ($source =~ /\bcluster_tx_enqueue_wait_exact\s*\(/s
				&& !$allowed_callsite{$path})
			{
				push @unexpected, File::Spec->abs2rel($path, $root);
			}
		},
		no_chdir => 1,
	},
	$backend);

is_deeply(\@unexpected, [],
	'TARGET production callsites are absent or confined to the named heap backend files');

done_testing();
