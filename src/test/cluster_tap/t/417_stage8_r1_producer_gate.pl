#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 417_stage8_r1_producer_gate.pl
#    Validate the public Stage 8 R1 observation manifest.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Spec;
use File::Temp qw(tempdir);
use FindBin;
use Test::More;

my $root = abs_path("$FindBin::RealBin/../../../..");
my $manifest =
	"$root/src/test/cluster_tap/data/stage8-r1-public-observability.tsv";
my @header = qw(
	schema_version object_id object_kind semantic_event producer_path
	producer_symbol consumer_path consumer_symbol observable_key lifecycle
	positive_test_path positive_test_id negative_test_id public_status);
my %object_kind = map { $_ => 1 } qw(
	STATE GUARD MESSAGE COUNTER GAUGE PAIR HISTOGRAM TEST_GATE);
my $required_tap =
	'src/test/cluster_tap/t/418_stage8_r1_observability_2node.pl';
my @nonpublic_patterns = (
	[ private => qr/(?<![a-z0-9])private(?![a-z0-9])/i ],
	[ future => qr/(?<![a-z0-9])future(?![a-z0-9])/i ],
	[ downstream_r => qr/(?<![a-z0-9])r(?:[2-3]|[5-9]|1[0-4])(?![a-z0-9])/i ],
	[ ad => qr/(?<![a-z0-9])ad(?:-[0-9]+)?(?![a-z0-9])/i ],
	[ talk => qr/(?<![a-z0-9])talk(?![a-z0-9])/i ],
	[ commit => qr/(?<![a-z0-9])commit(?![a-z0-9])/i ],
	[ review => qr/(?<![a-z0-9])review(?![a-z0-9])/i ],
	[ formal => qr/(?<![a-z0-9])formal(?![a-z0-9])/i ],
	[ campaign => qr/(?<![a-z0-9])campaign(?![a-z0-9])/i ]);

sub slurp
{
	my ($path) = @_;

	open(my $fh, '<', $path) or return undef;
	local $/;
	my $text = <$fh>;
	close($fh) or die "close $path: $!";
	return $text;
}

sub path_error
{
	my ($path) = @_;

	return 'EMPTY' if !defined($path) || $path eq '';
	return 'ABSOLUTE'
		if File::Spec->file_name_is_absolute($path)
		|| $path =~ m{\A(?:[A-Za-z]:[\\/]|~[/\\])};
	return 'FORMAT' if $path =~ /\\/;
	return 'TRAVERSAL' if grep { $_ eq '..' } split(m{/}, $path, -1);
	my ($first) = split(m{/}, $path, 2);
	return 'MACHINE_LOCAL'
		if defined($first)
		&& $first =~ /\A(?:Users|home|tmp|var|private|opt|Volumes|scratchpad|\.worktrees)\z/i;
	return undef;
}

sub validate_manifest
{
	my ($path, $expected_row_count) = @_;
	$expected_row_count //= 16;
	my @errors;
	my @rows;
	my $text = slurp($path);

	return ([ 'OPEN' ], \@rows) unless defined($text);
	for my $entry (@nonpublic_patterns)
	{
		my ($name, $pattern) = @$entry;
		push @errors, "NONPUBLIC:$name" if $text =~ $pattern;
	}

	my @lines = split(/\n/, $text, -1);
	pop @lines if @lines && $lines[-1] eq '';
	my @actual_header = split(/\t/, shift(@lines) // '', -1);
	push @errors, 'HEADER'
		unless join("\t", @actual_header) eq join("\t", @header);

	my %seen;
	for my $line_index (0 .. $#lines)
	{
		my $line_number = $line_index + 2;
		my @values = split(/\t/, $lines[$line_index], -1);

		if (@values != @header)
		{
			push @errors, "COLUMNS:$line_number";
			next;
		}

		my %row;
		@row{@header} = @values;
		push @rows, \%row;
		push @errors, "SCHEMA:$line_number"
			unless $row{schema_version} eq '1';
		push @errors, "ID:$line_number"
			unless $row{object_id} =~ /\A[a-z0-9._-]+\z/;
		push @errors, "DUPLICATE:$row{object_id}"
			if $seen{$row{object_id}}++;
		push @errors, "KIND:$line_number"
			unless $object_kind{$row{object_kind}};
		push @errors, "STATUS:$line_number"
			unless $row{public_status} eq 'ACTIVE';

		for my $field (qw(semantic_event producer_symbol consumer_symbol
			observable_key lifecycle positive_test_id negative_test_id))
		{
			push @errors, "REQUIRED:$line_number:$field"
				if $row{$field} eq '';
		}

		for my $field (qw(producer_path consumer_path positive_test_path))
		{
			my $reason = path_error($row{$field});
			if (defined($reason))
			{
				push @errors, "PATH_$reason:$line_number:$field";
				next;
			}

			my $full = "$root/$row{$field}";
			my $resolved = abs_path($full);
			if (!defined($resolved) || !-f $resolved)
			{
				push @errors, "MISSING:$line_number:$field";
				next;
			}
			push @errors, "PATH_ESCAPE:$line_number:$field"
				unless index($resolved, "$root/") == 0;
		}

		push @errors, "TEST_PRODUCER:$line_number"
			if $row{producer_path} =~ m{\Asrc/test/};
		push @errors, "TAP_PATH:$line_number"
			unless $row{positive_test_path} eq $required_tap;

		for my $pair ([qw(producer_path producer_symbol)],
			[qw(consumer_path consumer_symbol)])
		{
			my ($path_field, $symbol_field) = @$pair;
			next if defined(path_error($row{$path_field}));
			my $source = slurp("$root/$row{$path_field}");
			my $symbol = $row{$symbol_field};
			push @errors, "SYMBOL:$line_number:$symbol_field:$symbol"
				if !defined($source) || $symbol eq ''
				|| index($source, $symbol) < 0;
		}

		if (!defined(path_error($row{positive_test_path})))
		{
			my $tap = slurp("$root/$row{positive_test_path}");
			for my $id_field (qw(positive_test_id negative_test_id))
			{
				my $id = $row{$id_field};
				push @errors, "ANCHOR:$line_number:$id_field:$id"
					if !defined($tap) || $id eq ''
					|| $tap !~ /(?<![A-Za-z0-9_])\Q$id\E(?![A-Za-z0-9_])/;
			}
		}
	}

	push @errors, 'ROW_COUNT' unless @rows == $expected_row_count;
	return (\@errors, \@rows);
}

sub fixture_text
{
	my @rows;

	for my $number (1 .. 16)
	{
		push @rows, [
			'1', "fixture-$number", 'COUNTER', "fixture-event-$number",
			'src/backend/cluster/cluster_gcs_block.c',
			'cluster_gcs_get_pi_watermark_retire_count',
			'src/backend/cluster/cluster_debug.c',
			'cluster_gcs_get_pi_watermark_retire_count',
			'gcs.pi_watermark_retire_count', 'postmaster-incarnation',
			$required_tap, 'L1', 'L2', 'ACTIVE' ];
	}
	return \@rows;
}

sub write_fixture
{
	my ($directory, $name, $rows) = @_;
	my $path = "$directory/$name";

	open(my $fh, '>', $path) or die "open $path: $!";
	print {$fh} join("\t", @header), "\n";
	print {$fh} join("\t", @$_), "\n" for @$rows;
	close($fh) or die "close $path: $!";
	return $path;
}

sub has_error
{
	my ($errors, $pattern) = @_;
	return scalar(grep { $_ =~ $pattern } @$errors) > 0;
}

ok(-f $manifest, 'L1 public observation manifest exists');
my ($manifest_errors, $manifest_rows) = validate_manifest($manifest, 27);
is(scalar(@$manifest_errors), 0,
	'L2 public manifest satisfies every structural and behavior anchor rule')
	or diag(join(';', @$manifest_errors));
is(scalar(@$manifest_rows), 27,
	'L3 public manifest contains exactly twenty-seven current observation facts');

my $temporary = tempdir(CLEANUP => 1);
my $valid_rows = fixture_text();
my ($valid_errors) =
	validate_manifest(write_fixture($temporary, 'valid.tsv', $valid_rows));
is(scalar(@$valid_errors), 0, 'L4 controlled public fixture is valid')
	or diag(join(';', @$valid_errors));

my $short_rows = fixture_text();
pop @{$short_rows->[0]};
my ($short_errors) =
	validate_manifest(write_fixture($temporary, 'short.tsv', $short_rows));
ok(has_error($short_errors, qr/\ACOLUMNS:/), 'L5 a 13-column row is rejected');

my $duplicate_rows = fixture_text();
push @$duplicate_rows, [ @{$duplicate_rows->[0]} ];
my ($duplicate_errors) =
	validate_manifest(write_fixture($temporary, 'duplicate.tsv', $duplicate_rows));
ok(has_error($duplicate_errors, qr/\ADUPLICATE:/),
	'L6 a duplicate object id is rejected');

my $test_producer_rows = fixture_text();
$test_producer_rows->[0][4] = $required_tap;
$test_producer_rows->[0][5] = 'done_testing';
my ($test_producer_errors) = validate_manifest(
	write_fixture($temporary, 'test-producer.tsv', $test_producer_rows));
ok(has_error($test_producer_errors, qr/\ATEST_PRODUCER:/),
	'L7 a test-only producer is rejected');

for my $case (
	[ absolute => '/tmp/product.c', qr/\APATH_ABSOLUTE:/ ],
	[ traversal => '../private/product.c', qr/\APATH_TRAVERSAL:/ ],
	[ machine_local => 'Users/developer/product.c', qr/\APATH_MACHINE_LOCAL:/ ])
{
	my ($name, $bad_path, $expected) = @$case;
	my $rows = fixture_text();
	$rows->[0][4] = $bad_path;
	my ($errors) =
		validate_manifest(write_fixture($temporary, "$name.tsv", $rows));
	ok(has_error($errors, $expected), "L8 $name path is rejected");
}

for my $case (
	[ private => 'private' ], [ future => 'future' ], [ downstream_r => 'R12' ],
	[ ad => 'AD-999' ], [ talk => 'talk' ], [ commit => 'commit' ],
	[ review => 'review' ], [ formal => 'formal' ], [ campaign => 'campaign' ])
{
	my ($name, $word) = @$case;
	my $rows = fixture_text();
	$rows->[0][3] = $word;
	my ($errors) = validate_manifest(
		write_fixture($temporary, "nonpublic-$name.tsv", $rows));
	ok(has_error($errors, qr/\ANONPUBLIC:\Q$name\E\z/),
		"L9 nonpublic $name vocabulary is rejected");
}

my $missing_symbol_rows = fixture_text();
$missing_symbol_rows->[0][5] = 'stage8_r1_symbol_does_not_exist';
my ($missing_symbol_errors) = validate_manifest(
	write_fixture($temporary, 'missing-symbol.tsv', $missing_symbol_rows));
ok(has_error($missing_symbol_errors, qr/\ASYMBOL:/),
	'L10 a missing producer symbol is rejected');

my $stale_anchor_rows = fixture_text();
$stale_anchor_rows->[0][11] = 'L_DOES_NOT_EXIST';
my ($stale_anchor_errors) = validate_manifest(
	write_fixture($temporary, 'stale-anchor.tsv', $stale_anchor_rows));
ok(has_error($stale_anchor_errors, qr/\AANCHOR:/),
	'L11 a stale TAP anchor is rejected');

done_testing();
