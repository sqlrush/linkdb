#-------------------------------------------------------------------------
#
# 001_init.pl
#    File-level regression for the pgrac-init Stage 0.20 wrapper.
#
#    This TAP test exercises pgrac-init at the filesystem level (no
#    server start; that is covered by 002_start.pl):
#      - --help / --version contracts
#      - Invalid flags are rejected with a "Try --help" hint
#      - Empty PGDATA is bootstrapped end-to-end (initdb succeeds,
#        cluster.node_id appears in postgresql.conf, pgrac.conf gets
#        a [node.<id>] section)
#      - Idempotent re-run is a no-op
#      - Conflicting re-run without --force is rejected
#      - --force re-run rewrites the conf bits
#      - Out-of-range --node-id is rejected
#
# IDENTIFICATION
#    src/bin/pgrac/t/001_init.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Digest::SHA qw(sha256_hex);
use PostgreSQL::Test::Utils;
use Test::More;

sub crc32c
{
	my ($bytes) = @_;
	my $crc = 0xffffffff;

	for my $byte (unpack('C*', $bytes))
	{
		$crc ^= $byte;
		for (1 .. 8)
		{
			$crc = (($crc >> 1) ^ (($crc & 1) ? 0x82f63b78 : 0))
			  & 0xffffffff;
		}
	}
	return ($crc ^ 0xffffffff) & 0xffffffff;
}

sub wal_state_slot
{
	my ($thread_id, $node_id) = @_;
	my $slot = "\0" x 512;

	substr($slot, 0, 4, pack('L<', 0x50475754));
	substr($slot, 4, 2, pack('S<', 1));
	substr($slot, 6, 2, pack('S<', $thread_id));
	substr($slot, 8, 4, pack('l<', $node_id));
	substr($slot, 12, 4, pack('L<', 1));
	substr($slot, 16, 4, pack('L<', 1));
	substr($slot, 24, 8, pack('q<', 1));
	substr($slot, 32, 8, pack('q<', 1));
	substr($slot, 40, 8, pack('Q<', 1));
	substr($slot, 48, 8, pack('Q<', 1));
	substr($slot, 504, 4, pack('L<', crc32c(substr($slot, 0, 504))));
	return $slot;
}

sub wal_state_image
{
	my ($slot_number, $slot_thread_id) = @_;
	my $header = "\0" x 512;

	substr($header, 0, 4, pack('L<', 0x50475753));
	substr($header, 4, 2, pack('S<', 1));
	substr($header, 8, 4, pack('L<', 128));
	substr($header, 16, 8, pack('q<', 1));
	substr($header, 504, 4,
		pack('L<', crc32c(substr($header, 0, 504))));

	my $image = $header . ("\0" x (128 * 512));
	if (defined $slot_number)
	{
		$slot_thread_id //= $slot_number;
		substr($image, 512 * $slot_number, 512,
			wal_state_slot($slot_thread_id, $slot_number - 1));
	}
	return $image;
}

sub write_wal_state
{
	my ($path, $bytes) = @_;
	open(my $fh, '>:raw', $path) or die "open $path: $!";
	print {$fh} $bytes or die "write $path: $!";
	close($fh) or die "close $path: $!";
	chmod(0600, $path) or die "chmod $path: $!";
}

sub wal_state_identity
{
	my ($path) = @_;
	my @st = stat($path);
	my $bytes = PostgreSQL::Test::Utils::slurp_file($path);
	return join(':', $st[0], $st[1], length($bytes), sha256_hex($bytes));
}

# ----------
# 1. CLI plumbing.
# ----------
program_help_ok('pgrac-init');
program_version_ok('pgrac-init');

# Invalid option -> exit 1 with "Try --help" hint.
my ($stdout, $stderr);
my $rc = IPC::Run::run([ 'pgrac-init', '--bogus-flag' ],
	'>', \$stdout, '2>', \$stderr);
ok(!$rc, 'pgrac-init rejects unknown option');
like($stderr, qr/--help/, 'rejection message includes "--help" hint');

# Out-of-range node id is rejected.
command_fails(
	[ 'pgrac-init', '-D', '/tmp/nonexistent', '--node-id=200' ],
	'pgrac-init rejects --node-id out of [0, 127]');

# ----------
# 2. End-to-end bootstrap on a fresh PGDATA.
# ----------
my $tempdir = PostgreSQL::Test::Utils::tempdir;
my $datadir = "$tempdir/data";

command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=7', '--cluster-name=tap-test' ],
	'pgrac-init -D EMPTY succeeds');

ok(-d $datadir, 'pgrac-init created PGDATA');
ok(-f "$datadir/PG_VERSION",
	'pgrac-init produced a PG-shaped data directory');
ok(-f "$datadir/postgresql.conf",
	'postgresql.conf exists');
ok(-f "$datadir/pgrac.conf",
	'pgrac.conf exists');

my $pgconf = PostgreSQL::Test::Utils::slurp_file("$datadir/postgresql.conf");
like($pgconf, qr/^cluster\.node_id\s*=\s*7\s*$/m,
	'cluster.node_id = 7 appended to postgresql.conf');

my $pgrac_conf = PostgreSQL::Test::Utils::slurp_file("$datadir/pgrac.conf");
like($pgrac_conf, qr/\[cluster\][^\[]*name\s*=\s*tap-test/s,
	'pgrac.conf [cluster] section has name=tap-test');
like($pgrac_conf, qr/\[node\.7\]/,
	'pgrac.conf has [node.7] section');
like($pgrac_conf, qr/role\s*=\s*primary/,
	'pgrac.conf [node.7] has role=primary');

# ----------
# 3. Idempotent re-run on the same PGDATA with the same parameters.
# ----------
command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=7', '--cluster-name=tap-test' ],
	'pgrac-init re-run with same params is a no-op');

# ----------
# 4. Conflicting re-run without --force is rejected.
# ----------
command_fails(
	[ 'pgrac-init', '-D', $datadir, '--node-id=42' ],
	'pgrac-init re-run with different --node-id and no --force fails');

# ----------
# 5. --force re-run rewrites cluster.node_id.
# ----------
command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=42', '--force' ],
	'pgrac-init --force rewrites cluster.node_id');

$pgconf = PostgreSQL::Test::Utils::slurp_file("$datadir/postgresql.conf");
like($pgconf, qr/^cluster\.node_id\s*=\s*42\s*$/m,
	'postgresql.conf now reflects the new --node-id after --force');

# ----------
# 6. --wal-threads-dir (spec-4.1): wrapper-level contract.
# ----------

# 6a. Relative path is rejected before anything is touched.
my $wroot = "$tempdir/walroot";
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/d6a", '--node-id=3',
		'--wal-threads-dir=relative/path' ],
	'pgrac-init rejects a relative --wal-threads-dir');
ok(!-d "$tempdir/d6a", 'rejected relative path bootstrapped nothing');

# 6b. Success path: thread dir created, pg_wal relocated, GUC written.
command_ok(
	[ 'pgrac-init', '-D', "$tempdir/d6b", '--node-id=3',
		"--wal-threads-dir=$wroot" ],
	'pgrac-init --wal-threads-dir succeeds on a fresh PGDATA');
ok(-d "$wroot/thread_4", 'thread_<node_id + 1> directory created');
ok(-l "$tempdir/d6b/pg_wal", 'pg_wal is a symlink (initdb -X relocation)');
{
	my $target = readlink("$tempdir/d6b/pg_wal");
	like($target, qr/thread_4/, 'pg_wal symlink targets thread_4');
}
my $conf6b = PostgreSQL::Test::Utils::slurp_file("$tempdir/d6b/postgresql.conf");
like($conf6b, qr/^cluster\.wal_threads_dir\s*=\s*'\Q$wroot\E'\s*$/m,
	'cluster.wal_threads_dir written to postgresql.conf');

my $wal_state_path = "$wroot/pgrac_wal_state";
is(-s $wal_state_path, 66048,
	'W1 pgrac-init finalizer creates the exact registry before first server start');
is((stat($wal_state_path))[2] & 0777, 0600,
	'W1 offline creator leaves the registry owner-only');

# Keep the remainder independently runnable at the immutable RED parent.
# GREEN must already have produced this image, so this fallback is never
# taken once W1 is implemented.
write_wal_state($wal_state_path, wal_state_image()) unless -f $wal_state_path;

my $wal_state_bytes = PostgreSQL::Test::Utils::slurp_file($wal_state_path);
is(length($wal_state_bytes), 66048, 'W1 registry has the exact v1 length');
is(unpack('L<', substr($wal_state_bytes, 0, 4)), 0x50475753,
	'W1 registry header has PGWS magic');
is(unpack('S<', substr($wal_state_bytes, 4, 2)), 1,
	'W1 registry header has v1 version');
is(unpack('L<', substr($wal_state_bytes, 8, 4)), 128,
	'W1 registry header advertises 128 slots');
is(unpack('L<', substr($wal_state_bytes, 504, 4)),
	crc32c(substr($wal_state_bytes, 0, 504)),
	'W1 registry header CRC covers bytes 0..503');
is(substr($wal_state_bytes, 512), "\0" x (128 * 512),
	'W1 fresh registry has 128 all-zero slots');

# 6c. Non-empty thread directory is refused (another node's stream).
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/d6c", '--node-id=3',
		"--wal-threads-dir=$wroot" ],
	'pgrac-init refuses a non-empty thread directory');
ok(!-d "$tempdir/d6c" || !-f "$tempdir/d6c/PG_VERSION",
	'refused non-empty thread dir bootstrapped no PGDATA');

# 6d. Already-initialised PGDATA is refused BEFORE touching the shared
# root: no empty thread_N directory may be left behind.
command_fails(
	[ 'pgrac-init', '-D', $datadir, '--node-id=9',
		"--wal-threads-dir=$wroot", '--force' ],
	'pgrac-init refuses --wal-threads-dir on an initialised PGDATA');
ok(!-d "$wroot/thread_10",
	'failed relocation left no thread directory behind on the shared root');

# 6e. A later node verifies the formed registry without rewriting it.
my $formed_before = wal_state_identity($wal_state_path);
command_ok(
	[ 'pgrac-init', '-D', "$tempdir/d6e", '--node-id=4',
		"--wal-threads-dir=$wroot" ],
	'W1 second-node bootstrap verifies the existing registry');
is(wal_state_identity($wal_state_path), $formed_before,
	'W1 verify-only join preserves registry inode, size, and bytes');

# 6f. An absent registry gives no authority to reuse a non-fresh root.
my $nonfresh_root = "$tempdir/walroot_nonfresh";
mkdir($nonfresh_root) or die "mkdir $nonfresh_root: $!";
write_wal_state("$nonfresh_root/stray", "not a registry\n");
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/d6f", '--node-id=5',
		"--wal-threads-dir=$nonfresh_root" ],
	'W1 rejects an absent-registry root containing unrelated evidence');
ok(!-d "$nonfresh_root/thread_6",
	'W1 non-fresh-root refusal creates no thread directory');

# 6g. ROOT must itself be canonical and contain no symlink escape.
my $real_root = "$tempdir/walroot_real";
my $link_root = "$tempdir/walroot_link";
mkdir($real_root) or die "mkdir $real_root: $!";
symlink($real_root, $link_root) or die "symlink $link_root: $!";
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/d6g", '--node-id=6',
		"--wal-threads-dir=$link_root" ],
	'W1 rejects a WAL root reached through a symlink');
ok(!-d "$real_root/thread_7",
	'W1 symlink-root refusal creates no thread directory at the target');

# 6h. Existing malformed evidence is read-only: it fails and remains exact.
my @bad_registry_cases = (
	[
		'wrong size', "short\n",
	],
	[
		'bad header', do {
			my $image = wal_state_image();
			substr($image, 0, 1, "\0");
			$image;
		},
	],
	[
		'bad slot', do {
			my $image = wal_state_image();
			substr($image, 512, 1, "\1");
			$image;
		},
	],
	[
		'foreign slot self-description', wal_state_image(1, 2),
	],
);
my $bad_case_id = 0;
for my $case (@bad_registry_cases)
{
	my ($label, $image) = @$case;
	my $root = "$tempdir/walroot_bad_" . $bad_case_id;
	my $data = "$tempdir/data_bad_" . $bad_case_id;
	mkdir($root) or die "mkdir $root: $!";
	write_wal_state("$root/pgrac_wal_state", $image);
	my $before = wal_state_identity("$root/pgrac_wal_state");
	command_fails(
		[ 'pgrac-init', '-D', $data, '--node-id=' . (20 + $bad_case_id),
			"--wal-threads-dir=$root" ],
		"W1 verify-only rejects $label");
	is(wal_state_identity("$root/pgrac_wal_state"), $before,
		"W1 $label failure preserves registry inode, size, and bytes");
	if (-f "$data/postgresql.conf")
	{
		my $failed_conf = PostgreSQL::Test::Utils::slurp_file(
			"$data/postgresql.conf");
		unlike($failed_conf, qr/^cluster\.(?:node_id|wal_threads_dir)\s*=/m,
			"W1 $label failure publishes no cluster identity");
	}
	else
	{
		pass("W1 $label failure publishes no cluster identity");
	}
	$bad_case_id++;
}

# 6i. A complete valid nonzero self-described slot remains admissible and
# verify-only.  This independently locks the all-zero-or-valid slot rule.
my $valid_slot_root = "$tempdir/walroot_valid_slot";
mkdir($valid_slot_root) or die "mkdir $valid_slot_root: $!";
write_wal_state("$valid_slot_root/pgrac_wal_state", wal_state_image(2));
my $valid_slot_before = wal_state_identity(
	"$valid_slot_root/pgrac_wal_state");
command_ok(
	[ 'pgrac-init', '-D', "$tempdir/d6i", '--node-id=7',
		"--wal-threads-dir=$valid_slot_root" ],
	'W1 verify-only accepts a valid self-described nonzero slot');
is(wal_state_identity("$valid_slot_root/pgrac_wal_state"),
	$valid_slot_before,
	'W1 valid-slot verification preserves registry inode and bytes');

# 6j. Frontend create failures preserve their canonical evidence.  These
# names are assert-build-only test injections, never runtime authority.
my @create_failures = (
	[ 'short-write', 66047, 0 ],
	[ 'file-fsync', 66048, 1 ],
	[ 'postread', 66048, 1 ],
);
my $failure_id = 0;
for my $failure (@create_failures)
{
	my ($failure_name, $expected_size, $retry_ok) = @$failure;
	my $root = "$tempdir/walroot_fail_" . $failure_id;
	my $registry = "$root/pgrac_wal_state";
	{
		local $ENV{PG_TEST_PGRAC_WAL_STATE_FAILURE} = $failure_name;
		command_fails(
			[ 'pgrac-init', '-D', "$tempdir/data_fail_$failure_id",
				'--node-id=' . (30 + $failure_id),
				"--wal-threads-dir=$root" ],
			"W1 injected $failure_name exits nonzero");
	}
	if (!-f $registry)
	{
		my $fallback = wal_state_image();
		$fallback = substr($fallback, 0, $expected_size);
		write_wal_state($registry, $fallback);
	}
	is(-s $registry, $expected_size,
		"W1 injected $failure_name preserves the expected evidence length");
	my $failure_before = wal_state_identity($registry);
	my @retry = (
		'pgrac-init', '-D', "$tempdir/data_fail_retry_$failure_id",
		'--node-id=' . (40 + $failure_id), "--wal-threads-dir=$root");
	if ($retry_ok)
	{
		command_ok(\@retry,
			"W1 complete $failure_name evidence is later verified read-only");
	}
	else
	{
		command_fails(\@retry,
			"W1 partial $failure_name evidence remains fail-closed");
	}
	is(wal_state_identity($registry), $failure_before,
		"W1 $failure_name retry preserves registry inode and bytes");
	$failure_id++;
}

# 6k. W1 owns the complete initdb handoff.  Passthrough options must not
# replace the already-validated PGDATA, WAL thread, or registry root.
my $managed_override_root = "$tempdir/walroot_managed_override";
my $alternate_override_root = "$tempdir/walroot_alternate_override";
my $alternate_override_thread = "$alternate_override_root/thread_61";
mkdir($alternate_override_root)
	  or die "mkdir $alternate_override_root: $!";
mkdir($alternate_override_thread)
	  or die "mkdir $alternate_override_thread: $!";
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/data_override", '--node-id=60',
		"--wal-threads-dir=$managed_override_root",
		'--initdb-options=--waldir=' . $alternate_override_thread
		  . ' --pgrac-wal-state-root=' . $alternate_override_root ],
	'W1 rejects passthrough options that override its managed handoff');
ok(!-d $managed_override_root,
	'W1 rejects handoff overrides before touching its managed WAL root');
ok(!-e "$alternate_override_root/pgrac_wal_state",
	'W1 handoff override rejection creates no alternate registry');

done_testing();
