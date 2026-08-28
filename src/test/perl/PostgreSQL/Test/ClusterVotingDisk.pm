#-------------------------------------------------------------------------
#
# ClusterVotingDisk.pm
#	  Test-only formatter for the frozen pgrac voting-disk member layout.
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterVotingDisk;

use strict;
use warnings;

use Exporter 'import';

our @EXPORT_OK = qw(format_voting_file);

# A voting backing file can later become the exact Linux block-device
# authority used by the Stage-8 two-phase harness.  Allocate the final PGRD
# extent from creation; growing a file after loop attachment does not update
# the loop device's attested capacity.
use constant VOTING_FILE_BYTES_MIN => 525824;
use constant VOTING_SLOT_BYTES => 512;
use constant VOTING_SLOT_CRC_OFFSET => 508;
use constant VOTING_SLOT_MAGIC => 0x51564F54;
use constant VOTING_SLOT_VERSION => 1;
use constant CLUSTER_MAX_NODES => 128;

sub _crc32c
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

sub format_voting_file
{
	my ($path, $disk_index, $max_nodes) = @_;
	$max_nodes //= CLUSTER_MAX_NODES;

	die 'voting disk path is required' unless defined $path && length $path;
	die 'voting disk index must be a non-negative integer'
	  unless defined $disk_index && $disk_index =~ /^\d+$/;
	die 'voting disk max_nodes must be between 1 and 128'
	  unless $max_nodes =~ /^\d+$/ && $max_nodes >= 1 && $max_nodes <= CLUSTER_MAX_NODES;

	open(my $fh, '>:raw', $path) or die "open $path: $!";
	for my $node_id (0 .. $max_nodes - 1)
	{
		my $slot = "\0" x VOTING_SLOT_BYTES;
		substr($slot, 0, 12, pack('V3', VOTING_SLOT_MAGIC, VOTING_SLOT_VERSION,
			$node_id));
		substr($slot, 48, 4, pack('V', $disk_index));
		substr($slot, VOTING_SLOT_CRC_OFFSET, 4,
			pack('V', _crc32c(substr($slot, 0, VOTING_SLOT_CRC_OFFSET))));
		print {$fh} $slot or die "write $path: $!";
	}
	truncate($fh, VOTING_FILE_BYTES_MIN) or die "truncate $path: $!";
	close($fh) or die "close $path: $!";
	return;
}

1;
