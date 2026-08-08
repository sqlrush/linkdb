#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 409_stage8_r4a_undo_block0.pl
#    Verify the R4A undo block-zero local identity core through its real
#    standalone unit-test executable.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/409_stage8_r4a_undo_block0.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original file.
#    Spec: spec-8.4a-undo-block0-authority-prerequisite.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Utils;
use Test::More;

my $source_root = "$FindBin::RealBin/../../../..";
my $unit_test = "$source_root/src/test/cluster_unit/test_cluster_undo_block0";

PostgreSQL::Test::Utils::command_like(
	[$unit_test],
	qr/^1\.\.8\n(?:ok [1-8] - [^\n]+\n){8}\# All 8 tests passed\.\n$/,
	'R4A local identity, generation and resident-state negatives pass');

done_testing();
