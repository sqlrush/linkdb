#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 276_rl02_death_legs.pl
#    RF-ROOT P9 RL-02/04 (faithful fault legs): death-of-recoverer /
#    kill-the-replay-actor legs, honestly SKIP-with-reason.
#
#    RL-02/03/04 semantics: the replay actor (thread recovery) must be
#    killed at specific mutation points and the next actor must re-verify
#    from canonical sources (never adopt private progress).  Under fence
#    The production IPMI provider is selected, but no certified real
#    target/protected-set fixture is configured in the generic TAP farm.
#    Therefore the replay actor NEVER reaches mutation: it stops BLOCKED at
#    NeedSet/admit with zero GES/replay/publish.  There is no recoverer to kill
#    in this environment — the leg is honestly SKIP-with-reason, never a mock
#    PASS or a substitute for deployment certification.
#
#    What IS covered by other legs:
#      - t/271 L4: the survivor's first recovery never executes the
#        replay actor (fence provider-0 BLOCKED), asserted against the
#        live log — the honest observable half of RL-02/04.
#      - RL-03 (stable-base) is separately gated on STOP-RF-PAGE-STABLE-
#        BASE (mutation=0 by contract).
#      - Once a certified real target/protected-set fixture lets the replay
#        actor reach mutation, RL-02/04's kill-and-recover legs become
#        constructible.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: RF-ROOT §9.2 RL-02/04 (Stage 8 contract)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use Test::More;

my $reason = 'RL-02/04 death-of-recoverer legs: no certified real IPMI '
	. 'target/protected-set fixture is configured, so the replay actor never '
	. 'reaches mutation (NeedSet/admit BLOCKED), '
	. 'so there is no recoverer to kill — honest SKIP-with-reason per '
	. 'contract; the observable half is covered by t/271 L4 (replay actor '
	. 'did not execute, zero GES/replay/publish).  This skip is not deployment '
	. 'certification and cannot contribute to formal GREEN.';

plan skip_all => $reason;
