#!/usr/bin/env python3
"""Focused static contract for the R11 source-removed build."""

from __future__ import annotations

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
BACKEND = ROOT / "src" / "backend"
INCLUDE = ROOT / "src" / "include"


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def production_text() -> str:
    chunks: list[str] = []
    for base in (BACKEND, INCLUDE):
        for path in sorted(base.rglob("*")):
            if path.suffix in {".c", ".h"}:
                chunks.append(f"\n/* {path.relative_to(ROOT)} */\n")
                chunks.append(path.read_text(encoding="utf-8"))
    return "".join(chunks)


class R11LegacySourceRemovedTests(unittest.TestCase):
    def test_convert_source_header_and_unit_are_absent(self) -> None:
        for relative in (
            "src/backend/cluster/cluster_pcm_x_convert.c",
            "src/include/cluster/cluster_pcm_x_convert.h",
            "src/test/cluster_unit/test_cluster_pcm_x_convert.c",
        ):
            self.assertFalse((ROOT / relative).exists(), relative)

    def test_build_manifests_do_not_name_convert_object_or_unit(self) -> None:
        for relative in (
            "src/backend/cluster/Makefile",
            "src/test/cluster_unit/Makefile",
        ):
            text = read(relative)
            self.assertFalse("cluster_pcm_x_convert.o" in text, relative)
            self.assertFalse("test_cluster_pcm_x_convert" in text, relative)

    def test_production_has_no_legacy_include_or_family_identity(self) -> None:
        text = production_text()
        forbidden = (
            r"cluster/cluster_pcm_x_convert\.h",
            r"\bPcmXTicket",
            r"\bPcmXLocal(?:Handle|Holder|Reliable|WriterClaim|Progress|Cutoff)",
            r"\bPcmXMaster(?:Admission|Ticket|Drive|Probe|Blocker|Wfg|Pending)",
            r"\bPcmX(?:DrainPoll|Retire|PeerFrontier|OutboundTargetFrontier)",
            r"\bcluster_pcm_x_local_",
            r"\bcluster_pcm_x_master_",
            r"\bcluster_pcm_x_(?:convert|retry_work|nested_wait)_",
            r"\bPcmXRuntimeSnapshot\b",
            r"\bPCM_X_RUNTIME_",
        )
        for pattern in forbidden:
            self.assertIsNone(re.search(pattern, text), pattern)

    def test_old_payload_semantics_are_absent(self) -> None:
        text = production_text()
        for name in (
            "PcmXEnqueuePayload",
            "PcmXPrehandleCancelPayload",
            "PcmXAdmitAckPayload",
            "PcmXPhasePayload",
            "PcmXRevokePayload",
            "PcmXGrantPayload",
            "PcmXInstallReadyPayload",
            "PcmXFinalAckPayload",
            "PcmXBlockerSetHeaderPayload",
            "PcmXBlockerChunkPayload",
            "PcmXDrainPollPayload",
            "PcmXRetirePayload",
        ):
            self.assertFalse(name in text, name)

    def test_source_wrapper_fallback_and_legacy_ticks_are_absent(self) -> None:
        text = production_text()
        for symbol in (
            "cluster_gcs_pcm_x_acquire_writer",
            "gcs_block_pcm_x_acquire_writer_impl",
            "cluster_gcs_block_pcm_x_formation_tick",
            "cluster_gcs_block_pcm_x_image_pump_tick",
            "cluster_gcs_pcm_x_terminal_kick",
            "cluster_gcs_pcm_x_blocker_probe_kick",
        ):
            self.assertFalse(symbol in text, symbol)

    def test_target_native_replacements_remain_positive(self) -> None:
        pcm_lock = read("src/backend/cluster/cluster_pcm_lock.c")
        gcs = read("src/backend/cluster/cluster_gcs_block.c")
        bufmgr = read("src/backend/storage/buffer/bufmgr.c")
        for anchor in (
            "cluster_pcm_lock_resource_x_bootstrap_request_exact",
            "cluster_pcm_lock_resource_x_bootstrap_round_step_exact",
            "cluster_pcm_lock_resource_x_assert_bootstrapped_exact",
            "cluster_pcm_lock_resource_x_requester_activate_exact",
        ):
            self.assertTrue(anchor in pcm_lock, anchor)
        self.assertTrue("cluster_gcs_resource_x_target_acquire_exact" in gcs)
        self.assertTrue("gcs_block_legacy_pcm_x_stale_ingress" in gcs)
        self.assertTrue("ResourceXWriterUseContext" in bufmgr)
        self.assertFalse("RESOURCE_X_WRITER_SOURCE" in bufmgr)

    def test_legacy_message_values_remain_reserved(self) -> None:
        envelope = read("src/include/cluster/cluster_ic_envelope.h")
        self.assertRegex(envelope, r"PGRAC_IC_MSG_PCM_X_ENQUEUE\s*=\s*41")
        self.assertRegex(envelope, r"PGRAC_IC_MSG_PCM_X_RETIRE_ACK\s*=\s*64")


if __name__ == "__main__":
    unittest.main()
