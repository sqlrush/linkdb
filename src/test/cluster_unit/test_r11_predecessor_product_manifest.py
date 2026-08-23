#!/usr/bin/env python3
"""Unit tests for the immutable R11 predecessor product manifest."""

from __future__ import annotations

import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
CHECKER_PATH = ROOT / "src" / "tools" / "check_r11_predecessor_product_manifest.py"
MANIFEST_PATH = (
    ROOT
    / "src"
    / "test"
    / "cluster_unit"
    / "data"
    / "r11-predecessor-product-manifest-v1.json"
)
SPEC = importlib.util.spec_from_file_location("r11_predecessor_manifest", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {CHECKER_PATH}")
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class R11PredecessorProductManifestTests(unittest.TestCase):
    def _manifest(self) -> dict[str, object]:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def _audit_mutation(self, mutation) -> None:
        manifest = copy.deepcopy(self._manifest())
        mutation(manifest)
        with tempfile.TemporaryDirectory() as tempdir:
            path = pathlib.Path(tempdir) / "manifest.json"
            path.write_text(CHECKER.canonical_json(manifest), encoding="utf-8")
            CHECKER.audit_manifest(ROOT, path)

    def test_exact_five_row_manifest_is_green_on_one_tree(self) -> None:
        report = CHECKER.audit_manifest(ROOT, MANIFEST_PATH)
        self.assertEqual([row["predecessor_id"] for row in report["rows"]], CHECKER.REQUIRED_IDS)
        self.assertEqual(report["product_commit"], CHECKER.PRODUCT_COMMIT)
        self.assertEqual(report["product_tree"], CHECKER.PRODUCT_TREE)
        manifest = self._manifest()
        self.assertIn(
            "src/tools/check_pcm_x_write_gate_ast.py",
            manifest["rows"][2]["source_paths"],
        )
        self.assertIn(
            "src/test/cluster_unit/test_resource_x_send_c1.py",
            manifest["rows"][4]["source_paths"],
        )

    def test_execution_artifact_hash_mismatch_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "execution artifact hash"):
            self._audit_mutation(
                lambda manifest: manifest["execution_artifact"].__setitem__(
                    "sha256", "0" * 64
                )
            )

    def test_manifest_command_cannot_override_real_execution(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "real execution record"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][0]["green_evidence"][0].__setitem__(
                    "command", "true"
                )
            )

    def test_red_evidence_cannot_be_a_source_diff(self) -> None:
        evidence = copy.deepcopy(self._manifest()["rows"][0]["red_evidence"])
        evidence["command"] = "git diff --exit-code base..head -- src/backend"
        execution_key = evidence["execution_key"]
        execution = {
            "command": evidence["command"],
            "exit_status": evidence["exit_status"],
        }
        with self.assertRaisesRegex(CHECKER.ManifestError, "behavioral replay"):
            CHECKER._verify_evidence(
                ROOT,
                CHECKER.PRODUCT_COMMIT,
                evidence,
                "R6 RED",
                1,
                {execution_key: execution},
            )

    def test_red_evidence_requires_exact_parent_and_test_artifact(self) -> None:
        evidence = copy.deepcopy(self._manifest()["rows"][0]["red_evidence"])
        evidence["command"] = "make -s test_cluster_pcm_x_convert"
        execution_key = evidence["execution_key"]
        execution = {
            "command": evidence["command"],
            "exit_status": evidence["exit_status"],
        }
        with self.assertRaisesRegex(CHECKER.ManifestError, "exact parent replay"):
            CHECKER._verify_evidence(
                ROOT,
                CHECKER.PRODUCT_COMMIT,
                evidence,
                "R6 RED",
                1,
                {execution_key: execution},
            )

    def test_spec_hash_mismatch_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "normative body"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][0].__setitem__("spec_body_sha256", "0" * 64)
            )

    def test_product_tree_mismatch_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "product tree"):
            self._audit_mutation(
                lambda manifest: manifest.__setitem__("product_tree", "0" * 40)
            )

    def test_non_green_row_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "GREEN"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][2].__setitem__("status", "PROVISIONAL")
            )

    def test_missing_source_path_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "source path"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][1]["source_paths"].append(
                    "src/backend/cluster/not_present.c"
                )
            )

    def test_missing_anchor_symbol_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "anchor"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][3]["anchors"].__setitem__(
                    "consumer", "src/backend/cluster/cluster_pcm_lock.c::not_present"
                )
            )

    def test_wrong_artifact_blob_hash_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "artifact blob"):
            self._audit_mutation(
                lambda manifest: manifest["rows"][4]["green_evidence"][0].__setitem__(
                    "artifact_blob_sha256", "0" * 64
                )
            )

    def test_false_execution_attestation_is_rejected(self) -> None:
        with self.assertRaisesRegex(CHECKER.ManifestError, "attestation"):
            self._audit_mutation(
                lambda manifest: manifest["execution_attestation"].__setitem__(
                    "no_judge_change", False
                )
            )


if __name__ == "__main__":
    unittest.main()
