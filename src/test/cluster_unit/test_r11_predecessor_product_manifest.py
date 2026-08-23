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
