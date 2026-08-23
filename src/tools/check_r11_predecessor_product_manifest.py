#!/usr/bin/env python3
"""Fail-closed verifier for R11PredecessorProductManifestV1."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from typing import Any


SCHEMA = "R11PredecessorProductManifestV1"
REQUIRED_IDS = ["R6", "R7", "R8", "R9", "R10"]
SPEC_BODY_SHA256 = {
    "R6": "880579f3ceb73f5e86c706924243c87b2e9c43189c033528a6c77678e6980663",
    "R7": "8a5d6d2ba67d70c0cfecc52d2d7ece186685c78a134cd3f139f855d11b9c4a18",
    "R8": "ffa0e269a59f82b718731ffbe56484df30f0879d4baac4cf463ac55136e99784",
    "R9": "1729c7674070bd55589aa1e6d15c8d2b5f9c86abcdaf0c28708d3f556fad0b7a",
    "R10": "5ffb335a1ce40a8c80e84d45711cb8015b5db343fad1d43527cdd66e0156ddf6",
}
PRODUCT_COMMIT = "c283d28b37390318539c048665d3338eac458a43"
PRODUCT_TREE = "3fd3e3a60a18c27ffa0a78747967d407ad2b5b5c"
PATHSPECS = ["src/backend/**", "src/include/**"]
ATTESTATIONS = [
    "clean_checkout",
    "no_skip",
    "no_timeout",
    "no_forced_cancel",
    "no_judge_change",
]
ANCHORS = ["producer", "consumer", "observable", "negative_counterexample"]


class ManifestError(RuntimeError):
    """The immutable predecessor product manifest is incomplete or stale."""


def canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _git(source_root: pathlib.Path, *args: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(source_root), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", b"")
        if isinstance(detail, bytes):
            detail = detail.decode("utf-8", errors="replace")
        raise ManifestError(f"git evidence lookup failed: {detail.strip()}") from error
    if binary:
        return result.stdout
    return result.stdout.decode("utf-8").strip()


def _load_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"cannot read predecessor manifest: {error}") from error
    if not isinstance(value, dict):
        raise ManifestError("predecessor manifest root must be an object")
    if raw != canonical_json(value):
        raise ManifestError("predecessor manifest is not canonical JSON")
    return value


def _blob(source_root: pathlib.Path, commit: str, path: str) -> bytes:
    if not path.startswith("src/") or ".." in pathlib.PurePosixPath(path).parts:
        raise ManifestError(f"unsafe source path: {path}")
    return _git(source_root, "show", f"{commit}:{path}", binary=True)  # type: ignore[return-value]


def _path_symbol(value: object, field: str) -> tuple[str, str]:
    if not isinstance(value, str) or value.count("::") != 1:
        raise ManifestError(f"{field} must be path::symbol")
    path, symbol = value.split("::")
    if not path or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", symbol):
        raise ManifestError(f"malformed {field}")
    return path, symbol


def _verify_anchor(
    source_root: pathlib.Path,
    commit: str,
    value: object,
    field: str,
    source_paths: set[str],
) -> tuple[str, str]:
    path, symbol = _path_symbol(value, field)
    if path not in source_paths:
        raise ManifestError(f"{field} anchor path is outside the exact source path set: {path}")
    blob = _blob(source_root, commit, path)
    text = blob.decode("utf-8", errors="replace")
    if re.search(rf"\b{re.escape(symbol)}\b", text) is None:
        raise ManifestError(f"{field} anchor symbol is absent: {value}")
    return path, symbol


def _verify_evidence(
    source_root: pathlib.Path,
    commit: str,
    evidence: object,
    field: str,
    expected_exit: int,
) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise ManifestError(f"{field} must be an object")
    required = {
        "command",
        "artifact",
        "artifact_blob_sha256",
        "exit_status",
        "behavior",
    }
    if set(evidence) != required:
        raise ManifestError(f"{field} evidence fields are stale or incomplete")
    if not isinstance(evidence["command"], str) or not evidence["command"].strip():
        raise ManifestError(f"{field} command is empty")
    if evidence["exit_status"] != expected_exit:
        raise ManifestError(f"{field} exit status must be {expected_exit}")
    if not isinstance(evidence["behavior"], str) or not evidence["behavior"].strip():
        raise ManifestError(f"{field} behavior is empty")
    path, symbol = _path_symbol(evidence["artifact"], f"{field} artifact")
    blob = _blob(source_root, commit, path)
    digest = hashlib.sha256(blob).hexdigest()
    if evidence["artifact_blob_sha256"] != digest:
        raise ManifestError(f"{field} artifact blob hash mismatch for {path}")
    text = blob.decode("utf-8", errors="replace")
    if re.search(rf"\b{re.escape(symbol)}\b", text) is None:
        raise ManifestError(f"{field} artifact anchor is absent: {symbol}")
    return dict(evidence)


def _exact_source_paths(
    source_root: pathlib.Path, base: str, head: str
) -> list[str]:
    output = _git(
        source_root,
        "diff",
        "--name-only",
        f"{base}..{head}",
        "--",
        *PATHSPECS,
    )
    return [] if not output else str(output).splitlines()


def _build_objects(source_paths: list[str]) -> list[str]:
    return sorted(
        path[:-2] + ".o"
        for path in source_paths
        if path.startswith("src/backend/") and path.endswith(".c")
    )


def audit_manifest(
    source_root: pathlib.Path, manifest_path: pathlib.Path
) -> dict[str, Any]:
    source_root = source_root.resolve()
    manifest = _load_manifest(manifest_path.resolve())
    if manifest.get("schema_version") != SCHEMA:
        raise ManifestError(f"schema_version must be {SCHEMA}")
    if manifest.get("product_commit") != PRODUCT_COMMIT:
        raise ManifestError("product commit does not match the frozen predecessor identity")
    if manifest.get("product_tree") != PRODUCT_TREE:
        raise ManifestError("product tree does not match the frozen predecessor identity")
    actual_tree = _git(source_root, "rev-parse", f"{PRODUCT_COMMIT}^{{tree}}")
    if actual_tree != PRODUCT_TREE:
        raise ManifestError("product tree cannot be reproduced from product commit")
    try:
        _git(source_root, "merge-base", "--is-ancestor", PRODUCT_COMMIT, "HEAD")
    except ManifestError as error:
        raise ManifestError("product commit is not an ancestor of the checked tree") from error

    compiler = manifest.get("compiler_fingerprint")
    if not isinstance(compiler, dict) or set(compiler) != {
        "architecture",
        "build_system",
        "compiler",
        "configure_args",
        "enable_cassert",
    }:
        raise ManifestError("compiler/configuration fingerprint is incomplete")
    if compiler["architecture"] != "aarch64" or compiler["build_system"] != "aarch64-apple-darwin25.6.0":
        raise ManifestError("compiler architecture fingerprint is stale")
    if not isinstance(compiler["compiler"], str) or "Apple clang version 21.0.0" not in compiler["compiler"]:
        raise ManifestError("compiler fingerprint is stale")
    if not isinstance(compiler["configure_args"], list) or not compiler["configure_args"]:
        raise ManifestError("configure argument fingerprint is empty")
    if compiler["enable_cassert"] is not True:
        raise ManifestError("clean predecessor build must enable cassert")

    attestation = manifest.get("execution_attestation")
    if not isinstance(attestation, dict) or set(attestation) != set(ATTESTATIONS):
        raise ManifestError("execution attestation is incomplete")
    if any(attestation[name] is not True for name in ATTESTATIONS):
        raise ManifestError("execution attestation must be exact and entirely true")

    shared = manifest.get("shared_green_evidence")
    if not isinstance(shared, list) or len(shared) < 1:
        raise ManifestError("shared GREEN evidence is absent")
    shared_report = [
        _verify_evidence(source_root, PRODUCT_COMMIT, item, "shared GREEN", 0)
        for item in shared
    ]

    rows = manifest.get("rows")
    if not isinstance(rows, list):
        raise ManifestError("predecessor rows are absent")
    ids = [row.get("predecessor_id") for row in rows if isinstance(row, dict)]
    if ids != REQUIRED_IDS:
        raise ManifestError("predecessor rows must be ordered exact R6,R7,R8,R9,R10")

    row_report: list[dict[str, Any]] = []
    for row in rows:
        row_id = row["predecessor_id"]
        if row.get("spec_body_sha256") != SPEC_BODY_SHA256[row_id]:
            raise ManifestError(f"{row_id}: normative body hash mismatch")
        if row.get("status") != "GREEN":
            raise ManifestError(f"{row_id}: predecessor status is not GREEN")
        implementation = row.get("implementation_range")
        if not isinstance(implementation, dict) or set(implementation) != {"base", "head"}:
            raise ManifestError(f"{row_id}: implementation range is incomplete")
        base = implementation["base"]
        head = implementation["head"]
        if not all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) for value in (base, head)):
            raise ManifestError(f"{row_id}: implementation range is malformed")
        try:
            _git(source_root, "merge-base", "--is-ancestor", head, PRODUCT_COMMIT)
        except ManifestError as error:
            raise ManifestError(f"{row_id}: implementation head is outside product commit") from error
        expected_paths = _exact_source_paths(source_root, base, head)
        source_paths = row.get("source_paths")
        if source_paths != sorted(set(expected_paths)):
            raise ManifestError(f"{row_id}: exact source path set mismatch")
        for path in source_paths:
            try:
                _blob(source_root, PRODUCT_COMMIT, path)
            except ManifestError as error:
                raise ManifestError(f"{row_id}: source path is absent from product tree: {path}") from error
        expected_objects = _build_objects(source_paths)
        if row.get("build_objects") != expected_objects:
            raise ManifestError(f"{row_id}: exact build-object path set mismatch")

        anchors = row.get("anchors")
        if not isinstance(anchors, dict) or set(anchors) != set(ANCHORS):
            raise ManifestError(f"{row_id}: anchor set is incomplete or unordered")
        path_set = set(source_paths)
        for name in ANCHORS:
            _verify_anchor(
                source_root,
                PRODUCT_COMMIT,
                anchors[name],
                f"{row_id} {name}",
                path_set,
            )
        red = _verify_evidence(
            source_root, PRODUCT_COMMIT, row.get("red_evidence"), f"{row_id} RED", 1
        )
        green = row.get("green_evidence")
        if not isinstance(green, list) or not green:
            raise ManifestError(f"{row_id}: GREEN evidence is absent")
        green_report = [
            _verify_evidence(source_root, PRODUCT_COMMIT, item, f"{row_id} GREEN", 0)
            for item in green
        ]
        if row.get("review_status") not in {
            "NOT_REQUIRED",
            "APPROVED",
            "USER_APPROVED_DEVIATIONS_RECORDED",
        }:
            raise ManifestError(f"{row_id}: reviewer-approved status is absent")
        row_report.append(
            {
                "predecessor_id": row_id,
                "source_path_count": len(source_paths),
                "build_object_count": len(expected_objects),
                "red_evidence": red,
                "green_evidence": green_report,
            }
        )

    return {
        "schema_version": SCHEMA,
        "product_commit": PRODUCT_COMMIT,
        "product_tree": PRODUCT_TREE,
        "shared_green_evidence": shared_report,
        "rows": row_report,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    args = parser.parse_args()
    try:
        report = audit_manifest(args.source_root, args.manifest)
    except ManifestError as error:
        print(f"R11 predecessor manifest RED: {error}", file=sys.stderr)
        return 1
    print(canonical_json(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
