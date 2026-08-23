#!/usr/bin/env python3
"""Fail-closed verifier for R11PredecessorProductManifestV1."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import pathlib
import re
import shlex
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
PATHSPECS = ["src/backend/**", "src/include/**", "src/test/**", "src/tools/**"]
ATTESTATIONS = [
    "clean_checkout",
    "no_skip",
    "no_timeout",
    "no_forced_cancel",
    "no_judge_change",
]
ANCHORS = ["producer", "consumer", "observable", "negative_counterexample"]
EXECUTION_STATUS = {
    "red_r6": 1,
    "red_r7": 1,
    "red_r8": 1,
    "red_r9": 1,
    "red_r10": 1,
    "configure": 0,
    "full_build": 0,
    "build_units": 0,
    "green_r6_identity": 0,
    "green_r7_retry": 0,
    "green_r8_r10_pcm_lock": 0,
    "green_r9_r10_gcs_block": 0,
    "green_r10_wire": 0,
    "green_r10_lms": 0,
}
EXECUTION_TAP_PLANS = {
    "green_r6_identity": 11,
    "green_r7_retry": 13,
    "green_r8_r10_pcm_lock": 122,
    "green_r9_r10_gcs_block": 128,
    "green_r10_wire": 21,
    "green_r10_lms": 32,
}


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


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ManifestError(f"cannot read execution evidence: {path}: {error}") from error
    return digest.hexdigest()


def _load_execution_artifact(
    source_root: pathlib.Path, manifest: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    reference = manifest.get("execution_artifact")
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise ManifestError("execution artifact reference is incomplete")
    relative = reference["path"]
    if not isinstance(relative, str) or not relative.startswith("src/test/"):
        raise ManifestError("execution artifact path is outside product verification data")
    path = source_root / relative
    if _sha256_file(path) != reference["sha256"]:
        raise ManifestError("execution artifact hash mismatch")
    artifact = _load_manifest(path)
    if artifact.get("schema_version") != "R11PredecessorExecutionV1":
        raise ManifestError("execution artifact schema is stale")
    if artifact.get("product_commit") != PRODUCT_COMMIT or artifact.get("product_tree") != PRODUCT_TREE:
        raise ManifestError("execution artifact product identity mismatch")
    tool_hash = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    if artifact.get("capture_tool_sha256") != tool_hash:
        raise ManifestError("execution artifact capture-tool hash mismatch")
    records = artifact.get("records")
    if not isinstance(records, list):
        raise ManifestError("execution artifact records are absent")
    by_key: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise ManifestError("execution record is not an object")
        required = {
            "command",
            "cwd",
            "exit_status",
            "key",
            "log_bytes",
            "log_path",
            "log_sha256",
            "output_sha256",
            "tap_not_ok",
            "tap_ok",
            "tap_plan",
        }
        if set(record) != required:
            raise ManifestError("execution record fields are stale or incomplete")
        key = record["key"]
        if key not in EXECUTION_STATUS or key in by_key:
            raise ManifestError(f"unexpected or duplicate execution record: {key}")
        if record["exit_status"] != EXECUTION_STATUS[key]:
            raise ManifestError(f"execution record has wrong real exit status: {key}")
        log_relative = record["log_path"]
        if not isinstance(log_relative, str) or not log_relative.startswith("src/test/"):
            raise ManifestError(f"execution log path is unsafe: {key}")
        log_path = source_root / log_relative
        if _sha256_file(log_path) != record["log_sha256"]:
            raise ManifestError(f"execution log hash mismatch: {key}")
        try:
            output = gzip.decompress(log_path.read_bytes())
        except (OSError, EOFError) as error:
            raise ManifestError(f"execution log is not valid gzip: {key}") from error
        if len(output) != record["log_bytes"] or hashlib.sha256(output).hexdigest() != record["output_sha256"]:
            raise ManifestError(f"execution log payload mismatch: {key}")
        text = output.decode("utf-8", errors="replace")
        plan_match = re.search(r"(?m)^1\.\.(\d+)\s*$", text)
        tap_plan = int(plan_match.group(1)) if plan_match else 0
        tap_ok = len(re.findall(r"(?m)^ok \d+ - ", text))
        tap_not_ok = len(re.findall(r"(?m)^not ok \d+ - ", text))
        if (record["tap_plan"], record["tap_ok"], record["tap_not_ok"]) != (
            tap_plan,
            tap_ok,
            tap_not_ok,
        ):
            raise ManifestError(f"execution TAP summary mismatch: {key}")
        expected_plan = EXECUTION_TAP_PLANS.get(key)
        if expected_plan is not None and (
            tap_plan != expected_plan or tap_ok != expected_plan or tap_not_ok != 0
        ):
            raise ManifestError(f"execution TAP result is not exact GREEN: {key}")
        if expected_plan is not None and re.search(r"(?mi)^.*#\s*SKIP\b", text):
            raise ManifestError(f"execution log contains SKIP: {key}")
        by_key[key] = dict(record)
    if set(by_key) != set(EXECUTION_STATUS):
        raise ManifestError("execution artifact does not contain the exact command set")
    return by_key


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
    executions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise ManifestError(f"{field} must be an object")
    required = {
        "command",
        "execution_key",
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
    execution_key = evidence["execution_key"]
    if execution_key not in executions:
        raise ManifestError(f"{field} execution record is absent")
    execution = executions[execution_key]
    if (
        execution["command"] != evidence["command"]
        or execution["exit_status"] != evidence["exit_status"]
    ):
        raise ManifestError(f"{field} does not match its real execution record")
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

    executions = _load_execution_artifact(source_root, manifest)

    shared = manifest.get("shared_green_evidence")
    if not isinstance(shared, list) or len(shared) < 1:
        raise ManifestError("shared GREEN evidence is absent")
    shared_report = [
        _verify_evidence(
            source_root, PRODUCT_COMMIT, item, "shared GREEN", 0, executions
        )
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
            source_root,
            PRODUCT_COMMIT,
            row.get("red_evidence"),
            f"{row_id} RED",
            1,
            executions,
        )
        green = row.get("green_evidence")
        if not isinstance(green, list) or not green:
            raise ManifestError(f"{row_id}: GREEN evidence is absent")
        green_report = [
            _verify_evidence(
                source_root,
                PRODUCT_COMMIT,
                item,
                f"{row_id} GREEN",
                0,
                executions,
            )
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


def _capture_command(
    source_root: pathlib.Path,
    execution_root: pathlib.Path,
    output_dir: pathlib.Path,
    key: str,
    argv: list[str],
    cwd: pathlib.Path,
    env_update: dict[str, str] | None = None,
    command_display: str | None = None,
) -> dict[str, Any]:
    environment = os.environ.copy()
    if env_update:
        environment.update(env_update)
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output = result.stdout
    log_path = output_dir / f"{key}.log.gz"
    log_path.write_bytes(gzip.compress(output, compresslevel=9, mtime=0))
    text = output.decode("utf-8", errors="replace")
    plan_match = re.search(r"(?m)^1\.\.(\d+)\s*$", text)
    relative_log = log_path.relative_to(source_root).as_posix()
    record = {
        "command": command_display or shlex.join(argv),
        "cwd": cwd.relative_to(execution_root).as_posix() or ".",
        "exit_status": result.returncode,
        "key": key,
        "log_bytes": len(output),
        "log_path": relative_log,
        "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
        "output_sha256": hashlib.sha256(output).hexdigest(),
        "tap_not_ok": len(re.findall(r"(?m)^not ok \d+ - ", text)),
        "tap_ok": len(re.findall(r"(?m)^ok \d+ - ", text)),
        "tap_plan": int(plan_match.group(1)) if plan_match else 0,
    }
    expected = EXECUTION_STATUS[key]
    if result.returncode != expected:
        raise ManifestError(
            f"capture command {key} returned {result.returncode}, expected {expected}; "
            f"see {relative_log}"
        )
    return record


def capture_execution_artifact(
    source_root: pathlib.Path,
    execution_root: pathlib.Path,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    source_root = source_root.resolve()
    execution_root = execution_root.resolve()
    output_dir = output_dir.resolve()
    try:
        output_dir.relative_to(source_root)
    except ValueError as error:
        raise ManifestError("capture output directory must be inside source root") from error
    if _git(execution_root, "rev-parse", "HEAD") != PRODUCT_COMMIT:
        raise ManifestError("capture root is not the exact predecessor product commit")
    if _git(execution_root, "rev-parse", "HEAD^{tree}") != PRODUCT_TREE:
        raise ManifestError("capture root is not the exact predecessor product tree")
    if _git(execution_root, "status", "--porcelain", "--untracked-files=no"):
        raise ManifestError("capture root has tracked modifications")
    if (execution_root / "GNUmakefile").exists():
        raise ManifestError("capture root is not a fresh unconfigured checkout")
    output_dir.mkdir(parents=True, exist_ok=True)

    ranges = {
        "red_r6": (
            "0471e307462c7ccc64287f698259e863cc2dc7ec",
            "44222f69ca254626b78283396083eab00809c3f8",
        ),
        "red_r7": (
            "44222f69ca254626b78283396083eab00809c3f8",
            "33b53695036c0c9bca17327535b8bfb9e3d7db5a",
        ),
        "red_r8": (
            "14751227f6fbc0926761f580562329e97c4efcd9",
            "38f3963bd0e2df0254cd9d382cc61475f924aaca",
        ),
        "red_r9": (
            "33b53695036c0c9bca17327535b8bfb9e3d7db5a",
            "14751227f6fbc0926761f580562329e97c4efcd9",
        ),
        "red_r10": (
            "38f3963bd0e2df0254cd9d382cc61475f924aaca",
            "5bb8e6d85bece75672fd490c480c163504cb9bf7",
        ),
    }
    records: list[dict[str, Any]] = []
    for key, (base, head) in ranges.items():
        records.append(
            _capture_command(
                source_root,
                execution_root,
                output_dir,
                key,
                ["git", "diff", "--exit-code", f"{base}..{head}", "--", *PATHSPECS],
                execution_root,
            )
        )

    install_prefix = execution_root.parent / "install"
    configure_args = [
        f"--prefix={install_prefix}",
        "--with-openssl",
        "--with-icu",
        "--with-lz4",
        "--with-zstd",
        "--enable-cluster",
        "--enable-injection-points",
        "--enable-tap-tests",
        "--enable-cassert",
    ]
    env_update = {
        "PKG_CONFIG_PATH": "/opt/homebrew/opt/icu4c@78/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:/opt/homebrew/opt/lz4/lib/pkgconfig:/opt/homebrew/opt/zstd/lib/pkgconfig",
        "LDFLAGS": "-L/opt/homebrew/opt/openssl@3/lib",
        "CPPFLAGS": "-I/opt/homebrew/opt/openssl@3/include",
    }
    env_display = " ".join(f"{name}={shlex.quote(value)}" for name, value in env_update.items())
    configure_command = f"env {env_display} {shlex.join(['./configure', *configure_args])}"
    records.append(
        _capture_command(
            source_root,
            execution_root,
            output_dir,
            "configure",
            ["./configure", *configure_args],
            execution_root,
            env_update,
            configure_command,
        )
    )
    records.append(
        _capture_command(
            source_root,
            execution_root,
            output_dir,
            "full_build",
            ["make", "-j8"],
            execution_root,
        )
    )
    unit_dir = execution_root / "src" / "test" / "cluster_unit"
    unit_targets = [
        "test_cluster_resource_x_identity",
        "test_cluster_resource_x_retry",
        "test_cluster_gcs_block",
        "test_cluster_pcm_lock",
        "test_cluster_resource_x_node_wire",
        "test_cluster_lms_outbound",
    ]
    records.append(
        _capture_command(
            source_root,
            execution_root,
            output_dir,
            "build_units",
            ["make", "-j8", *unit_targets],
            unit_dir,
        )
    )
    green_commands = [
        ("green_r6_identity", "test_cluster_resource_x_identity"),
        ("green_r7_retry", "test_cluster_resource_x_retry"),
        ("green_r8_r10_pcm_lock", "test_cluster_pcm_lock"),
        ("green_r9_r10_gcs_block", "test_cluster_gcs_block"),
        ("green_r10_wire", "test_cluster_resource_x_node_wire"),
        ("green_r10_lms", "test_cluster_lms_outbound"),
    ]
    for key, binary in green_commands:
        records.append(
            _capture_command(
                source_root,
                execution_root,
                output_dir,
                key,
                [f"./{binary}"],
                unit_dir,
            )
        )

    artifact = {
        "capture_tool_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
        "product_commit": PRODUCT_COMMIT,
        "product_tree": PRODUCT_TREE,
        "records": records,
        "schema_version": "R11PredecessorExecutionV1",
    }
    artifact_path = output_dir.parent / "r11-predecessor-execution-v1.json"
    artifact_path.write_text(canonical_json(artifact), encoding="utf-8")
    return artifact_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--capture-execution-root", type=pathlib.Path)
    parser.add_argument("--capture-output-dir", type=pathlib.Path)
    args = parser.parse_args()
    try:
        if args.capture_execution_root is not None or args.capture_output_dir is not None:
            if args.capture_execution_root is None or args.capture_output_dir is None:
                raise ManifestError("both capture paths are required")
            artifact = capture_execution_artifact(
                args.source_root, args.capture_execution_root, args.capture_output_dir
            )
            print(artifact)
            return 0
        if args.manifest is None:
            raise ManifestError("--manifest is required for audit")
        report = audit_manifest(args.source_root, args.manifest)
    except ManifestError as error:
        print(f"R11 predecessor manifest RED: {error}", file=sys.stderr)
        return 1
    print(canonical_json(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
