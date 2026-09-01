#!/usr/bin/env python3
"""Validate the bounded state space in spec-8.4D.

This checker intentionally uses only the Python standard library.  It treats
the JSON matrix as the source of truth, enumerates every declared authority
and composition combination, and applies the explicitly ordered rule list.
An optional public source tree verifies that frozen implementation anchors and
existing focused tests still exist.  It never modifies either repository.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "specs/matrices/spec-8.4d-authority-matrix-v2.json"
EXPECTED_MATRIX_ID = "S8-8.4D-CURRENT-MX-AUTHORITY-V2-CTRC"
EXPECTED_MATRIX_SHA256 = "7c56f3d686804297814043c0454fc8024a6a660ae22e156a10b44f46b144f90d"
EXPECTED_COUNTS = {
    "authority_combinations": 55296,
    "authority_rules_reached": 17,
    "composition_combinations": 13824,
    "composition_rules_reached": 17,
    "lifecycle_pairs": 117,
    "lifecycle_default_fail_closed_pairs": 102,
    "invariants": 22,
    "tests": 36,
    "code_contracts": 21,
    "ctrc_source_classes": 5,
    "ctrc_reference_kinds": 5,
    "ctrc_target_kinds": 4,
    "ctrc_publication_fields": 16,
    "ctrc_request_fields": 24,
    "ctrc_reply_header_fields": 16,
    "ctrc_ack_fields": 27,
    "ctrc_receipt_pairs": 49,
    "ctrc_receipt_default_fail_closed_pairs": 40,
    "ctrc_seal_pairs": 49,
    "ctrc_seal_default_fail_closed_pairs": 38,
    "ctrc_crash_cuts": 9,
    "ctrc_wal_fields": 18,
}


class MatrixError(Exception):
    """One or more normative matrix checks failed."""


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def require_unique(items: Iterable[str], label: str, errors: list[str]) -> None:
    values = list(items)
    duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
    if duplicates:
        fail(errors, f"{label} contains duplicates: {', '.join(duplicates)}")


def validate_exact_layout(
    fields: Any, total_bytes: int, label: str, errors: list[str]
) -> list[dict[str, Any]]:
    """Validate one closed, gap-free, explicitly offset byte layout."""
    if not isinstance(fields, list) or not fields:
        fail(errors, f"{label} fields must be a non-empty list")
        return []
    cursor = 0
    names: list[str] = []
    valid: list[dict[str, Any]] = []
    for index, field in enumerate(fields):
        if not isinstance(field, dict):
            fail(errors, f"{label}[{index}] must be an object")
            continue
        name = field.get("name")
        names.append(name if isinstance(name, str) else f"<invalid-{index}>")
        if not isinstance(name, str) or not name:
            fail(errors, f"{label}[{index}].name must be non-empty")
        if field.get("offset") != cursor:
            fail(errors, f"{label}.{name}: expected offset {cursor}, got {field.get('offset')!r}")
        size = field.get("size")
        if not isinstance(size, int) or size <= 0:
            fail(errors, f"{label}.{name}: size must be a positive integer")
            continue
        cursor += size
        valid.append(field)
    require_unique(names, f"{label} field names", errors)
    if cursor != total_bytes:
        fail(errors, f"{label} must cover exactly {total_bytes} bytes, got {cursor}")
    return valid


def validate_tlv_fields(fields: Any, label: str, errors: list[str]) -> list[dict[str, Any]]:
    """Validate ascending, unique, fixed-width CTRC canonical TLV fields."""
    if not isinstance(fields, list) or not fields:
        fail(errors, f"{label} must be a non-empty list")
        return []
    tags: list[int] = []
    names: list[str] = []
    valid: list[dict[str, Any]] = []
    for index, field in enumerate(fields):
        if not isinstance(field, dict):
            fail(errors, f"{label}[{index}] must be an object")
            continue
        tag = field.get("tag")
        name = field.get("name")
        size = field.get("size")
        if not isinstance(tag, int) or tag <= 0:
            fail(errors, f"{label}[{index}].tag must be a positive integer")
        else:
            tags.append(tag)
        if not isinstance(name, str) or not name:
            fail(errors, f"{label}[{index}].name must be non-empty")
        else:
            names.append(name)
        if not isinstance(size, int) or size <= 0:
            fail(errors, f"{label}[{index}].size must be a positive integer")
        valid.append(field)
    require_unique([str(tag) for tag in tags], f"{label} tags", errors)
    require_unique(names, f"{label} names", errors)
    if tags != sorted(tags):
        fail(errors, f"{label} tags must be stored in ascending order")
    return valid


def load_matrix(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise MatrixError(f"cannot load {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MatrixError(f"{path}: top-level JSON value must be an object")
    return value


def canonical_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_domains(domains: dict[str, Any], label: str, errors: list[str]) -> None:
    if not isinstance(domains, dict) or not domains:
        fail(errors, f"{label}: domain map must be a non-empty object")
        return
    for axis, values in domains.items():
        if not isinstance(axis, str) or not axis:
            fail(errors, f"{label}: axis names must be non-empty strings")
            continue
        if not isinstance(values, list) or not values:
            fail(errors, f"{label}.{axis}: values must be a non-empty list")
            continue
        if any(not isinstance(value, str) or not value for value in values):
            fail(errors, f"{label}.{axis}: every value must be a non-empty string")
        require_unique(values, f"{label}.{axis}", errors)


def validate_condition(
    condition: Any,
    axes: dict[str, list[str]],
    rule_id: str,
    errors: list[str],
) -> None:
    if not isinstance(condition, dict):
        fail(errors, f"{rule_id}: when must be an object")
        return
    for axis, accepted in condition.items():
        if axis not in axes:
            fail(errors, f"{rule_id}: unknown axis {axis!r}")
            continue
        accepted_values = accepted if isinstance(accepted, list) else [accepted]
        if not accepted_values:
            fail(errors, f"{rule_id}: {axis} has an empty accepted-value list")
            continue
        require_unique(accepted_values, f"{rule_id}.{axis}", errors)
        for value in accepted_values:
            if value not in axes[axis]:
                fail(errors, f"{rule_id}: {axis} contains unknown value {value!r}")


def matches(rule: dict[str, Any], sample: dict[str, str]) -> bool:
    for axis, accepted in rule["when"].items():
        accepted_values = accepted if isinstance(accepted, list) else [accepted]
        if sample[axis] not in accepted_values:
            return False
    return True


def combinations(axes: dict[str, list[str]]) -> Iterable[dict[str, str]]:
    names = list(axes)
    for values in itertools.product(*(axes[name] for name in names)):
        yield dict(zip(names, values))


def validate_rule_table(
    *,
    table_name: str,
    axes: dict[str, list[str]],
    rules: Any,
    result_key: str,
    known_tests: set[str],
    errors: list[str],
) -> tuple[int, Counter[str]]:
    if not isinstance(rules, list) or not rules:
        fail(errors, f"{table_name}: rules must be a non-empty list")
        return 0, Counter()

    ids: list[str] = []
    priorities: list[int] = []
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            fail(errors, f"{table_name}[{index}] must be an object")
            continue
        rule_id = rule.get("id")
        priority = rule.get("priority")
        ids.append(rule_id if isinstance(rule_id, str) else f"<invalid-{index}>")
        priorities.append(priority if isinstance(priority, int) else -1)
        if not isinstance(rule_id, str) or not rule_id:
            fail(errors, f"{table_name}[{index}]: id must be a non-empty string")
            rule_id = f"{table_name}[{index}]"
        if not isinstance(priority, int) or priority < 0:
            fail(errors, f"{rule_id}: priority must be a non-negative integer")
        if not isinstance(rule.get(result_key), str) or not rule[result_key]:
            fail(errors, f"{rule_id}: {result_key} must be a non-empty string")
        validate_condition(rule.get("when"), axes, rule_id, errors)
        tests = rule.get("tests")
        if not isinstance(tests, list) or not tests:
            fail(errors, f"{rule_id}: tests must be a non-empty list")
        else:
            require_unique(tests, f"{rule_id}.tests", errors)
            for test_id in tests:
                if test_id not in known_tests:
                    fail(errors, f"{rule_id}: unknown test reference {test_id!r}")

    require_unique(ids, f"{table_name} rule ids", errors)
    require_unique([str(value) for value in priorities], f"{table_name} priorities", errors)
    if priorities != sorted(priorities):
        fail(errors, f"{table_name}: rules must be stored in increasing priority order")
    if rules and rules[-1].get("when") != {}:
        fail(errors, f"{table_name}: final rule must be the explicit default with when={{}}")

    selected = Counter()
    total = 0
    for sample in combinations(axes):
        total += 1
        matching = [rule for rule in rules if matches(rule, sample)]
        if not matching:
            fail(errors, f"{table_name}: uncovered combination {sample}")
            continue
        minimum = min(rule["priority"] for rule in matching)
        winners = [rule for rule in matching if rule["priority"] == minimum]
        if len(winners) != 1:
            fail(
                errors,
                f"{table_name}: combination {sample} has {len(winners)} equal-priority winners",
            )
            continue
        selected[winners[0]["id"]] += 1

    for rule in rules:
        rule_id = rule.get("id", "<invalid>")
        if selected[rule_id] == 0:
            fail(errors, f"{table_name}: rule {rule_id} is unreachable/shadowed")

    return total, selected


def validate_positive_rules(matrix: dict[str, Any], errors: list[str]) -> None:
    positive = set(matrix.get("positive_authority_verdicts", []))
    required_axes = matrix.get("positive_required_axes", [])
    authority_axes = matrix["domains"]["authority"]
    require_unique(positive, "positive authority verdicts", errors)
    require_unique(required_axes, "positive required axes", errors)
    for axis in required_axes:
        if axis not in authority_axes:
            fail(errors, f"positive_required_axes contains unknown authority axis {axis!r}")
    for rule in matrix.get("authority_rules", []):
        if rule.get("verdict") not in positive:
            continue
        missing = [axis for axis in required_axes if axis not in rule.get("when", {})]
        if missing:
            fail(errors, f"{rule.get('id')}: positive row omits axes {', '.join(missing)}")
        if not rule.get("invariants"):
            fail(errors, f"{rule.get('id')}: positive row has no invariant references")
        if not rule.get("tests"):
            fail(errors, f"{rule.get('id')}: positive row has no focused tests")


def validate_invariants(matrix: dict[str, Any], errors: list[str]) -> set[str]:
    invariants = matrix.get("invariants")
    evidence_classes = matrix.get("evidence_classes", {})
    if not isinstance(invariants, list) or not invariants:
        fail(errors, "invariants must be a non-empty list")
        return set()
    ids: list[str] = []
    for entry in invariants:
        if not isinstance(entry, dict):
            fail(errors, "every invariant must be an object")
            continue
        invariant_id = entry.get("id")
        ids.append(invariant_id if isinstance(invariant_id, str) else "<invalid>")
        if entry.get("evidence") not in evidence_classes:
            fail(errors, f"{invariant_id}: unknown evidence class {entry.get('evidence')!r}")
        for key in ("id", "name", "text"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                fail(errors, f"invariant {invariant_id}: {key} must be non-empty")
    require_unique(ids, "invariant ids", errors)
    known = set(ids)
    for table_name in ("authority_rules",):
        for rule in matrix.get(table_name, []):
            refs = rule.get("invariants")
            if not isinstance(refs, list) or not refs:
                fail(errors, f"{rule.get('id')}: invariant references must be non-empty")
                continue
            for invariant_id in refs:
                if invariant_id not in known:
                    fail(errors, f"{rule.get('id')}: unknown invariant {invariant_id!r}")
    return known


def validate_tests(matrix: dict[str, Any], errors: list[str]) -> set[str]:
    tests = matrix.get("tests")
    if not isinstance(tests, list) or not tests:
        fail(errors, "tests must be a non-empty list")
        return set()
    ids: list[str] = []
    allowed_status = {
        "NEW_MATRIX_GATE",
        "EXISTING",
        "EXISTING_PLUS_EXTENSION",
        "REQUIRED_NEW",
        "CAMPAIGN_GATE",
    }
    for entry in tests:
        if not isinstance(entry, dict):
            fail(errors, "every test entry must be an object")
            continue
        test_id = entry.get("id")
        ids.append(test_id if isinstance(test_id, str) else "<invalid>")
        for key in ("id", "status", "path", "symbol", "purpose"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                fail(errors, f"test {test_id}: {key} must be non-empty")
        if entry.get("status") not in allowed_status:
            fail(errors, f"test {test_id}: unknown status {entry.get('status')!r}")
    require_unique(ids, "test ids", errors)
    return set(ids)


def validate_conflict_matrix(matrix: dict[str, Any], errors: list[str]) -> None:
    table = matrix.get("native_conflict_matrix")
    if not isinstance(table, dict):
        fail(errors, "native_conflict_matrix must be an object")
        return
    modes = table.get("modes")
    rows = table.get("rows")
    expected_rows = {
        "MultiXactStatusForKeyShare",
        "MultiXactStatusForShare",
        "MultiXactStatusForNoKeyUpdate",
        "MultiXactStatusForUpdate",
        "MultiXactStatusNoKeyUpdate",
        "MultiXactStatusUpdate",
    }
    if not isinstance(modes, list) or len(modes) != 4:
        fail(errors, "native_conflict_matrix must define exactly four lock modes")
        return
    require_unique(modes, "native conflict modes", errors)
    if not isinstance(rows, dict) or set(rows) != expected_rows:
        missing = expected_rows - set(rows or {})
        extra = set(rows or {}) - expected_rows
        fail(errors, f"native conflict rows mismatch; missing={sorted(missing)}, extra={sorted(extra)}")
        return
    for status, values in rows.items():
        if not isinstance(values, list) or len(values) != len(modes):
            fail(errors, f"{status}: expected {len(modes)} conflict booleans")
        elif any(not isinstance(value, bool) for value in values):
            fail(errors, f"{status}: conflict cells must be booleans")


def validate_lifecycle(
    matrix: dict[str, Any], known_tests: set[str], errors: list[str]
) -> tuple[int, int]:
    lifecycle = matrix.get("lifecycle")
    if not isinstance(lifecycle, dict):
        fail(errors, "lifecycle must be an object")
        return 0, 0
    states = lifecycle.get("states")
    events = lifecycle.get("events")
    transitions = lifecycle.get("transitions")
    if not isinstance(states, list) or not states:
        fail(errors, "lifecycle.states must be non-empty")
        return 0, 0
    if not isinstance(events, list) or not events:
        fail(errors, "lifecycle.events must be non-empty")
        return 0, 0
    if not isinstance(transitions, list) or not transitions:
        fail(errors, "lifecycle.transitions must be non-empty")
        return 0, 0
    require_unique(states, "lifecycle states", errors)
    require_unique(events, "lifecycle events", errors)
    if lifecycle.get("default") != "FAIL_CLOSED_NO_STATE_MUTATION":
        fail(errors, "lifecycle default must be FAIL_CLOSED_NO_STATE_MUTATION")
    pairs: list[str] = []
    ids: list[str] = []
    for transition in transitions:
        transition_id = transition.get("id")
        ids.append(transition_id)
        source = transition.get("from")
        event = transition.get("event")
        target = transition.get("to")
        pairs.append(f"{source}\0{event}")
        if source not in states or target not in states:
            fail(errors, f"{transition_id}: unknown lifecycle state {source!r}->{target!r}")
        if event not in events:
            fail(errors, f"{transition_id}: unknown lifecycle event {event!r}")
        if not isinstance(transition.get("guard"), str) or not transition["guard"]:
            fail(errors, f"{transition_id}: guard must be non-empty")
        tests = transition.get("tests")
        if not isinstance(tests, list) or not tests:
            fail(errors, f"{transition_id}: tests must be non-empty")
        else:
            for test_id in tests:
                if test_id not in known_tests:
                    fail(errors, f"{transition_id}: unknown test reference {test_id!r}")
    require_unique(ids, "lifecycle transition ids", errors)
    require_unique(pairs, "lifecycle state/event pairs", errors)
    total_pairs = len(states) * len(events)
    default_pairs = total_pairs - len(transitions)
    if default_pairs <= 0:
        fail(errors, "lifecycle must retain at least one explicit fail-closed default pair")
    return total_pairs, default_pairs


def validate_test_references(
    refs: Any, label: str, known_tests: set[str], errors: list[str]
) -> None:
    if not isinstance(refs, list) or not refs:
        fail(errors, f"{label}: tests must be a non-empty list")
        return
    require_unique(refs, f"{label}.tests", errors)
    for test_id in refs:
        if test_id not in known_tests:
            fail(errors, f"{label}: unknown test reference {test_id!r}")


def validate_closed_machine(
    machine: Any, label: str, known_tests: set[str], errors: list[str]
) -> tuple[int, int]:
    if not isinstance(machine, dict):
        fail(errors, f"{label} must be an object")
        return 0, 0
    states = machine.get("states")
    events = machine.get("events")
    transitions = machine.get("transitions")
    if not isinstance(states, list) or not states:
        fail(errors, f"{label}.states must be non-empty")
        return 0, 0
    if not isinstance(events, list) or not events:
        fail(errors, f"{label}.events must be non-empty")
        return 0, 0
    if not isinstance(transitions, list) or not transitions:
        fail(errors, f"{label}.transitions must be non-empty")
        return 0, 0
    require_unique(states, f"{label} states", errors)
    require_unique(events, f"{label} events", errors)
    if machine.get("default") != "FAIL_CLOSED_NO_STATE_MUTATION":
        fail(errors, f"{label} default must be FAIL_CLOSED_NO_STATE_MUTATION")
    ids: list[str] = []
    pairs: list[str] = []
    for index, transition in enumerate(transitions):
        if not isinstance(transition, dict):
            fail(errors, f"{label}.transitions[{index}] must be an object")
            continue
        transition_id = transition.get("id")
        ids.append(transition_id if isinstance(transition_id, str) else "<invalid>")
        source = transition.get("from")
        event = transition.get("event")
        target = transition.get("to")
        pairs.append(f"{source}\0{event}")
        if source not in states or target not in states:
            fail(errors, f"{transition_id}: unknown {label} state {source!r}->{target!r}")
        if event not in events:
            fail(errors, f"{transition_id}: unknown {label} event {event!r}")
        if not isinstance(transition.get("guard"), str) or not transition["guard"]:
            fail(errors, f"{transition_id}: guard must be non-empty")
        validate_test_references(
            transition.get("tests"), transition_id or label, known_tests, errors
        )
    require_unique(ids, f"{label} transition ids", errors)
    require_unique(pairs, f"{label} state/event pairs", errors)
    total_pairs = len(states) * len(events)
    default_pairs = total_pairs - len(transitions)
    if default_pairs <= 0:
        fail(errors, f"{label} must retain fail-closed default pairs")
    return total_pairs, default_pairs


def validate_ctrc(
    matrix: dict[str, Any], known_tests: set[str], errors: list[str]
) -> dict[str, int]:
    ctrc = matrix.get("terminal_reference_protocol")
    if not isinstance(ctrc, dict):
        fail(errors, "terminal_reference_protocol must be an object")
        return {
            "ctrc_source_classes": 0,
            "ctrc_reference_kinds": 0,
            "ctrc_target_kinds": 0,
            "ctrc_publication_fields": 0,
            "ctrc_request_fields": 0,
            "ctrc_reply_header_fields": 0,
            "ctrc_ack_fields": 0,
            "ctrc_receipt_pairs": 0,
            "ctrc_receipt_default_fail_closed_pairs": 0,
            "ctrc_seal_pairs": 0,
            "ctrc_seal_default_fail_closed_pairs": 0,
            "ctrc_crash_cuts": 0,
            "ctrc_wal_fields": 0,
        }
    if ctrc.get("protocol_id") != "CTRC-V1":
        fail(errors, "terminal_reference_protocol.protocol_id must be CTRC-V1")
    if ctrc.get("evidence_class") != "PGRAC_ADAPTATION":
        fail(errors, "CTRC must be labelled PGRAC_ADAPTATION")
    if ctrc.get("progress_owner") != "existing cluster_undo_cleaner":
        fail(errors, "CTRC progress owner must remain the existing cluster_undo_cleaner")
    if "canonical physical TT slot" not in str(ctrc.get("status_authority", "")):
        fail(errors, "CTRC status authority must remain the canonical physical TT slot")
    if ctrc.get("normative_invariants") != [f"MXA-I{value}" for value in range(13, 23)]:
        fail(errors, "CTRC normative invariants must remain MXA-I13..MXA-I22 in order")
    if not isinstance(ctrc.get("forbidden_authorities"), list) or len(ctrc["forbidden_authorities"]) != 6:
        fail(errors, "CTRC must retain the six closed non-authority artifact classes")

    expected_key_fields = [
        "format_version",
        "system_identifier",
        "origin_node_id",
        "owner_instance",
        "origin_boot_incarnation",
        "cluster_epoch",
        "formation_epoch",
        "admission_record_generation",
        "root_descriptor_incarnation",
        "root_id",
        "root_generation",
        "segment_id",
        "segment_generation",
        "slot_offset",
        "slot_wrap",
        "xid",
    ]
    expected_publication_fields = [
        "requester_node_id",
        "requester_boot_incarnation",
        "capability_record_generation",
        "requester_backend_id",
        "wire_request_id",
        "operation_id",
        "attempt_generation",
        "descriptor_hash",
        "member_ordinal",
        "member_role",
        "reference_kind",
        "target_kind",
        "journal_sequence",
        "key_sequence",
        "journal_slot_generation",
        "grant_generation",
    ]
    if ctrc.get("transaction_key_fields") != expected_key_fields:
        fail(errors, "CTRC transaction_key_fields changed or are incomplete")
    if ctrc.get("publication_id_fields") != expected_publication_fields:
        fail(errors, "CTRC publication_id_fields changed or are incomplete")
    expected_reference_kinds = [
        "CTRC_REF_HEAP_ITL_UBA",
        "CTRC_REF_CURRENT_MX_LOCKER",
        "CTRC_REF_CURRENT_MX_UPDATER",
        "CTRC_REF_RECOMPOSED_SURVIVOR",
        "CTRC_REF_HOT_FOLLOW_EDGE",
    ]
    if ctrc.get("reference_kinds") != expected_reference_kinds:
        fail(errors, "CTRC reference kinds must remain the frozen five-value order")
    if ctrc.get("reference_kind_values") != {
        name: value for value, name in enumerate(expected_reference_kinds, 1)
    }:
        fail(errors, "CTRC reference kind numeric mapping changed")
    if ctrc.get("target_kinds") != [
        "CTRC_TARGET_EXACT_ITL_SLOT",
        "CTRC_TARGET_EXACT_TID",
        "CTRC_TARGET_PAGE_PENDING_ITL_SLOT",
        "CTRC_TARGET_PAGE_PENDING_OFFNUM",
    ]:
        fail(errors, "CTRC target kinds changed")
    if ctrc.get("target_kind_values") != {
        "CTRC_TARGET_EXACT_ITL_SLOT": 1,
        "CTRC_TARGET_EXACT_TID": 2,
        "CTRC_TARGET_PAGE_PENDING_ITL_SLOT": 3,
        "CTRC_TARGET_PAGE_PENDING_OFFNUM": 4,
    }:
        fail(errors, "CTRC target kind numeric mapping changed")
    generations = ctrc.get("generation_contract")
    if not isinstance(generations, dict):
        fail(errors, "CTRC generation_contract must be an object")
    else:
        for key in (
            "zero",
            "grant_generation",
            "seal_generation",
            "journal_sequence",
            "key_sequence",
            "journal_slot_generation",
        ):
            if not isinstance(generations.get(key), str) or not generations[key]:
                fail(errors, f"CTRC generation_contract.{key} must be non-empty")
        if "uint32" not in str(generations.get("grant_generation", "")):
            fail(errors, "CTRC grant generation must remain uint32 for proof reserved8[4]")

    key_encoding = ctrc.get("transaction_key_encoding")
    if not isinstance(key_encoding, dict):
        fail(errors, "CTRC transaction_key_encoding must be an object")
    else:
        if key_encoding.get("version") != 1 or key_encoding.get("total_bytes") != 96:
            fail(errors, "CTRC transaction key must remain version 1 and 96 bytes")
        if key_encoding.get("endianness") != "little":
            fail(errors, "CTRC transaction key must remain little-endian")
        key_fields = validate_exact_layout(
            key_encoding.get("fields"), 96, "CTRC transaction key", errors
        )
        expected_key_layout = [
            ("format_version", 0, 1), ("owner_instance", 1, 1),
            ("origin_node_id", 2, 2), ("segment_id", 4, 4),
            ("segment_generation", 8, 4), ("slot_offset", 12, 2),
            ("slot_wrap", 14, 2), ("xid", 16, 4),
            ("cluster_epoch", 20, 4), ("system_identifier", 24, 8),
            ("origin_boot_incarnation", 32, 8), ("formation_epoch", 40, 8),
            ("admission_record_generation", 48, 8),
            ("root_descriptor_incarnation", 56, 8), ("root_id", 64, 8),
            ("root_generation", 72, 8), ("reserved", 80, 16),
        ]
        if [(f.get("name"), f.get("offset"), f.get("size")) for f in key_fields] != expected_key_layout:
            fail(errors, "CTRC transaction-key field layout changed")
        if not key_fields or key_fields[-1].get("required") != 0:
            fail(errors, "CTRC transaction-key reserved bytes must be required zero")

    publication_encoding = ctrc.get("publication_id_encoding")
    publication_fields: list[dict[str, Any]] = []
    if not isinstance(publication_encoding, dict):
        fail(errors, "CTRC publication_id_encoding must be an object")
    else:
        publication_fields = validate_tlv_fields(
            publication_encoding.get("fields"), "CTRC publication fields", errors
        )
        expected_publication_layout = [
            (tag, name, size)
            for tag, (name, size) in enumerate(
                [
                    ("requester_node_id", 2), ("requester_boot_incarnation", 8),
                    ("capability_record_generation", 4), ("requester_backend_id", 4),
                    ("wire_request_id", 8), ("operation_id", 8),
                    ("attempt_generation", 4), ("descriptor_hash", 8),
                    ("member_ordinal", 2), ("member_role", 1),
                    ("reference_kind", 1), ("target_kind", 1),
                    ("journal_sequence", 8), ("key_sequence", 8),
                    ("journal_slot_generation", 8), ("grant_generation", 4),
                ],
                1,
            )
        ]
        if [(f.get("tag"), f.get("name"), f.get("size")) for f in publication_fields] != expected_publication_layout:
            fail(errors, "CTRC publication TLV field mapping changed")
        if publication_encoding.get("ordinary_itl_sentinels") != {
            "descriptor_hash": 0, "member_ordinal": 65535, "member_role": 0
        }:
            fail(errors, "CTRC ordinary-ITL publication sentinels changed")

    target_encoding = ctrc.get("target_encoding")
    if not isinstance(target_encoding, dict):
        fail(errors, "CTRC target_encoding must be an object")
    else:
        expected_target_groups = {
            "common_fields": [
                (1, "target_kind", 1), (2, "spc_oid", 4), (3, "db_oid", 4),
                (4, "rel_number", 4), (5, "fork_number", 4),
                (6, "block_number", 4), (7, "predecessor_page_lsn", 8),
                (8, "predecessor_page_scn", 8),
                (9, "publication_own_generation", 8),
                (10, "publication_acquisition_epoch", 8),
                (11, "relation_persistence", 1), (12, "needs_wal", 1),
            ],
            "exact_itl_fields": [
                (20, "itl_slot_index", 2), (21, "itl_slot_wrap", 2),
                (22, "itl_xid", 4), (23, "itl_class", 1), (24, "uba", 16),
                (25, "planned_predecessor_sha256", 32),
                (26, "planned_successor_sha256", 32),
            ],
            "pending_itl_fields": [(27, "page_operation_kind", 1)],
            "exact_tid_fields": [
                (30, "offset_number", 2), (31, "itemid_flags", 2),
                (32, "itemid_offset", 2), (33, "itemid_length", 2),
                (34, "tuple_header_sha256", 32), (35, "mx_origin_node_id", 2),
                (36, "multixact_id", 4), (37, "mx_cluster_epoch", 4),
                (38, "descriptor_hash", 8),
                (39, "successor_topology_sha256", 32),
            ],
            "pending_offnum_fields": [(40, "intended_descriptor_hash", 8)],
        }
        all_target_tags: list[str] = []
        for group, expected in expected_target_groups.items():
            fields = validate_tlv_fields(
                target_encoding.get(group), f"CTRC target {group}", errors
            )
            actual = [(f.get("tag"), f.get("name"), f.get("size")) for f in fields]
            if actual != expected:
                fail(errors, f"CTRC target {group} mapping changed")
            all_target_tags.extend(str(f.get("tag")) for f in fields)
        require_unique(all_target_tags, "CTRC target tags across groups", errors)
        finalization = str(target_encoding.get("finalization", ""))
        if "before the PREPARED-to-APPLIED" not in finalization:
            fail(errors, "CTRC pending targets must finalize before APPLIED")
        for term in (
            "predecessor_page_lsn/scn are the exact pre-mutation sample",
            "never requires an unknowable post-mutation equality",
            "newer LSN alone as absence",
            "CANCELLED_PREMUTATION",
        ):
            if term not in finalization:
                fail(errors, f"CTRC target finalization contract missing {term!r}")

    origin_registry = ctrc.get("origin_registry_contract")
    if not isinstance(origin_registry, dict) or set(origin_registry) != {
        "creation", "missing", "touch_before_use", "identity_change", "zero_touched",
        "terminal_zero_grant",
    }:
        fail(errors, "CTRC origin_registry_contract is incomplete")
    elif "never" not in origin_registry["missing"] or "before" not in origin_registry["touch_before_use"]:
        fail(errors, "CTRC origin registry lost-state/touch ordering polarity changed")
    elif "cannot prepare a receipt" not in origin_registry["terminal_zero_grant"]:
        fail(errors, "CTRC terminal zero-grant publication rule changed")

    source_classes = ctrc.get("source_classes")
    if not isinstance(source_classes, list):
        fail(errors, "CTRC source_classes must be a list")
        source_classes = []
    expected_source_classes = {
        "REGISTERED_REFERENCE",
        "TERMINAL_PROJECTION_DISCHARGE",
        "SUCCESSOR_BEFORE_PREDECESSOR",
        "PROVEN_LOCAL_NONCLUSTER",
        "FAIL_CLOSED_UNREACHABLE",
    }
    require_unique(
        [entry.get("id") for entry in source_classes if isinstance(entry, dict)],
        "CTRC source class ids",
        errors,
    )
    if {entry.get("class") for entry in source_classes if isinstance(entry, dict)} != expected_source_classes:
        fail(errors, "CTRC source class set changed or is incomplete")
    for entry in source_classes:
        if not isinstance(entry, dict):
            fail(errors, "every CTRC source class must be an object")
            continue
        for key in ("id", "class", "owner", "requirement"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                fail(errors, f"CTRC source class {entry.get('id')}: {key} must be non-empty")
        validate_test_references(entry.get("tests"), entry.get("id", "CTRC source"), known_tests, errors)
    source_by_class = {
        entry.get("class"): entry for entry in source_classes if isinstance(entry, dict)
    }
    successor_source = source_by_class.get("SUCCESSOR_BEFORE_PREDECESSOR", {})
    successor_text = " ".join(
        str(successor_source.get(key, "")) for key in ("owner", "requirement")
    )
    for term in ("KO/SMGR", "drop/truncate", "KO DONE", "durable"):
        if term not in successor_text:
            fail(errors, f"CTRC object-removal source contract missing {term!r}")

    receipt_pairs, receipt_defaults = validate_closed_machine(
        ctrc.get("receipt_machine"), "CTRC receipt machine", known_tests, errors
    )
    seal_pairs, seal_defaults = validate_closed_machine(
        ctrc.get("seal_machine"), "CTRC seal machine", known_tests, errors
    )

    wire = ctrc.get("wire_contract")
    expected_wire = {
        "magic_value": "0x50474354",
        "hello_capability_value": "0x00400000",
        "defined_capability_count": 21,
        "current_mx_wire_version": 2,
        "forward_kind_value": 11,
        "predecessor_forward_kind_value": 10,
        "reply_status_value": 30,
        "predecessor_reply_status_value": 29,
    }
    if not isinstance(wire, dict):
        fail(errors, "CTRC wire_contract must be an object")
    else:
        for key, expected in expected_wire.items():
            if wire.get(key) != expected:
                fail(errors, f"CTRC wire {key} must be {expected!r}, got {wire.get(key)!r}")
        if wire.get("reserved_capability_holes") != ["0x00004000", "0x00040000"]:
            fail(errors, "CTRC must preserve both historical capability holes")
        if wire.get("suboperations") != {"CLOSE_AND_CLEAN": 1, "CERTIFICATE_COMMITTED": 2}:
            fail(errors, "CTRC wire suboperations changed")
        if wire.get("request_bytes") != 128:
            fail(errors, "CTRC seal request must remain exactly 128 bytes")
        request_fields = validate_exact_layout(
            wire.get("request_fields"), 128, "CTRC seal request", errors
        )
        expected_request_anchors = {
            "request_id": (0, 8), "cluster_epoch_u64": (8, 8),
            "original_requester_node": (36, 4), "requester_backend_id": (40, 4),
            "grant_generation": (48, 4), "slot_wrap": (52, 2),
            "owner_instance": (54, 1), "suboperation": (55, 1),
            "segment_generation": (56, 4), "forward_kind": (63, 1),
            "magic": (64, 4), "wire_version": (68, 2),
            "wire_length": (70, 2), "seal_generation": (120, 8),
        }
        request_map = {field.get("name"): (field.get("offset"), field.get("size")) for field in request_fields}
        if any(request_map.get(name) != anchor for name, anchor in expected_request_anchors.items()):
            fail(errors, "CTRC request routing/identity anchor layout changed")
        required_request = {field.get("name"): field.get("required") for field in request_fields if "required" in field}
        if required_request != {
            "requester_backend_id": -2, "request_flags": 0,
            "selector_version": 1, "forward_kind": 11,
            "magic": "0x50474354", "wire_version": 2, "wire_length": 128,
        }:
            fail(errors, "CTRC request fixed-value contract changed")
        expected_reconstruction_terms = (
            "field-decode little-endian TT key",
            "both reserved words zero",
            "system_identifier from authenticated local cluster",
            "segment_id=zero_extend(undo_segment_id)",
            "slot_offset=tt_slot_id-1",
            "uint64 epoch fits uint32 and equals embedded epoch",
            "source and destination node ids in 0..15",
            "exactly one stored full key",
        )
        reconstruction = str(wire.get("request_key_reconstruction", ""))
        if any(term not in reconstruction for term in expected_reconstruction_terms):
            fail(errors, "CTRC request full-key reconstruction contract changed")
        if wire.get("reply_page_bytes") != 8192 or wire.get("reply_header_bytes") != 64:
            fail(errors, "CTRC reply must remain BLCKSZ=8192 with a 64-byte header")
        reply_fields = validate_exact_layout(
            wire.get("reply_header_fields"), 64, "CTRC reply header", errors
        )
        if wire.get("reply_results") != {
            "DENIED": 0, "LOCAL_RELEASE_ACK": 1, "PENDING_DRAIN": 2,
            "BLOCKED_RETAIN": 3, "CERTIFICATE_RECLAIMED": 4,
        }:
            fail(errors, "CTRC reply result domain changed")
        if wire.get("reply_ack_offset") != 64 or wire.get("reply_ack_bytes") != 416:
            fail(errors, "CTRC reply ACK placement changed")
        if wire.get("reply_tail_offset") != 480 or "all zero" not in str(wire.get("reply_tail_rule", "")):
            fail(errors, "CTRC reply zero-tail boundary changed")
        if "0..15" not in str(wire.get("formation_dependency_domain", "")):
            fail(errors, "CTRC v1 formation must remain inside the exact 16-entry dependency domain")
        expected_reply_names = [
            "magic", "wire_version", "header_length", "request_id", "cluster_epoch",
            "seal_generation", "source_node", "destination_node", "result",
            "suboperation", "first_reason", "reply_flags", "ack_length",
            "request_sha256_prefix", "body_length", "header_crc32c",
        ]
        if [field.get("name") for field in reply_fields] != expected_reply_names:
            fail(errors, "CTRC reply header field order changed")

    ack = ctrc.get("participant_ack_contract")
    ack_fields: list[dict[str, Any]] = []
    if not isinstance(ack, dict):
        fail(errors, "CTRC participant_ack_contract must be an object")
    else:
        if ack.get("version") != 1 or ack.get("total_bytes") != 416:
            fail(errors, "CTRC participant ACK must remain version 1 and 416 bytes")
        if ack.get("dependency_entries") != 16:
            fail(errors, "CTRC participant ACK must retain 16 dependency entries")
        ack_fields = validate_exact_layout(
            ack.get("fields"), 416, "CTRC participant ACK", errors
        )
        if ack.get("known_flags") != {"ZERO_RANGE": "0x0001", "ALL_DURABLE": "0x0002"}:
            fail(errors, "CTRC participant ACK flag allocation changed")
        if ack.get("empty_digest") != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855":
            fail(errors, "CTRC zero-range ACK must use SHA-256 of the empty byte string")
        if "tombstone only" not in str(ack.get("empty_positive", "")):
            fail(errors, "CTRC zero-range ACK must remain close-before-proof tombstone only")
        if "first_key_sequence=1" not in str(ack.get("nonempty_positive", "")):
            fail(errors, "CTRC nonempty ACK must cover key sequence 1..N")
        if ack.get("crc_rule") != "CRC32C over bytes 0..411":
            fail(errors, "CTRC ACK CRC boundary changed")
        ack_map = {field.get("name"): (field.get("offset"), field.get("size")) for field in ack_fields}
        for name, anchor in {
            "transaction_key": (0, 96), "first_key_sequence": (144, 8),
            "last_key_sequence": (152, 8), "row_digest_sha256": (224, 32),
            "required_lsn_vector": (264, 128), "reserved": (392, 20),
            "crc32c": (412, 4),
        }.items():
            if ack_map.get(name) != anchor:
                fail(errors, f"CTRC ACK anchor {name} changed")

    digest = ctrc.get("digest_contract")
    if not isinstance(digest, dict):
        fail(errors, "CTRC digest_contract must be an object")
    else:
        if digest.get("algorithm") != "SHA-256":
            fail(errors, "CTRC digest algorithm must remain SHA-256")
        if digest.get("participant_ack_bytes") != 32:
            fail(errors, "CTRC participant ACK digest must remain 32 bytes")
        if digest.get("wal_certificate_bytes") != 16:
            fail(errors, "CTRC WAL certificate digest must remain 16 bytes")
        if "sorted" not in str(digest.get("encoding", "")):
            fail(errors, "CTRC digest encoding must define canonical sorting")
        if digest.get("row_domain_hex") != "50475241432d435452432d524f572d563100":
            fail(errors, "CTRC row digest domain changed")
        if digest.get("ackset_domain_hex") != "50475241432d435452432d41434b5345542d563100":
            fail(errors, "CTRC ACK-set digest domain changed")
        if digest.get("release_disposition_values") != {
            "CANCELLED_PREMUTATION": 1,
            "CLEANED_ABSENT": 2,
            "CLEANED_TERMINAL_REWRITE": 3,
            "CLEANED_SUCCESSOR_REPLACED": 4,
        }:
            fail(errors, "CTRC release-disposition allocation changed")
        if digest.get("row_components") != [
            "96-byte transaction_key_encoding",
            "uint32_le publication_id_encoding length plus bytes",
            "uint32_le canonical target_encoding length plus bytes",
            "uint8 release disposition",
            "uint64_le highest local WAL LSN",
            "16 uint64_le required-LSN entries in node-id order",
        ]:
            fail(errors, "CTRC row digest component order changed")
        if digest.get("target_rule") != (
            "CLEANED dispositions require finalized exact target; only "
            "CANCELLED_PREMUTATION may retain canonical pending target"
        ):
            fail(errors, "CTRC digest finalized/pending target rule changed")
        if digest.get("nonempty_row_derivation") != (
            "SHA-256(ROW domain || uint64_le(N) || for key_sequence 1..N: "
            "uint32_le(row_bytes_length) || row_bytes)"
        ):
            fail(errors, "CTRC nonempty row-digest derivation changed")
        if digest.get("empty_row_derivation") != (
            "sole persistent N=0 tombstone uses SHA-256(empty byte string), "
            "with no domain or count bytes"
        ):
            fail(errors, "CTRC empty row-digest exception changed")
        if digest.get("ackset_derivation") != (
            "SHA-256(ACKSET domain || uint16_le(participant_count) || in "
            "ascending participant node id: uint32_le(416) || all 416 ACK "
            "bytes including CRC32C)"
        ):
            fail(errors, "CTRC ACK-set digest derivation changed")
        if digest.get("wal_derivation") != (
            "first 16 hash-output bytes of ackset_derivation without integer "
            "byte-order conversion"
        ):
            fail(errors, "CTRC WAL digest truncation rule changed")

    wal = ctrc.get("wal_contract")
    wal_fields: list[dict[str, Any]] = []
    if not isinstance(wal, dict):
        fail(errors, "CTRC wal_contract must be an object")
    else:
        for key, expected in {
            "opcode_value": "0xA0",
            "payload_version": 1,
            "payload_size": 96,
            "slot_flag_value": "0x01",
        }.items():
            if wal.get(key) != expected:
                fail(errors, f"CTRC WAL {key} must be {expected!r}, got {wal.get(key)!r}")
        wal_fields = wal.get("payload_fields") if isinstance(wal.get("payload_fields"), list) else []
        cursor = 0
        names: list[str] = []
        for field in wal_fields:
            if not isinstance(field, dict):
                fail(errors, "every CTRC WAL field must be an object")
                continue
            names.append(field.get("name"))
            if field.get("offset") != cursor:
                fail(errors, f"CTRC WAL field {field.get('name')}: expected offset {cursor}")
            size = field.get("size")
            if not isinstance(size, int) or size <= 0:
                fail(errors, f"CTRC WAL field {field.get('name')}: size must be positive")
            else:
                cursor += size
        require_unique(names, "CTRC WAL field names", errors)
        if cursor != 96:
            fail(errors, f"CTRC WAL fields must cover exactly 96 bytes, got {cursor}")
        if wal.get("apply_predecessors") != ["COMMITTED_EXACT", "ABORTED_EXACT"]:
            fail(errors, "CTRC WAL apply predecessors changed")
        if wal.get("touched_bitmap_rule") != (
            "v1 permits only node-id bits 0..15; bits 16..127 must be zero and "
            "touched bitmap must equal the frozen origin registry exactly"
        ):
            fail(errors, "CTRC WAL touched-bitmap domain changed")
        expected_clear = {"UNUSED", "ACTIVE", "RECYCLABLE", "INVALID", "FRESH_TEMPLATE"}
        if set(wal.get("clear_on_states", [])) != expected_clear:
            fail(errors, "CTRC release-bit clear-state set changed")
        if "does not require" not in str(wal.get("redo_admission_rule", "")):
            fail(errors, "CTRC redo must not depend on reopening a historical formation")
        validate_test_references(wal.get("tests"), "CTRC WAL", known_tests, errors)

    release = ctrc.get("release_rules")
    if not isinstance(release, dict):
        fail(errors, "CTRC release_rules must be an object")
    else:
        if "horizon" not in str(release.get("committed", "")):
            fail(errors, "CTRC committed release must retain the MVCC horizon")
        if "no wall-clock or SCN guess" not in str(release.get("aborted", "")):
            fail(errors, "CTRC aborted release must forbid guessed horizons")
        validate_test_references(release.get("tests"), "CTRC release rules", known_tests, errors)

    capacity = ctrc.get("capacity_contract")
    if not isinstance(capacity, dict):
        fail(errors, "CTRC capacity_contract must be an object")
    else:
        if capacity.get("origin_key_entries") != "CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT":
            fail(errors, "CTRC origin-key capacity formula changed")
        if capacity.get("participant_key_entries") != "origin_key_entries * min(max(cluster_conf_declared_node_count_early(), 1), CLUSTER_SF_DEP_MAX_ORIGINS)":
            fail(errors, "CTRC participant-key capacity formula changed")
        if capacity.get("receipt_entries") != "NBuffers + MaxBackends * CLUSTER_CURRENT_MX_MAX_MEMBERS":
            fail(errors, "CTRC receipt capacity formula changed")
        if capacity.get("ack_summary_entries") != "participant_key_entries":
            fail(errors, "CTRC ACK-summary capacity formula changed")
        validate_test_references(capacity.get("tests"), "CTRC capacity", known_tests, errors)

    crash_cuts = ctrc.get("crash_cut_results")
    if not isinstance(crash_cuts, list):
        fail(errors, "CTRC crash_cut_results must be a list")
        crash_cuts = []
    require_unique(
        [entry.get("id") for entry in crash_cuts if isinstance(entry, dict)],
        "CTRC crash-cut ids",
        errors,
    )
    for entry in crash_cuts:
        if not isinstance(entry, dict):
            fail(errors, "every CTRC crash cut must be an object")
            continue
        for key in ("id", "cut", "result"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                fail(errors, f"CTRC crash cut {entry.get('id')}: {key} must be non-empty")
        validate_test_references(entry.get("tests"), entry.get("id", "CTRC cut"), known_tests, errors)

    def collect_test_refs(value: Any) -> set[str]:
        found: set[str] = set()
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "tests" and isinstance(child, list):
                    found.update(item for item in child if isinstance(item, str))
                else:
                    found.update(collect_test_refs(child))
        elif isinstance(value, list):
            for child in value:
                found.update(collect_test_refs(child))
        return found

    expected_ctrc_tests = {f"MXA-T{value}" for value in range(20, 36)}
    referenced_ctrc_tests = collect_test_refs(ctrc) & expected_ctrc_tests
    if referenced_ctrc_tests != expected_ctrc_tests:
        fail(
            errors,
            "CTRC protocol test coverage mismatch; "
            f"missing={sorted(expected_ctrc_tests - referenced_ctrc_tests)}, "
            f"extra={sorted(referenced_ctrc_tests - expected_ctrc_tests)}",
        )

    return {
        "ctrc_source_classes": len(source_classes),
        "ctrc_reference_kinds": len(ctrc.get("reference_kinds", [])),
        "ctrc_target_kinds": len(ctrc.get("target_kinds", [])),
        "ctrc_publication_fields": len(publication_fields),
        "ctrc_request_fields": len(request_fields) if isinstance(wire, dict) else 0,
        "ctrc_reply_header_fields": len(reply_fields) if isinstance(wire, dict) else 0,
        "ctrc_ack_fields": len(ack_fields),
        "ctrc_receipt_pairs": receipt_pairs,
        "ctrc_receipt_default_fail_closed_pairs": receipt_defaults,
        "ctrc_seal_pairs": seal_pairs,
        "ctrc_seal_default_fail_closed_pairs": seal_defaults,
        "ctrc_crash_cuts": len(crash_cuts),
        "ctrc_wal_fields": len(wal_fields),
    }


def file_contains(path: Path, needle: str) -> bool:
    try:
        return needle in path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False


def validate_public_anchors(
    matrix: dict[str, Any], public_tree: Path, errors: list[str]
) -> None:
    if not public_tree.is_dir():
        fail(errors, f"public source tree does not exist: {public_tree}")
        return
    for contract in matrix.get("code_contracts", []):
        if contract.get("status") == "REQUIRED_NEW":
            continue
        path = public_tree / contract.get("path", "")
        symbol = contract.get("symbol", "")
        if not path.is_file():
            fail(errors, f"{contract.get('id')}: missing code path {path}")
        elif not file_contains(path, symbol):
            fail(errors, f"{contract.get('id')}: symbol {symbol!r} not found in {path}")
    for test in matrix.get("tests", []):
        if test.get("status") not in {"EXISTING", "EXISTING_PLUS_EXTENSION"}:
            continue
        path = public_tree / test["path"]
        if not path.is_file():
            fail(errors, f"{test['id']}: expected existing test path {path}")
        elif not file_contains(path, test["symbol"]):
            fail(errors, f"{test['id']}: symbol {test['symbol']!r} not found in {path}")


def validate_matrix(matrix: dict[str, Any], public_tree: Path | None) -> dict[str, int]:
    errors: list[str] = []
    if matrix.get("schema_version") != 2:
        fail(errors, "schema_version must be 2")
    if matrix.get("matrix_id") != EXPECTED_MATRIX_ID:
        fail(errors, f"matrix_id must remain {EXPECTED_MATRIX_ID}")
    if matrix.get("status") != "FROZEN_FOR_IMPLEMENTATION":
        fail(errors, "matrix status must remain FROZEN_FOR_IMPLEMENTATION")
    if matrix.get("approved_date") != "2026-09-01":
        fail(errors, "matrix approved_date must remain 2026-09-01")
    matrix_sha256 = canonical_sha256(matrix)
    if matrix_sha256 != EXPECTED_MATRIX_SHA256:
        fail(
            errors,
            "frozen matrix digest changed; update the private spec, focused tests, "
            "and EXPECTED_MATRIX_SHA256 together "
            f"(expected {EXPECTED_MATRIX_SHA256}, got {matrix_sha256})",
        )
    for key in ("matrix_id", "title", "status", "approved_date"):
        if not isinstance(matrix.get(key), str) or not matrix[key]:
            fail(errors, f"{key} must be a non-empty string")

    domains = matrix.get("domains")
    if not isinstance(domains, dict):
        raise MatrixError("domains must be an object")
    authority_axes = domains.get("authority")
    composition_axes = domains.get("composition")
    validate_domains(authority_axes, "domains.authority", errors)
    validate_domains(composition_axes, "domains.composition", errors)

    known_tests = validate_tests(matrix, errors)
    known_invariants = validate_invariants(matrix, errors)
    validate_positive_rules(matrix, errors)
    validate_conflict_matrix(matrix, errors)

    authority_total, authority_selected = validate_rule_table(
        table_name="authority_rules",
        axes=authority_axes,
        rules=matrix.get("authority_rules"),
        result_key="verdict",
        known_tests=known_tests,
        errors=errors,
    )
    composition_total, composition_selected = validate_rule_table(
        table_name="composition_rules",
        axes=composition_axes,
        rules=matrix.get("composition_rules"),
        result_key="decision",
        known_tests=known_tests,
        errors=errors,
    )
    lifecycle_total, lifecycle_defaults = validate_lifecycle(matrix, known_tests, errors)
    ctrc_summary = validate_ctrc(matrix, known_tests, errors)

    contract_ids = [entry.get("id") for entry in matrix.get("code_contracts", [])]
    require_unique(contract_ids, "code contract ids", errors)
    for contract in matrix.get("code_contracts", []):
        for key in ("id", "path", "symbol", "responsibility"):
            if not isinstance(contract.get(key), str) or not contract[key]:
                fail(errors, f"code contract {contract.get('id')}: {key} must be non-empty")
        if contract.get("status", "EXISTING") not in {
            "EXISTING",
            "EXISTING_PLUS_EXTENSION",
            "REQUIRED_NEW",
        }:
            fail(errors, f"code contract {contract.get('id')}: invalid status")

    if public_tree is not None:
        validate_public_anchors(matrix, public_tree.resolve(), errors)

    summary = {
        "authority_combinations": authority_total,
        "authority_rules_reached": len(authority_selected),
        "composition_combinations": composition_total,
        "composition_rules_reached": len(composition_selected),
        "lifecycle_pairs": lifecycle_total,
        "lifecycle_default_fail_closed_pairs": lifecycle_defaults,
        "invariants": len(known_invariants),
        "tests": len(known_tests),
        "code_contracts": len(contract_ids),
    }
    summary.update(ctrc_summary)
    for key, expected in EXPECTED_COUNTS.items():
        if summary[key] != expected:
            fail(errors, f"{key} must remain {expected}, got {summary[key]}")

    if errors:
        rendered = "\n".join(f"  - {message}" for message in errors)
        raise MatrixError(f"matrix validation failed ({len(errors)} errors):\n{rendered}")

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matrix",
        type=Path,
        default=DEFAULT_MATRIX,
        help=f"matrix JSON (default: {DEFAULT_MATRIX})",
    )
    parser.add_argument(
        "--public-tree",
        type=Path,
        help="optional public source tree used to verify frozen code/test anchors",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        matrix = load_matrix(args.matrix.resolve())
        summary = validate_matrix(matrix, args.public_tree)
    except MatrixError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"GREEN: {matrix['matrix_id']}")
    for key, value in summary.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
