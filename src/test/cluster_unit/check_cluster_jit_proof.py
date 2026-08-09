#!/usr/bin/env python3
# -------------------------------------------------------------------------
#
# check_cluster_jit_proof.py
#     Compiler and link-object proof driver for cluster JIT contracts.
#
# IDENTIFICATION
#     src/test/cluster_unit/check_cluster_jit_proof.py
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# -------------------------------------------------------------------------

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, TextIO, Tuple


PASS = 0
SEMANTIC_RED = 1
ENVIRONMENT_FAILURE = 2
PROOF_INTERNAL = 3

ABI_PROOF_IDS = (
    "JIT-A-T2-PAGEVERSION-TYPE",
    "JIT-A-T2-PAGEVERSION-SIZE-24",
    "JIT-A-T2-PAGEVERSION-TOKEN-OFFSET-16",
    "JIT-A-T2-ID251-VALUE-251",
    "JIT-A-T2-ID251-HEADER-16",
    "JIT-A-T2-ID251-ENTRY-48",
    "JIT-A-T2-ID251-MAX-33-1600",
    "JIT-A-T2-SCALAR-GENERATION-ABSENT",
    "JIT-A-T2-ROUTE-ABI-8",
    "JIT-A-T2-COLD-POLL-100",
)


class ProofFailure(Exception):
    """A closed proof result used by the command-line provider."""

    def __init__(self, result_class: int, message: str) -> None:
        super().__init__(message)
        self.result_class = result_class


def repository_root(script_path: Path) -> Path:
    """Derive the source root from the provider path, never from CWD."""

    path = script_path.resolve(strict=False)
    expected = ("src", "test", "cluster_unit")
    if tuple(path.parent.parts[-3:]) != expected:
        raise ProofFailure(PROOF_INTERNAL,
                           "provider path is outside src/test/cluster_unit")
    return path.parents[3]


def resolve_input_path(value: str, root: Path) -> Path:
    """Resolve provider inputs against an explicit root, never process CWD."""

    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return path.resolve(strict=False)


@dataclass(frozen=True)
class ManifestRow:
    proof_id: str
    symbol: str
    defining_object: str
    allowed_references: Tuple[str, ...]


def parse_manifest(stream: TextIO) -> List[ManifestRow]:
    """Parse the declarative symbol-owner manifest fail-closed."""

    reader = csv.DictReader(stream, delimiter="\t")
    expected = ["proof_id", "symbol", "defining_object",
                "allowed_reference_objects"]
    if reader.fieldnames != expected:
        raise ProofFailure(PROOF_INTERNAL, "malformed manifest header")

    rows: List[ManifestRow] = []
    proof_ids = set()
    symbols = set()
    for lineno, raw in enumerate(reader, start=2):
        if None in raw or any(value is None for value in raw.values()):
            raise ProofFailure(PROOF_INTERNAL,
                               f"malformed manifest row {lineno}")
        proof_id = raw["proof_id"].strip()
        symbol = raw["symbol"].strip()
        owner = raw["defining_object"].strip()
        refs_raw = raw["allowed_reference_objects"].strip()
        if not proof_id or not symbol or not owner:
            raise ProofFailure(PROOF_INTERNAL,
                               f"empty manifest field at row {lineno}")
        if not re.fullmatch(r"JIT-[A-Z0-9-]+", proof_id):
            raise ProofFailure(PROOF_INTERNAL,
                               f"invalid proof ID at row {lineno}")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", symbol):
            raise ProofFailure(PROOF_INTERNAL,
                               f"invalid symbol at row {lineno}")
        if proof_id in proof_ids:
            raise ProofFailure(PROOF_INTERNAL,
                               f"duplicate proof ID {proof_id}")
        if symbol in symbols:
            raise ProofFailure(PROOF_INTERNAL,
                               f"duplicate symbol {symbol}")
        proof_ids.add(proof_id)
        symbols.add(symbol)
        if refs_raw in ("", "-"):
            refs: Tuple[str, ...] = ()
        else:
            refs = tuple(item.strip() for item in refs_raw.split(","))
            if any(not item for item in refs) or len(set(refs)) != len(refs):
                raise ProofFailure(PROOF_INTERNAL,
                                   f"invalid references at row {lineno}")
        rows.append(ManifestRow(proof_id, symbol, owner, refs))

    if not rows:
        raise ProofFailure(PROOF_INTERNAL, "empty ownership manifest")
    return rows


def parse_nm_posix(output: str) -> Tuple[Tuple[str, str], ...]:
    """Parse `nm -P -g` output without guessing non-POSIX layouts."""

    records: List[Tuple[str, str]] = []
    for lineno, line in enumerate(output.splitlines(), start=1):
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) < 2 or len(fields[1]) != 1:
            raise ProofFailure(PROOF_INTERNAL,
                               f"malformed nm output at line {lineno}")
        symbol = fields[0]
        symbol_type = fields[1]
        if symbol.startswith("_"):
            symbol = symbol[1:]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$.@]*", symbol):
            raise ProofFailure(PROOF_INTERNAL,
                               f"unsupported nm symbol at line {lineno}")
        if not re.fullmatch(r"[A-Za-z?]", symbol_type):
            raise ProofFailure(PROOF_INTERNAL,
                               f"unsupported nm type at line {lineno}")
        records.append((symbol, symbol_type))
    return tuple(records)


@dataclass(frozen=True)
class LinkMap:
    objects: Tuple[str, ...]
    symbol_owners: Mapping[str, Tuple[str, ...]]

    def symbol_owner(self, symbol: str) -> str | None:
        owners = self.symbol_owners.get(symbol, ())
        if not owners:
            return None
        if len(owners) != 1:
            raise ProofFailure(PROOF_INTERNAL,
                               f"ambiguous map owner for {symbol}")
        return owners[0]


def canonical_object_name(value: str, root: Path | None = None) -> str:
    """Canonicalize a plain object path without hiding archive membership."""

    if value == "linker synthesized":
        return value
    archive_member = re.fullmatch(r"(.+\.(?:a|lib))\(([^()]+)\)", value)
    if archive_member:
        archive_path = Path(archive_member.group(1))
        if not archive_path.is_absolute():
            if root is None:
                raise ProofFailure(PROOF_INTERNAL,
                                   "relative archive without object root")
            archive_path = root / archive_path
        archive = str(archive_path.resolve(strict=False))
        return f"{archive}({archive_member.group(2)})"
    path = Path(value)
    if not path.is_absolute():
        if root is None:
            raise ProofFailure(PROOF_INTERNAL,
                               "relative object without object root")
        path = root / path
    return str(path.resolve(strict=False))


def parse_link_map(text: str, object_root: Path | None = None) -> LinkMap:
    """Parse Darwin or GNU linker maps with exact object attribution."""

    if "# Object files:" in text and "# Symbols:" in text:
        return _parse_darwin_link_map(text, object_root)
    if "Linker script and memory map" in text:
        return _parse_gnu_link_map(text, object_root)
    raise ProofFailure(PROOF_INTERNAL, "unrecognized linker map format")


def _parse_darwin_link_map(
        text: str, object_root: Path | None = None) -> LinkMap:
    section = ""
    indexes: Dict[int, str] = {}
    owners: Dict[str, List[str]] = {}
    object_re = re.compile(r"^\[\s*(\d+)\]\s+(.+?)\s*$")
    symbol_re = re.compile(
        r"^0x[0-9A-Fa-f]+\s+0x[0-9A-Fa-f]+\s+"
        r"\[\s*(\d+)\]\s+(.+?)\s*$")

    for line in text.splitlines():
        if line == "# Object files:":
            section = "objects"
            continue
        if line == "# Symbols:":
            section = "symbols"
            continue
        if line.startswith("# Sections:"):
            section = "sections"
            continue
        if section == "objects":
            match = object_re.match(line)
            if not match:
                if line.startswith("#") or not line.strip():
                    continue
                raise ProofFailure(PROOF_INTERNAL,
                                   "malformed Darwin object map row")
            index = int(match.group(1))
            path = canonical_object_name(match.group(2), object_root)
            if index in indexes:
                raise ProofFailure(PROOF_INTERNAL,
                                   f"ambiguous object index {index}")
            indexes[index] = path
        elif section == "symbols":
            match = symbol_re.match(line)
            if not match:
                if line.startswith("#") or not line.strip():
                    continue
                raise ProofFailure(PROOF_INTERNAL,
                                   "malformed Darwin symbol map row")
            index = int(match.group(1))
            if index not in indexes:
                raise ProofFailure(PROOF_INTERNAL,
                                   f"unknown object index {index}")
            symbol = match.group(2)
            if symbol.startswith("_"):
                symbol = symbol[1:]
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$.@]*", symbol):
                continue
            owners.setdefault(symbol, []).append(indexes[index])

    real_objects = tuple(path for index, path in sorted(indexes.items())
                         if index != 0 and path != "linker synthesized")
    if not real_objects:
        raise ProofFailure(PROOF_INTERNAL, "linker map has no input objects")
    return LinkMap(real_objects,
                   {name: tuple(paths) for name, paths in owners.items()})


def _parse_gnu_link_map(
        text: str, object_root: Path | None = None) -> LinkMap:
    """Parse GNU map input and symbol rows without source inspection."""

    objects: List[str] = []
    owners: Dict[str, List[str]] = {}
    current_object: str | None = None
    object_re = re.compile(r"(?:^|\s)(\S+\.o)(?:\)|\s|$)")
    symbol_re = re.compile(
        r"^\s*0x[0-9A-Fa-f]+\s+([A-Za-z_][A-Za-z0-9_$.@]*)\s*$")
    in_map = False

    for line in text.splitlines():
        if line.strip() == "Linker script and memory map":
            in_map = True
            continue
        if not in_map:
            continue
        object_match = object_re.search(line)
        if object_match:
            current_object = canonical_object_name(
                object_match.group(1), object_root)
            if current_object not in objects:
                objects.append(current_object)
        symbol_match = symbol_re.match(line)
        if symbol_match and current_object is not None:
            symbol = symbol_match.group(1)
            owners.setdefault(symbol, []).append(current_object)

    if not objects:
        raise ProofFailure(PROOF_INTERNAL, "GNU linker map has no objects")
    return LinkMap(tuple(objects),
                   {name: tuple(paths) for name, paths in owners.items()})


def verify_ownership(
        rows: Sequence[ManifestRow],
        object_symbols: Mapping[str, Sequence[Tuple[str, str]]],
        link_map: LinkMap) -> List[str]:
    """Return only named semantic failures; prerequisites raise class 2/3."""

    failures: List[str] = []
    mapped_objects = set(link_map.objects)
    supplied_objects = set(object_symbols)
    for row in rows:
        if row.defining_object not in supplied_objects:
            raise ProofFailure(
                ENVIRONMENT_FAILURE,
                f"missing expected object {row.defining_object}")
        if row.defining_object not in mapped_objects:
            raise ProofFailure(
                ENVIRONMENT_FAILURE,
                f"expected object absent from link map {row.defining_object}")

        definitions: List[Tuple[str, str]] = []
        weak_aliases: List[str] = []
        references: List[str] = []
        for object_path, symbols in object_symbols.items():
            if object_path not in mapped_objects:
                raise ProofFailure(
                    ENVIRONMENT_FAILURE,
                    f"object absent from link map {object_path}")
            for symbol, symbol_type in symbols:
                if symbol != row.symbol:
                    continue
                normalized_type = symbol_type.upper()
                if normalized_type == "U":
                    references.append(object_path)
                elif normalized_type in ("C", "I", "V", "W"):
                    weak_aliases.append(object_path)
                elif symbol_type.isupper() and normalized_type in (
                        "A", "B", "D", "R", "S", "T"):
                    definitions.append((object_path, symbol_type))
                else:
                    raise ProofFailure(
                        PROOF_INTERNAL,
                        f"unsupported nm type {symbol_type} for {row.symbol}")

        if weak_aliases:
            failures.append(f"{row.proof_id}:unexpected-alias")
            continue
        if not definitions:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               f"missing expected symbol {row.symbol}")
        if len(definitions) != 1:
            failures.append(f"{row.proof_id}:duplicate-definition")
            continue
        if definitions[0][0] != row.defining_object:
            failures.append(f"{row.proof_id}:foreign-definition")
            continue
        map_owner = link_map.symbol_owner(row.symbol)
        if map_owner is None:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               f"symbol absent from link map {row.symbol}")
        if map_owner != row.defining_object:
            failures.append(f"{row.proof_id}:foreign-map-owner")
            continue

        allowed = set(row.allowed_references)
        allowed.add(row.defining_object)
        if any(reference not in allowed for reference in references):
            failures.append(f"{row.proof_id}:forbidden-reference")
    return failures


def audit_flag_delta(
        real_flags: Sequence[str]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Remove only LTO controls for the non-LTO ownership audit relink."""

    audit: List[str] = []
    removed: List[str] = []
    for flag in real_flags:
        is_lto = (flag == "-flto" or flag.startswith("-flto=") or
                  flag == "-fuse-linker-plugin" or
                  flag.startswith("-Wl,-plugin,") or
                  flag.startswith("-Wl,-plugin-opt,"))
        if is_lto:
            removed.append(flag)
        else:
            audit.append(flag)
    if not removed:
        raise ProofFailure(PROOF_INTERNAL,
                           "audit relink requested without LTO flags")
    return tuple(audit), tuple(removed)


def validate_audit_flag_delta(
        real_flags: Sequence[str],
        audit_flags: Sequence[str]) -> Tuple[str, ...]:
    """Require the audit flags to differ only by exact LTO controls."""

    expected, removed = audit_flag_delta(real_flags)
    if tuple(audit_flags) != expected:
        raise ProofFailure(PROOF_INTERNAL,
                           "non-LTO audit changed non-LTO flags")
    return removed


def hash_named_inputs(inputs: Iterable[Tuple[str, bytes]]) -> str:
    """Hash a stable name/content set without depending on filesystem paths."""

    digest = hashlib.sha256()
    seen = set()
    for name, content in sorted(inputs):
        if name in seen:
            raise ProofFailure(PROOF_INTERNAL,
                               f"duplicate provenance input {name}")
        seen.add(name)
        encoded_name = name.encode("utf-8")
        digest.update(len(encoded_name).to_bytes(8, "big"))
        digest.update(encoded_name)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    if not seen:
        raise ProofFailure(PROOF_INTERNAL, "empty provenance input set")
    return digest.hexdigest()


def normalize_fixture_flags(
        flags: Sequence[str], fixture_root: Path) -> Tuple[str, ...]:
    """Remove only the ephemeral self-fixture prefix from provenance."""

    prefix = str(fixture_root)
    return tuple(flag.replace(prefix, "$JIT_FIXTURE") for flag in flags)


def resolve_manifest_rows(
        rows: Sequence[ManifestRow], root: Path) -> Tuple[ManifestRow, ...]:
    """Bind declarative relative object names to one explicit build root."""

    def resolve(value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            path = root / path
        return str(path.resolve(strict=False))

    return tuple(ManifestRow(row.proof_id, row.symbol,
                             resolve(row.defining_object),
                             tuple(resolve(item)
                                   for item in row.allowed_references))
                 for row in rows)


def parse_runtime_observations(output: str):
    control_seen = False
    verdicts: Dict[str, str] = {}
    for lineno, line in enumerate(output.splitlines(), start=1):
        if not line:
            continue
        fields = line.split("\t")
        if fields == ["JIT_CONTROL", "HEADER-COMPILE-LINK-RUN", "PASS"]:
            if control_seen:
                raise ProofFailure(PROOF_INTERNAL,
                                   "duplicate runtime control")
            control_seen = True
            continue
        if len(fields) != 5 or fields[0] != "JIT_OBSERVATION":
            raise ProofFailure(PROOF_INTERNAL,
                               f"unexpected runtime output at line {lineno}")
        proof_id, verdict, observed, expected = fields[1:]
        if proof_id not in ABI_PROOF_IDS:
            raise ProofFailure(PROOF_INTERNAL,
                               f"unknown runtime proof ID {proof_id}")
        if proof_id in verdicts:
            raise ProofFailure(PROOF_INTERNAL,
                               f"duplicate runtime proof ID {proof_id}")
        if verdict not in ("PASS", "FAIL") or not expected:
            raise ProofFailure(PROOF_INTERNAL,
                               f"malformed runtime verdict {proof_id}")
        if (verdict == "PASS" and observed != "1") or \
                (verdict == "FAIL" and observed != "0"):
            raise ProofFailure(PROOF_INTERNAL,
                               f"inconsistent runtime verdict {proof_id}")
        verdicts[proof_id] = verdict

    if not control_seen:
        raise ProofFailure(PROOF_INTERNAL, "missing runtime positive control")
    missing = [proof_id for proof_id in ABI_PROOF_IDS
               if proof_id not in verdicts]
    if missing:
        raise ProofFailure(PROOF_INTERNAL,
                           "missing runtime proof IDs: " + ",".join(missing))
    return tuple(proof_id for proof_id in ABI_PROOF_IDS
                 if verdicts[proof_id] == "FAIL")


@dataclass(frozen=True)
class BuildConfig:
    compiler: Tuple[str, ...]
    cflags: Tuple[str, ...]
    cppflags: Tuple[str, ...]
    include_root: Path
    unit_root: Path
    port_library: Path


def _run(argv: Sequence[str], cwd: Path, timeout: int = 30):
    try:
        return subprocess.run(
            tuple(argv), cwd=str(cwd), text=True, capture_output=True,
            timeout=timeout, check=False)
    except (FileNotFoundError, PermissionError) as exc:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"tool unavailable: {argv[0]}: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"tool timeout: {argv[0]}") from exc


def _pg_config_value(pg_config: Path, option: str, root: Path) -> str:
    if not pg_config.is_file() or not os.access(pg_config, os.X_OK):
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"pg_config unavailable: {pg_config}")
    result = _run((str(pg_config), option), root)
    if result.returncode != 0 or result.stderr:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"pg_config failed for {option}")
    return result.stdout.strip()


def load_build_config(
        root: Path, compiler_override: str | None = None,
        include_override: str | None = None,
        port_library_override: str | None = None) -> BuildConfig:
    pg_config = root / "src/bin/pg_config/pg_config"
    compiler = (tuple(shlex.split(compiler_override))
                if compiler_override is not None else
                tuple(shlex.split(_pg_config_value(pg_config, "--cc", root))))
    if not compiler or shutil.which(compiler[0]) is None:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "configured compiler is unavailable")
    cflags = tuple(shlex.split(_pg_config_value(pg_config, "--cflags", root)))
    cppflags = tuple(shlex.split(
        _pg_config_value(pg_config, "--cppflags", root)))
    include_root = (resolve_input_path(include_override, root)
                    if include_override is not None else
                    (root / "src/include").resolve())
    unit_root = (root / "src/test/cluster_unit").resolve()
    port_library = (resolve_input_path(port_library_override, root)
                    if port_library_override is not None else
                    (root / "src/port/libpgport.a").resolve())
    if not include_root.is_dir() or not unit_root.is_dir():
        raise ProofFailure(ENVIRONMENT_FAILURE, "include root unavailable")
    if not port_library.is_file():
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"port library unavailable: {port_library}")
    return BuildConfig(compiler, cflags, cppflags, include_root, unit_root,
                       port_library)


def _object_compile_flags(
        config: BuildConfig, fixture_root: Path,
        lto_flags: Sequence[str] = ()) -> Tuple[str, ...]:
    return (config.cflags + config.cppflags +
            ("-I", str(fixture_root), "-I", str(config.unit_root),
             "-I", str(config.include_root), "-DFRONTEND") +
            tuple(lto_flags))


def _compile_fixture_objects(
        config: BuildConfig, sources: Sequence[Path], object_root: Path,
        flags: Sequence[str]) -> Tuple[Path, ...]:
    objects: List[Path] = []
    for source in sources:
        output = object_root / f"{source.stem}.o"
        result = _run(config.compiler + tuple(flags) +
                      ("-c", str(source), "-o", str(output)),
                      config.unit_root)
        if result.returncode != 0 or result.stderr:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               f"object positive control failed: {source.name}")
        if not output.is_file():
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               f"compiler omitted object: {output}")
        objects.append(output.resolve())
    return tuple(objects)


def _link_map_option(path: Path) -> str:
    if sys.platform == "darwin":
        return f"-Wl,-map,{path}"
    return f"-Wl,-Map,{path}"


def _link_fixture(
        config: BuildConfig, objects: Sequence[Path], binary: Path,
        link_map_path: Path, link_flags: Sequence[str] = ()) -> LinkMap:
    result = _run(config.compiler + tuple(link_flags) +
                  tuple(str(path) for path in objects) +
                  (_link_map_option(link_map_path), "-o", str(binary)),
                  config.unit_root)
    if result.returncode != 0 or result.stderr:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "link-map positive control failed")
    if not binary.is_file() or not link_map_path.is_file():
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "linker omitted binary or map")
    execution = _run((str(binary),), config.unit_root)
    if (execution.returncode != 0 or execution.stdout or execution.stderr):
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "linked positive control did not execute cleanly")
    try:
        map_text = link_map_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"link map unreadable: {link_map_path}") from exc
    return parse_link_map(map_text, config.unit_root)


def _tool_command(command: str | None, fallback: str) -> Tuple[str, ...]:
    words = tuple(shlex.split(command)) if command is not None else (fallback,)
    if not words or shutil.which(words[0]) is None:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"object tool unavailable: {words[0] if words else '-'}")
    return words


def _read_object_symbols(
        nm_command: Sequence[str], objects: Sequence[Path],
        cwd: Path) -> Mapping[str, Tuple[Tuple[str, str], ...]]:
    result: Dict[str, Tuple[Tuple[str, str], ...]] = {}
    for object_path in objects:
        invocation = tuple(nm_command) + ("-P", "-g", str(object_path))
        completed = _run(invocation, cwd)
        if completed.returncode != 0 or completed.stderr:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               f"object tool failed: {object_path}")
        result[str(object_path.resolve())] = parse_nm_posix(completed.stdout)
    return result


def _fixture_sources(root: Path) -> Tuple[Tuple[Path, ...], Path]:
    generated = root / "jit_generated.h"
    owner = root / "owner.c"
    reference = root / "reference.c"
    main_source = root / "main.c"
    generated.write_text("#define JIT_GENERATED_VALUE 7\n", encoding="utf-8")
    owner.write_text(
        '#include "postgres_fe.h"\n#include "jit_generated.h"\n'
        'int jit_proof_owned(void);\n'
        'int jit_proof_owned(void) { return JIT_GENERATED_VALUE; }\n',
        encoding="utf-8")
    reference.write_text(
        '#include "postgres_fe.h"\n'
        'int jit_proof_owned(void);\nint jit_proof_reference(void);\n'
        'int jit_proof_reference(void) { return jit_proof_owned(); }\n',
        encoding="utf-8")
    main_source.write_text(
        '#include "postgres_fe.h"\nint jit_proof_reference(void);\n'
        'int main(void) { return jit_proof_reference() == 7 ? 0 : 1; }\n',
        encoding="utf-8")
    return (owner, reference, main_source), generated


def _verify_fixture_ownership(
        objects: Sequence[Path], link_map: LinkMap,
        nm_command: Sequence[str], cwd: Path) -> None:
    by_stem = {path.stem: path for path in objects}
    if set(by_stem) != {"main", "owner", "reference"}:
        raise ProofFailure(PROOF_INTERNAL,
                           "unexpected object control inventory")
    owner = str(by_stem["owner"].resolve())
    reference = str(by_stem["reference"].resolve())
    row = ManifestRow("JIT-CTRL-EXACT-OWNER", "jit_proof_owned",
                      owner, (reference,))
    symbols = _read_object_symbols(nm_command, objects, cwd)
    failures = verify_ownership((row,), symbols, link_map)
    if failures:
        raise ProofFailure(PROOF_INTERNAL,
                           "known-good object control was rejected")


def run_native_controls(
        root: Path, compiler_override: str | None = None,
        include_override: str | None = None,
        port_library_override: str | None = None,
        nm_override: str | None = None) -> Mapping[str, str]:
    """Run real compiler/object/map and non-skipping LTO audit controls."""

    config = load_build_config(root, compiler_override, include_override,
                               port_library_override)
    nm_command = _tool_command(nm_override, "nm")
    version = _run(config.compiler + ("--version",), root)
    if version.returncode != 0 or version.stderr or not version.stdout:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "compiler version control failed")

    with tempfile.TemporaryDirectory(prefix="pgrac-jit-objects-") as name:
        temporary = Path(name).resolve()
        sources, generated = _fixture_sources(temporary)

        ordinary_root = temporary / "ordinary"
        ordinary_root.mkdir()
        ordinary_flags = _object_compile_flags(config, temporary)
        ordinary_objects = _compile_fixture_objects(
            config, sources, ordinary_root, ordinary_flags)
        ordinary_map = _link_fixture(
            config, ordinary_objects, temporary / "ordinary-control",
            temporary / "ordinary.map")
        _verify_fixture_ownership(ordinary_objects, ordinary_map,
                                  nm_command, config.unit_root)

        real_root = temporary / "real-lto"
        real_root.mkdir()
        real_flags = _object_compile_flags(config, temporary, ("-flto",))
        real_objects = _compile_fixture_objects(
            config, sources, real_root, real_flags)
        _link_fixture(config, real_objects, temporary / "real-lto-control",
                      temporary / "real-lto.map", ("-flto",))

        audit_flags, removed_compile = audit_flag_delta(real_flags)
        validate_audit_flag_delta(real_flags, audit_flags)
        audit_link_flags, removed_link = audit_flag_delta(("-flto",))
        validate_audit_flag_delta(("-flto",), audit_link_flags)
        audit_root = temporary / "audit"
        audit_root.mkdir()
        audit_objects = _compile_fixture_objects(
            config, sources, audit_root, audit_flags)
        audit_map = _link_fixture(
            config, audit_objects, temporary / "audit-control",
            temporary / "audit.map", audit_link_flags)
        _verify_fixture_ownership(audit_objects, audit_map,
                                  nm_command, config.unit_root)

        source_hash = hash_named_inputs(
            tuple((path.name, path.read_bytes()) for path in sources))
        generated_hash = hash_named_inputs(
            ((generated.name, generated.read_bytes()),))
        object_list_hash = hash_named_inputs((
            ("object-list", "\n".join(
                f"{path.name}->{path.stem}.o" for path in sources
            ).encode("utf-8")),
        ))
        recorded_real_flags = normalize_fixture_flags(real_flags, temporary)
        recorded_audit_flags = normalize_fixture_flags(audit_flags, temporary)
        flag_hash = hash_named_inputs((
            ("real-compile-flags",
             "\0".join(recorded_real_flags).encode("utf-8")),
            ("audit-compile-flags",
             "\0".join(recorded_audit_flags).encode("utf-8")),
            ("real-link-flags", b"-flto"),
            ("audit-link-flags", b""),
        ))
        return {
            "compiler_sha256": hashlib.sha256(
                version.stdout.encode("utf-8")).hexdigest(),
            "source_set_sha256": source_hash,
            "object_list_sha256": object_list_hash,
            "generated_inputs_sha256": generated_hash,
            "flag_pair_sha256": flag_hash,
            "removed_flags": ",".join(removed_compile + removed_link),
        }


def run_controls(args) -> int:
    root = repository_root(Path(__file__))
    receipt = run_native_controls(root, args.cc, args.include_root,
                                  args.port_library, args.nm)
    print("JIT_CONTROL:OBJECT-LINK-MAP:PASS")
    print("JIT_CONTROL:LTO-REAL-LINK-RUN:PASS")
    print("JIT_CONTROL:LTO-AUDIT-RELINK:PASS")
    for key in sorted(receipt):
        print(f"JIT_PROVENANCE:{key.upper()}:{receipt[key]}")
    return PASS


def run_ownership(args) -> int:
    root = repository_root(Path(__file__))
    manifest_path = resolve_input_path(args.manifest, root)
    link_map_path = resolve_input_path(args.link_map, root)
    object_root = (resolve_input_path(args.object_root, root)
                   if args.object_root else root)
    objects = tuple(resolve_input_path(item, object_root)
                    for item in args.object)
    if not manifest_path.is_file():
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"manifest unavailable: {manifest_path}")
    if not link_map_path.is_file():
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"link map unavailable: {link_map_path}")
    missing = [str(path) for path in objects if not path.is_file()]
    if missing:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "object unavailable: " + ",".join(missing))

    run_native_controls(root, args.cc, args.include_root,
                        args.port_library, args.nm)
    try:
        with manifest_path.open("r", encoding="utf-8", newline="") as stream:
            rows = resolve_manifest_rows(parse_manifest(stream), object_root)
        parsed_map = parse_link_map(
            link_map_path.read_text(encoding="utf-8"), object_root)
    except OSError as exc:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "ownership input unreadable") from exc
    nm_command = _tool_command(args.nm, "nm")
    symbols = _read_object_symbols(nm_command, objects, root)
    failures = verify_ownership(rows, symbols, parsed_map)
    print("JIT_CONTROL:OBJECT-LINK-MAP:PASS")
    for failure in failures:
        print(f"JIT_SEMANTIC_RED:{failure}")
    if failures:
        return SEMANTIC_RED
    print("JIT_PROOF:OWNERSHIP:PASS")
    return PASS


def _compile_argv(config: BuildConfig, source: Path, output: Path,
                  extra_flags: Sequence[str] = ()) -> Tuple[str, ...]:
    return (config.compiler + config.cflags + config.cppflags +
            ("-I", str(config.unit_root), "-I", str(config.include_root),
             "-DFRONTEND") + tuple(extra_flags) +
            (str(source), str(config.port_library), "-o", str(output)))


def _expected_instrumentation(stderr: str) -> None:
    warnings = [line for line in stderr.splitlines() if "warning:" in line]
    if not warnings:
        raise ProofFailure(PROOF_INTERNAL,
                           "StaticAssertDecl instrumentation was not observed")
    if any("JIT_STATIC_ASSERT_DECL:" not in line for line in warnings):
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "unexpected compiler diagnostic")


def _static_assert_red(stderr: str) -> Tuple[str, ...]:
    matches = tuple(proof_id for proof_id in ABI_PROOF_IDS
                    if proof_id in stderr)
    error_lines = [line for line in stderr.splitlines() if "error:" in line]
    if matches and error_lines and all(
            any(proof_id in line for proof_id in matches)
            for line in error_lines):
        return matches
    return ()


def _check_retired_scalar_names(config: BuildConfig, temporary: Path) -> None:
    positive_source = temporary / "scalar_positive.c"
    positive_binary = temporary / "scalar_positive"
    positive_source.write_text(
        '#include "postgres_fe.h"\n#include "access/xlogrecord.h"\n'
        'int main(void) { return sizeof(XLogRecord) == 0; }\n',
        encoding="utf-8")
    positive = _run(_compile_argv(config, positive_source, positive_binary),
                    config.unit_root)
    if positive.returncode != 0 or positive.stderr:
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           "scalar absence positive compiler control failed")

    retired = (
        "ClusterFailureGenerationToken",
        "ClusterFailureGenerationReadResult",
        "failure_generation",
    )
    for ordinal, identifier in enumerate(retired):
        source = temporary / f"scalar_retired_{ordinal}.c"
        binary = temporary / f"scalar_retired_{ordinal}"
        source.write_text(
            '#include "postgres_fe.h"\n#include "access/xlogrecord.h"\n'
            f'int main(void) {{ return (int) sizeof({identifier}); }}\n',
            encoding="utf-8")
        result = _run(_compile_argv(config, source, binary), config.unit_root)
        if result.returncode == 0:
            raise ProofFailure(
                SEMANTIC_RED,
                "JIT-A-T2-SCALAR-GENERATION-ABSENT:retired-name-accepted")
        if identifier not in result.stderr:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               "unexpected retired-name compiler diagnostic")


def run_abi(args) -> int:
    root = repository_root(Path(__file__))
    config = load_build_config(root, args.cc, args.include_root,
                               args.port_library)
    source = (resolve_input_path(args.source, root) if args.source else
              config.unit_root / "test_cluster_jit_abi_contract.c")
    if not source.is_file():
        raise ProofFailure(ENVIRONMENT_FAILURE,
                           f"ABI source unavailable: {source}")

    with tempfile.TemporaryDirectory(prefix="pgrac-jit-abi-") as temp_name:
        temporary = Path(temp_name)
        control_source = temporary / "control.c"
        control_binary = temporary / "control"
        control_source.write_text(
            '#include "postgres_fe.h"\n'
            'typedef struct JitProofControl { uint8 bytes[16]; uint64 token; } '
            'JitProofControl;\n'
            'StaticAssertDecl(sizeof(JitProofControl) == 24, '
            '"JIT proof control layout");\n'
            'int main(void) { JitProofControl c = {{0}, 0}; '
            'return c.bytes[0] != 0; }\n',
            encoding="utf-8")
        control_compile = _run(
            _compile_argv(config, control_source, control_binary),
            config.unit_root)
        if control_compile.returncode != 0 or control_compile.stderr:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               "compiler/include/link positive control failed")
        control_run = _run((str(control_binary),), config.unit_root)
        if (control_run.returncode != 0 or control_run.stdout or
                control_run.stderr):
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               "compiler runtime positive control failed")

        probe_binary = temporary / "abi-probe"
        compile_result = _run(
            _compile_argv(config, source, probe_binary,
                          ("-DJIT_PROBE_STATIC_ASSERTS=1",)),
            config.unit_root)
        if compile_result.returncode != 0:
            named = _static_assert_red(compile_result.stderr)
            if named:
                print("JIT_CONTROL:COMPILER-INCLUDE-LINK:PASS")
                for proof_id in named:
                    print(f"JIT_SEMANTIC_RED:{proof_id}")
                return SEMANTIC_RED
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               "ordinary ABI compile/link failure")
        _expected_instrumentation(compile_result.stderr)

        probe_run = _run((str(probe_binary),), config.unit_root)
        if probe_run.stderr:
            raise ProofFailure(ENVIRONMENT_FAILURE,
                               "ABI probe emitted stderr")
        failures = parse_runtime_observations(probe_run.stdout)
        if failures and probe_run.returncode != SEMANTIC_RED:
            raise ProofFailure(PROOF_INTERNAL,
                               "ABI probe failure/result mismatch")
        if not failures and probe_run.returncode != PASS:
            raise ProofFailure(PROOF_INTERNAL,
                               "ABI probe pass/result mismatch")

        if "JIT-A-T2-PAGEVERSION-TYPE" not in failures:
            required_messages = (
                "JIT_STATIC_ASSERT_DECL:RfPageVersionV1 ABI drift",
                "JIT_STATIC_ASSERT_DECL:RfPageVersionV1 token offset",
            )
            if any(item not in compile_result.stderr
                   for item in required_messages):
                failures = tuple(failures) + (
                    "JIT-A-T2-PAGEVERSION-TYPE",)
        if "JIT-A-T2-SCALAR-GENERATION-ABSENT" not in failures:
            _check_retired_scalar_names(config, temporary)

        print("JIT_CONTROL:COMPILER-INCLUDE-LINK-RUN:PASS")
        print("JIT_CONTROL:STATICASSERT-INSTRUMENTATION:PASS")
        for proof_id in ABI_PROOF_IDS:
            if proof_id in failures:
                print(f"JIT_SEMANTIC_RED:{proof_id}")
        if failures:
            return SEMANTIC_RED
        print("JIT_PROOF:ABI:PASS")
        return PASS


class ProofProviderSelfTests(unittest.TestCase):
    """Self-fixtures name the checker mutations that must be rejected."""

    def test_repository_root_ignores_cwd(self) -> None:
        script = Path("/repo/src/test/cluster_unit/check_cluster_jit_proof.py")
        self.assertEqual(repository_root(script), Path("/repo"))

    def test_manifest_accepts_declarative_row(self) -> None:
        rows = parse_manifest(io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            "JIT-OWN-ONE\towned\tobj/owner.o\tobj/ref.o,obj/ref2.o\n"))
        self.assertEqual(rows[0].symbol, "owned")
        self.assertEqual(rows[0].allowed_references,
                         ("obj/ref.o", "obj/ref2.o"))

    def test_manifest_rejects_duplicate_proof_id(self) -> None:
        fixture = io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            "JIT-OWN-DUP\tone\towner.o\t-\n"
            "JIT-OWN-DUP\ttwo\towner.o\t-\n")
        with self.assertRaisesRegex(ProofFailure, "duplicate proof ID") as exc:
            parse_manifest(fixture)
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_manifest_rejects_duplicate_symbol(self) -> None:
        fixture = io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            "JIT-OWN-A\tone\towner.o\t-\n"
            "JIT-OWN-B\tone\towner.o\t-\n")
        with self.assertRaisesRegex(ProofFailure, "duplicate symbol") as exc:
            parse_manifest(fixture)
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_nm_parser_accepts_normal_readable_object(self) -> None:
        symbols = parse_nm_posix("_owned T 0 0\n_owned U 0 0\n")
        self.assertEqual(symbols, (("owned", "T"), ("owned", "U")))

    def test_nm_parser_rejects_malformed_output(self) -> None:
        with self.assertRaises(ProofFailure) as exc:
            parse_nm_posix("not-a-posix-nm-row\n")
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_map_parser_accepts_darwin_owner(self) -> None:
        parsed = parse_link_map(
            "# Object files:\n"
            "[  1] /tmp/owner.o\n"
            "[  2] /tmp/ref.o\n"
            "# Symbols:\n"
            "# Address Size File Name\n"
            "0x1 0x8 [  1] _owned\n"
            "0x9 0x6 [  1] literal string: value\n")
        self.assertEqual(parsed.symbol_owner("owned"),
                         canonical_object_name("/tmp/owner.o"))

    def test_map_parser_rejects_ambiguous_object_index(self) -> None:
        fixture = (
            "# Object files:\n"
            "[  1] /tmp/owner.o\n"
            "[  1] /tmp/other.o\n"
            "# Symbols:\n"
            "0x1 0x8 [  1] _owned\n")
        with self.assertRaises(ProofFailure) as exc:
            parse_link_map(fixture)
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_map_parser_accepts_gnu_section_owned_symbol(self) -> None:
        parsed = parse_link_map(
            "Linker script and memory map\n"
            " .text.jit_proof_owned\n"
            "                0x0000000000001000 0xb obj/owner.o\n"
            "                0x0000000000001000 jit_proof_owned\n",
            Path("/build"))
        self.assertEqual(parsed.symbol_owner("jit_proof_owned"),
                         "/build/obj/owner.o")

    def test_owner_verifier_accepts_exact_definition_and_references(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        reference = canonical_object_name("/tmp/ref.o")
        rows = parse_manifest(io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            f"JIT-OWN-ONE\towned\t{owner}\t{reference}\n"))
        symbols = {
            owner: (("owned", "T"),),
            reference: (("owned", "U"),),
        }
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n[  2] /tmp/ref.o\n"
            "# Symbols:\n0x1 0x8 [  1] _owned\n")
        self.assertEqual(verify_ownership(rows, symbols, link_map), [])

    def test_owner_verifier_names_duplicate_strong_definition_red(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        alias = canonical_object_name("/tmp/alias.o")
        reference = canonical_object_name("/tmp/ref.o")
        rows = parse_manifest(io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            f"JIT-OWN-DUPDEF\towned\t{owner}\t{reference}\n"))
        symbols = {
            owner: (("owned", "T"),),
            alias: (("owned", "T"),),
            reference: (("owned", "U"),),
        }
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n[  2] /tmp/alias.o\n"
            "[  3] /tmp/ref.o\n# Symbols:\n0x1 0x8 [  1] _owned\n")
        failures = verify_ownership(rows, symbols, link_map)
        self.assertEqual(failures, ["JIT-OWN-DUPDEF:duplicate-definition"])

    def test_owner_verifier_names_forbidden_reference_red(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        reference = canonical_object_name("/tmp/ref.o")
        foreign = canonical_object_name("/tmp/foreign.o")
        rows = parse_manifest(io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            f"JIT-OWN-REF\towned\t{owner}\t{reference}\n"))
        symbols = {
            owner: (("owned", "T"),),
            foreign: (("owned", "U"),),
        }
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n[  2] /tmp/foreign.o\n"
            "# Symbols:\n0x1 0x8 [  1] _owned\n")
        failures = verify_ownership(rows, symbols, link_map)
        self.assertEqual(failures, ["JIT-OWN-REF:forbidden-reference"])

    def test_owner_verifier_names_foreign_definition_red(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        foreign = canonical_object_name("/tmp/foreign.o")
        rows = (ManifestRow("JIT-OWN-FOREIGN", "owned", owner, ()),)
        symbols = {
            owner: (),
            foreign: (("owned", "T"),),
        }
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n"
            "[  2] /tmp/foreign.o\n# Symbols:\n"
            "0x1 0x8 [  2] _owned\n")
        failures = verify_ownership(rows, symbols, link_map)
        self.assertEqual(failures,
                         ["JIT-OWN-FOREIGN:foreign-definition"])

    def test_owner_verifier_rejects_common_alias(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        rows = (ManifestRow("JIT-OWN-COMMON", "owned", owner, ()),)
        symbols = {owner: (("owned", "C"),)}
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n# Symbols:\n"
            "0x1 0x8 [  1] _owned\n")
        failures = verify_ownership(rows, symbols, link_map)
        self.assertEqual(failures,
                         ["JIT-OWN-COMMON:unexpected-alias"])

    def test_owner_verifier_classifies_unknown_symbol_type_internal(self) -> None:
        owner = canonical_object_name("/tmp/owner.o")
        rows = (ManifestRow("JIT-OWN-TYPE", "owned", owner, ()),)
        symbols = {owner: (("owned", "Z"),)}
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/owner.o\n# Symbols:\n"
            "0x1 0x8 [  1] _owned\n")
        with self.assertRaises(ProofFailure) as exc:
            verify_ownership(rows, symbols, link_map)
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_owner_verifier_classifies_missing_object_as_environment(self) -> None:
        missing = canonical_object_name("/tmp/missing.o")
        reference = canonical_object_name("/tmp/ref.o")
        rows = parse_manifest(io.StringIO(
            "proof_id\tsymbol\tdefining_object\tallowed_reference_objects\n"
            f"JIT-OWN-MISSING\towned\t{missing}\t-\n"))
        link_map = parse_link_map(
            "# Object files:\n[  1] /tmp/ref.o\n# Symbols:\n")
        with self.assertRaises(ProofFailure) as exc:
            verify_ownership(rows, {reference: ()}, link_map)
        self.assertEqual(exc.exception.result_class, ENVIRONMENT_FAILURE)

    def test_lto_audit_removes_only_lto_flags(self) -> None:
        audit, removed = audit_flag_delta((
            "-O2", "-flto=thin", "-DUSE_PGRAC_CLUSTER",
            "-fuse-linker-plugin", "-Wl,-dead_strip"))
        self.assertEqual(audit, ("-O2", "-DUSE_PGRAC_CLUSTER",
                                 "-Wl,-dead_strip"))
        self.assertEqual(removed, ("-flto=thin", "-fuse-linker-plugin"))

    def test_lto_audit_rejects_any_non_lto_flag_delta(self) -> None:
        real = ("-O2", "-flto", "-DUSE_PGRAC_CLUSTER")
        with self.assertRaises(ProofFailure) as exc:
            validate_audit_flag_delta(
                real, ("-O0", "-DUSE_PGRAC_CLUSTER"))
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_named_input_hash_is_order_stable_and_content_sensitive(self) -> None:
        first = hash_named_inputs((("b.c", b"two"), ("a.c", b"one")))
        reordered = hash_named_inputs((("a.c", b"one"), ("b.c", b"two")))
        changed = hash_named_inputs((("a.c", b"ONE"), ("b.c", b"two")))
        self.assertEqual(first, reordered)
        self.assertNotEqual(first, changed)

    def test_relative_manifest_paths_bind_to_repository_root(self) -> None:
        row = ManifestRow("JIT-OWN-ONE", "owned", "obj/owner.o",
                          ("obj/ref.o",))
        resolved = resolve_manifest_rows((row,), Path("/repo"))
        self.assertEqual(resolved[0].defining_object,
                         "/repo/obj/owner.o")
        self.assertEqual(resolved[0].allowed_references,
                         ("/repo/obj/ref.o",))

    def test_relative_cli_path_binds_to_repository_root(self) -> None:
        self.assertEqual(resolve_input_path("src/object.o", Path("/repo")),
                         Path("/repo/src/object.o"))

    def test_each_named_abi_diagnostic_is_semantic_red(self) -> None:
        for failing_id in ABI_PROOF_IDS:
            with self.subTest(proof_id=failing_id):
                lines = ["JIT_CONTROL\tHEADER-COMPILE-LINK-RUN\tPASS"]
                for proof_id in ABI_PROOF_IDS:
                    verdict = "FAIL" if proof_id == failing_id else "PASS"
                    observed = "0" if verdict == "FAIL" else "1"
                    lines.append(
                        f"JIT_OBSERVATION\t{proof_id}\t{verdict}\t"
                        f"{observed}\texpected")
                failures = parse_runtime_observations("\n".join(lines) + "\n")
                self.assertEqual(failures, (failing_id,))

    def test_runtime_observation_rejects_missing_control(self) -> None:
        with self.assertRaises(ProofFailure) as exc:
            parse_runtime_observations(
                "JIT_OBSERVATION\tJIT-A-T2-PAGEVERSION-TYPE\tFAIL\t"
                "0\tdeclared\n")
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)

    def test_runtime_observation_rejects_unknown_diagnostic(self) -> None:
        fixture = "JIT_CONTROL\tHEADER-COMPILE-LINK-RUN\tPASS\n"
        fixture += "JIT_OBSERVATION\tJIT-A-T2-UNKNOWN\tFAIL\t0\tx\n"
        with self.assertRaises(ProofFailure) as exc:
            parse_runtime_observations(fixture)
        self.assertEqual(exc.exception.result_class, PROOF_INTERNAL)


def run_self_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        ProofProviderSelfTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return PASS if result.wasSuccessful() else PROOF_INTERNAL


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run compiler/object-native cluster JIT proofs")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("self-test", help="run proof-provider fixtures")
    abi = subparsers.add_parser("abi", help="run compiler-native ABI probe")
    abi.add_argument("--cc", help="override compiler command")
    abi.add_argument("--include-root", help="override production include root")
    abi.add_argument("--port-library", help="override libpgport.a path")
    abi.add_argument("--source", help="override ABI probe translation unit")
    controls = subparsers.add_parser(
        "controls", help="run real object/map and LTO audit controls")
    controls.add_argument("--cc", help="override compiler command")
    controls.add_argument("--include-root",
                          help="override production include root")
    controls.add_argument("--port-library",
                          help="override libpgport.a path")
    controls.add_argument("--nm", help="override object-tool command")
    ownership = subparsers.add_parser(
        "ownership", help="verify a populated product ownership manifest")
    ownership.add_argument("--manifest", required=True)
    ownership.add_argument("--link-map", required=True)
    ownership.add_argument("--object", action="append", required=True)
    ownership.add_argument("--object-root")
    ownership.add_argument("--cc", help="override compiler command")
    ownership.add_argument("--include-root",
                           help="override production include root")
    ownership.add_argument("--port-library",
                           help="override libpgport.a path")
    ownership.add_argument("--nm", help="override object-tool command")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--self-test"]:
        arguments = ["self-test"]
    args = argument_parser().parse_args(arguments)
    try:
        if args.command == "self-test":
            return run_self_tests()
        if args.command == "abi":
            return run_abi(args)
        if args.command == "controls":
            return run_controls(args)
        if args.command == "ownership":
            return run_ownership(args)
        raise ProofFailure(PROOF_INTERNAL, "unhandled proof command")
    except ProofFailure as exc:
        labels = {
            SEMANTIC_RED: "JIT_SEMANTIC_RED",
            ENVIRONMENT_FAILURE: "JIT_ENVIRONMENT_FAILURE",
            PROOF_INTERNAL: "JIT_PROOF_INTERNAL",
        }
        print(f"{labels[exc.result_class]}:{exc}", file=sys.stderr)
        return exc.result_class


if __name__ == "__main__":
    raise SystemExit(main())
