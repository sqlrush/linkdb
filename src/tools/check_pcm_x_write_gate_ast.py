#!/usr/bin/env python3
"""Compiler-AST coverage gate for PGRAC PCM-X shared-buffer writers.

The checker consumes Clang's textual AST, not source-text allowlists.  It
requires exactly four terminal-owner kinds and rejects any direct shared
buffer content-lock mutator that lacks a named, compiler-visible gate proof.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import shlex
import subprocess
import sys
from collections import defaultdict
from typing import Iterable, NamedTuple


TERMINAL_OWNER_PREFIX = "pgrac_pcm_x_terminal_owner:"
FENCE_DOMINATED_PREFIX = "pgrac_pcm_x_fence_dominated:"
REQUIRED_OWNER_KINDS = {
    "T2_INSTALL",
    "T3_CLEAR",
    "R8_NEUTRALIZE",
    "NS_SOURCE_PREPARE",
}
OWNER_CARDINALITY = {
    "T2_INSTALL": 1,
    "T3_CLEAR": 1,
    "R8_NEUTRALIZE": 1,
    "NS_SOURCE_PREPARE": 1,
}
PAGE_ACCESSORS = {"BufHdrGetBlock", "BufferGetPage"}
PAGE_MUTATORS = {
    "MarkBufferDirty",
    "MarkBufferDirtyHint",
    "PageSetLSN",
    "TerminateBufferIO",
    "memcpy",
    "memmove",
    "__builtin___memcpy_chk",
    "__builtin___memmove_chk",
}


class CoverageError(RuntimeError):
    """The closed-world AST proof is incomplete or contradictory."""


class FunctionFacts(NamedTuple):
    name: str
    parameters: frozenset[str]
    calls: frozenset[str]
    annotations: tuple[str, ...]
    direct_exclusive: bool
    mutates_page: bool


class CoverageReport(NamedTuple):
    owner_kinds: set[str]
    owner_functions: dict[str, set[str]]
    dominated_mutators: set[str]


_FUNCTION_START = re.compile(r"(?m)^(?=\|-FunctionDecl |`-FunctionDecl )")
_FUNCTION_NAME = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*) '[^']*'(?:\s|$)")
_PARAMETER = re.compile(
    r"ParmVarDecl [^\n]*\b(?:used\s+)?([A-Za-z_][A-Za-z0-9_]*) '[^']*'"
)
_CALLEE = re.compile(
    r"DeclRefExpr [^\n]*\bFunction [^\n]* '([A-Za-z_][A-Za-z0-9_]*)'"
)
_ANNOTATION = re.compile(r'AnnotateAttr [^\n]* "([^"]+)"')


def parse_ast_dump(text: str) -> dict[str, FunctionFacts]:
    """Return definition facts from one or more Clang textual AST dumps."""

    functions: dict[str, FunctionFacts] = {}
    for block in _FUNCTION_START.split(text):
        if not block.startswith(("|-FunctionDecl ", "`-FunctionDecl ")):
            continue
        first_line = block.splitlines()[0]
        name_match = _FUNCTION_NAME.search(first_line)
        if name_match is None or "CompoundStmt" not in block:
            continue
        name = name_match.group(1)
        calls = frozenset(_CALLEE.findall(block))
        parameters = frozenset(_PARAMETER.findall(block))
        annotations = tuple(_ANNOTATION.findall(block))
        direct_exclusive = (
            "BufferDescriptorGetContentLock" in calls
            and "'LW_EXCLUSIVE'" in block
            and bool({"LWLockAcquire", "LWLockConditionalAcquire"} & calls)
        )
        mutates_page = bool(PAGE_ACCESSORS & calls) and bool(PAGE_MUTATORS & calls)
        facts = FunctionFacts(
            name,
            parameters,
            calls,
            annotations,
            direct_exclusive,
            mutates_page,
        )
        previous = functions.get(name)
        if previous is not None and previous != facts:
            raise CoverageError(f"conflicting AST definitions for {name}")
        functions[name] = facts
    return functions


def _terminal_owner(annotation: str, function: FunctionFacts) -> str:
    fields = annotation.split(":")
    if len(fields) != 4 or fields[0] != TERMINAL_OWNER_PREFIX[:-1]:
        raise CoverageError(f"{function.name}: malformed terminal owner annotation {annotation!r}")
    _, kind, parameter, proof = fields
    if kind not in REQUIRED_OWNER_KINDS:
        raise CoverageError(f"{function.name}: unknown terminal owner kind {kind}")
    if parameter not in function.parameters:
        raise CoverageError(f"{function.name}: terminal owner parameter {parameter} is absent")
    if proof not in function.calls:
        raise CoverageError(f"{function.name}: terminal owner does not call proof {proof}")
    return kind


def audit_functions(functions: dict[str, FunctionFacts]) -> CoverageReport:
    """Apply the exact-four owner and direct-mutator closed-world rules."""

    owner_functions: dict[str, set[str]] = defaultdict(set)
    terminal_functions: set[str] = set()
    dominated_mutators: set[str] = set()

    for function in functions.values():
        terminal_annotations = tuple(
            item for item in function.annotations if item.startswith(TERMINAL_OWNER_PREFIX)
        )
        dominated_annotations = tuple(
            item for item in function.annotations if item.startswith(FENCE_DOMINATED_PREFIX)
        )
        if len(terminal_annotations) > 1:
            raise CoverageError(f"{function.name}: multiple terminal owner annotations")
        if len(dominated_annotations) > 1:
            raise CoverageError(f"{function.name}: multiple fence-dominance annotations")
        if terminal_annotations and dominated_annotations:
            raise CoverageError(f"{function.name}: terminal owner cannot also be an ordinary mutator")
        if terminal_annotations:
            kind = _terminal_owner(terminal_annotations[0], function)
            owner_functions[kind].add(function.name)
            terminal_functions.add(function.name)

    owner_kinds = set(owner_functions)
    missing = REQUIRED_OWNER_KINDS - owner_kinds
    unknown = owner_kinds - REQUIRED_OWNER_KINDS
    if missing:
        raise CoverageError("missing terminal owner kind(s): " + ", ".join(sorted(missing)))
    if unknown:
        raise CoverageError("unknown terminal owner kind(s): " + ", ".join(sorted(unknown)))
    for kind, expected in OWNER_CARDINALITY.items():
        actual = len(owner_functions[kind])
        if actual != expected:
            raise CoverageError(
                f"terminal owner kind {kind} has {actual} function(s), expected {expected}"
            )

    for function in functions.values():
        if not function.direct_exclusive or not function.mutates_page:
            continue
        if function.name in terminal_functions:
            continue
        dominated_annotations = tuple(
            item for item in function.annotations if item.startswith(FENCE_DOMINATED_PREFIX)
        )
        if not dominated_annotations:
            raise CoverageError(f"{function.name}: direct page mutator has no AST gate proof")
        fields = dominated_annotations[0].split(":")
        if len(fields) != 2 or not fields[1]:
            raise CoverageError(
                f"{function.name}: malformed fence-dominance annotation {dominated_annotations[0]!r}"
            )
        proof = fields[1]
        if proof not in function.calls:
            raise CoverageError(f"{function.name}: fence annotation does not call {proof}")
        dominated_mutators.add(function.name)

    return CoverageReport(owner_kinds, dict(owner_functions), dominated_mutators)


def _candidate_sources(source_root: pathlib.Path) -> list[pathlib.Path]:
    backend = source_root / "src" / "backend"
    sources = []
    for source in sorted(backend.rglob("*.c")):
        if "BufferDescriptorGetContentLock" in source.read_text(encoding="utf-8"):
            sources.append(source)
    if not sources:
        raise CoverageError("no BufferDescriptorGetContentLock translation unit found")
    return sources


def _clang_ast(
    clang: str,
    cppflags: str,
    cflags: str,
    compile_directory: pathlib.Path,
    source: pathlib.Path,
) -> str:
    command = [clang]
    command.extend(shlex.split(cflags))
    command.extend(shlex.split(cppflags))
    command.extend(["-fsyntax-only", "-Xclang", "-ast-dump", str(source)])
    completed = subprocess.run(
        command,
        cwd=compile_directory,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise CoverageError(
            f"AST compiler failed for {source.relative_to(source_root)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout


def audit_product(
    source_root: pathlib.Path,
    compile_directory: pathlib.Path,
    clang: str,
    cppflags: str,
    cflags: str,
) -> tuple[CoverageReport, str, int]:
    """Build an in-memory exact-flag compilation DB and audit candidate TUs."""

    sources = _candidate_sources(source_root)
    digest = hashlib.sha256()
    functions: dict[str, FunctionFacts] = {}
    for source in sources:
        canonical = "\0".join(
            [
                str(compile_directory),
                clang,
                cflags,
                cppflags,
                str(source.relative_to(source_root)),
            ]
        )
        digest.update(canonical.encode("utf-8"))
        parsed = parse_ast_dump(
            _clang_ast(clang, cppflags, cflags, compile_directory, source)
        )
        for name, facts in parsed.items():
            previous = functions.get(name)
            if previous is not None and previous != facts:
                raise CoverageError(f"conflicting AST definitions for {name}")
            functions[name] = facts
    return audit_functions(functions), digest.hexdigest(), len(sources)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, required=True)
    parser.add_argument("--compile-directory", type=pathlib.Path)
    parser.add_argument("--clang", required=True)
    parser.add_argument("--cppflags", default="")
    parser.add_argument("--cflags", default="")
    args = parser.parse_args(argv)
    try:
        source_root = args.source_root.resolve()
        compile_directory = (
            args.compile_directory.resolve()
            if args.compile_directory is not None
            else source_root
        )
        report, identity, source_count = audit_product(
            source_root, compile_directory, args.clang, args.cppflags, args.cflags
        )
    except (CoverageError, OSError) as error:
        print(f"PCM-X AST coverage: RED: {error}", file=sys.stderr)
        return 1
    owners = ", ".join(
        f"{kind}={','.join(sorted(report.owner_functions[kind]))}"
        for kind in sorted(report.owner_kinds)
    )
    print(
        f"PCM-X AST coverage: GREEN: compile_db_sha256={identity} "
        f"translation_units={source_count} owners=[{owners}] "
        f"dominated_mutators={len(report.dominated_mutators)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
