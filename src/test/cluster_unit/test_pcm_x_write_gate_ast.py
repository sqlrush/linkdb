#!/usr/bin/env python3
"""Unit tests for the compiler-AST PCM-X write-gate coverage checker."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
CHECKER_PATH = ROOT / "src" / "tools" / "check_pcm_x_write_gate_ast.py"
SPEC = importlib.util.spec_from_file_location("pcm_x_write_gate_ast", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {CHECKER_PATH}")
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def function_ast(
    name: str,
    *,
    parameters: tuple[str, ...] = (),
    calls: tuple[str, ...] = (),
    annotations: tuple[str, ...] = (),
    direct_exclusive: bool = False,
    mutates_page: bool = False,
) -> str:
    lines = [
        f"|-FunctionDecl 0x1 <fixture.c:1:1, line:20:1> line:1:1 {name} 'void (void)'",
    ]
    for parameter in parameters:
        lines.append(f"| |-ParmVarDecl 0x2 <col:1> col:1 used {parameter} 'void *'")
    lines.append("| `-CompoundStmt 0x3 <line:2:1, line:20:1>")
    if direct_exclusive:
        calls = calls + (
            "BufferDescriptorGetContentLock",
            "LWLockAcquire",
        )
        lines.append("|   |-DeclRefExpr 0x4 <col:1> 'int' EnumConstant 0x5 'LW_EXCLUSIVE' 'int'")
    if mutates_page:
        calls = calls + ("BufHdrGetBlock", "memcpy")
    for call in calls:
        lines.extend(
            [
                "|   |-CallExpr 0x6 <col:1> 'void'",
                f"|   | `-DeclRefExpr 0x7 <col:1> 'void (void)' Function 0x8 '{call}' 'void (void)'",
            ]
        )
    for annotation in annotations:
        lines.append(f'|   `-AnnotateAttr 0x9 <col:1> "{annotation}"')
    return "\n".join(lines)


def exact_owner_ast() -> str:
    owners = (
        (
            "target_t2",
            "ref",
            "cluster_pcm_x_resource_x_t2_snapshot_exact",
            "T2_INSTALL",
        ),
        (
            "target_t3",
            "ref",
            "cluster_pcm_x_resource_x_t3_snapshot_exact",
            "T3_CLEAR",
        ),
        (
            "r8_neutralize",
            "ref",
            "cluster_pcm_x_resource_x_neutralize_snapshot_exact",
            "R8_NEUTRALIZE",
        ),
        (
            "prepare_ns",
            "expected_revoking",
            "cluster_pcm_x_ns_source_revalidate_exact",
            "NS_SOURCE_PREPARE",
        ),
    )
    return "\n".join(
        function_ast(
            name,
            parameters=(parameter,),
            calls=(proof,),
            annotations=(
                f"pgrac_pcm_x_terminal_owner:{kind}:{parameter}:{proof}",
            ),
        )
        for name, parameter, proof, kind in owners
    )


class AstCoverageTests(unittest.TestCase):
    def test_exact_four_owner_annotations_require_one_shared_n_s_owner(self) -> None:
        facts = CHECKER.parse_ast_dump(exact_owner_ast())
        report = CHECKER.audit_functions(facts)

        self.assertEqual(
            report.owner_kinds,
            {
                "T2_INSTALL",
                "T3_CLEAR",
                "R8_NEUTRALIZE",
                "NS_SOURCE_PREPARE",
            },
        )
        self.assertEqual(report.owner_functions["NS_SOURCE_PREPARE"], {"prepare_ns"})

    def test_missing_r8_owner_is_red(self) -> None:
        ast = exact_owner_ast().replace(
            function_ast(
                "r8_neutralize",
                parameters=("ref",),
                calls=("cluster_pcm_x_resource_x_neutralize_snapshot_exact",),
                annotations=(
                    "pgrac_pcm_x_terminal_owner:R8_NEUTRALIZE:ref:"
                    "cluster_pcm_x_resource_x_neutralize_snapshot_exact",
                ),
            ),
            "",
        )

        with self.assertRaisesRegex(CHECKER.CoverageError, "missing terminal owner kind.*R8_NEUTRALIZE"):
            CHECKER.audit_functions(CHECKER.parse_ast_dump(ast))

    def test_fifth_owner_kind_is_red(self) -> None:
        ast = exact_owner_ast() + "\n" + function_ast(
            "rogue_owner",
            parameters=("ref",),
            calls=("rogue_proof",),
            annotations=("pgrac_pcm_x_terminal_owner:FIFTH:ref:rogue_proof",),
        )

        with self.assertRaisesRegex(CHECKER.CoverageError, "unknown terminal owner kind.*FIFTH"):
            CHECKER.audit_functions(CHECKER.parse_ast_dump(ast))

    def test_unknown_direct_exclusive_page_mutator_is_red(self) -> None:
        ast = exact_owner_ast() + "\n" + function_ast(
            "rogue_mutator", direct_exclusive=True, mutates_page=True
        )

        with self.assertRaisesRegex(CHECKER.CoverageError, "rogue_mutator.*has no AST gate proof"):
            CHECKER.audit_functions(CHECKER.parse_ast_dump(ast))

    def test_direct_mutator_requires_named_gate_call(self) -> None:
        annotation = (
            "pgrac_pcm_x_fence_dominated:"
            "cluster_bufmgr_pcm_x_content_write_permitted"
        )
        ast = exact_owner_ast() + "\n" + function_ast(
            "guarded_mutator",
            calls=("cluster_bufmgr_pcm_x_content_write_permitted",),
            annotations=(annotation,),
            direct_exclusive=True,
            mutates_page=True,
        )
        CHECKER.audit_functions(CHECKER.parse_ast_dump(ast))

        missing_call_ast = exact_owner_ast() + "\n" + function_ast(
            "lying_mutator",
            annotations=(annotation,),
            direct_exclusive=True,
            mutates_page=True,
        )
        with self.assertRaisesRegex(CHECKER.CoverageError, "lying_mutator.*does not call"):
            CHECKER.audit_functions(CHECKER.parse_ast_dump(missing_call_ast))

    def test_terminal_owner_requires_parameter_and_proof_call(self) -> None:
        ast = exact_owner_ast().replace(
            "| |-ParmVarDecl 0x2 <col:1> col:1 used ref 'void *'\n",
            "",
            1,
        )
        with self.assertRaisesRegex(CHECKER.CoverageError, "target_t2.*parameter.*ref"):
            CHECKER.audit_functions(CHECKER.parse_ast_dump(ast))


if __name__ == "__main__":
    unittest.main()
