#!/usr/bin/env python3
"""Validate docs and release-guardrail metadata for structured diagnostics."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REQUIRED_SUBCODES = {
    "sql.syntax": "errors/sql-syntax",
    "sql.relation_not_found": "errors/sql-relation-not-found",
    "sql.column_not_found": "errors/sql-column-not-found",
    "sql.ambiguous_column": "errors/sql-ambiguous-column",
    "sql.parameter_missing": "errors/sql-parameter-missing",
    "sql.parameter_type_mismatch": "errors/sql-parameter-type-mismatch",
    "sql.unsupported_feature": "errors/sql-unsupported-feature",
    "constraint.unique": "errors/constraint-unique",
    "constraint.not_null": "errors/constraint-not-null",
    "constraint.check": "errors/constraint-check",
    "constraint.foreign_key": "errors/constraint-foreign-key",
    "transaction.no_active_transaction": "errors/transaction-no-active-transaction",
    "transaction.invalid_state": "errors/transaction-invalid-state",
    "queue.write_timeout": "errors/queue-write-timeout",
    "queue.canceled": "errors/queue-canceled",
    "queue.full": "errors/queue-full",
    "queue.closed": "errors/queue-closed",
    "busy.writer_lock": "errors/busy-writer-lock",
    "busy.reader_conflict": "errors/busy-reader-conflict",
    "coordination.lock_timeout": "errors/coordination-lock-timeout",
    "coordination.sidecar_unavailable": "errors/coordination-sidecar-unavailable",
    "io.permission_denied": "errors/io-permission-denied",
    "io.disk_full": "errors/io-disk-full",
    "io.not_found": "errors/io-not-found",
    "format.unsupported_version": "errors/format-unsupported-version",
    "corruption.database_header": "errors/corruption-database-header",
    "corruption.page_checksum": "errors/corruption-page-checksum",
    "corruption.wal_frame": "errors/corruption-wal-frame",
    "corruption.wal_replay": "errors/corruption-wal-replay",
    "tde.key_required": "errors/tde-key-required",
    "tde.key_mismatch": "errors/tde-key-mismatch",
    "security.policy_denied": "errors/security-policy-denied",
    "security.mask_expression_invalid": "errors/security-mask-expression-invalid",
    "sync.scope_not_found": "errors/sync-scope-not-found",
    "sync.retention_blocked": "errors/sync-retention-blocked",
    "branch.not_found": "errors/branch-not-found",
    "branch.merge_conflict": "errors/branch-merge-conflict",
    "extension.untrusted_package": "errors/extension-untrusted-package",
    "internal.panic_captured": "errors/internal-panic-captured",
    "internal.invariant": "errors/internal-invariant",
}


MAINTAINED_BINDINGS = {
    "python": {
        "smoke_file": "tests/bindings/python/test_ffi.py",
        "status": "covered",
    },
    "go": {
        "smoke_file": "tests/bindings/go/smoke.go",
        "status": "covered",
    },
    "node": {
        "smoke_file": "tests/bindings/node/smoke.c",
        "status": "covered",
    },
    "dotnet": {
        "smoke_file": "tests/bindings/dotnet/Smoke/Program.cs",
        "status": "covered",
    },
    "java": {
        "smoke_file": "tests/bindings/java/Smoke.java",
        "status": "covered",
    },
    "dart": {
        "smoke_file": "tests/bindings/dart/smoke.dart",
        "status": "covered",
    },
    "wasm": {
        "smoke_file": "tests/bindings/web/smoke.js",
        "status": "covered",
    },
}


def parse_table_subcodes(path: Path) -> set[str]:
    subcodes = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        fields = [field.strip() for field in line.strip("|").split("|")]
        if len(fields) < 2:
            continue
        second = fields[1].strip().strip("`")
        if "." in second and " " not in second:
            subcodes.add(second)
    return subcodes


def has_anchor(path: Path, anchor: str) -> bool:
    return f'id="{anchor}"' in path.read_text(encoding="utf-8")


def validate(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    issues = []
    warnings = []

    error_codes = root / "docs/api/error-codes.md"
    diagnostics_page = root / "docs/user-guide/error-diagnostics.md"
    c_cpp = root / "docs/api/c-cpp.md"
    changelog = root / "docs/about/changelog.md"
    future_wins = root / "design/FUTURE_WINS.md"
    roadmap_phase = "Rich structured errors and developer diagnostics"

    if not error_codes.exists():
        issues.append("docs/api/error-codes.md is missing")
    if not diagnostics_page.exists():
        issues.append("docs/user-guide/error-diagnostics.md is missing")

    if error_codes.exists():
        text = error_codes.read_text(encoding="utf-8")
        if "DDB_ABI_VERSION" not in text:
            issues.append("error-codes doc missing DDB_ABI_VERSION mention")
        if "subcode" not in text:
            issues.append("error-codes doc missing subcode section")
        if "redaction" not in text.lower():
            issues.append("error-codes doc missing redaction guidance")
        if "retryable" not in text or "permanent" not in text:
            issues.append("error-codes doc missing retryability fields")
        if "docs" not in text:
            issues.append("error-codes doc missing docs anchor guidance")

        table_subcodes = parse_table_subcodes(error_codes)
        missing = sorted(set(REQUIRED_SUBCODES) - table_subcodes)
        if missing:
            issues.append(
                "error-codes doc missing first-slice subcodes: " + ", ".join(missing)
            )

        for subcode, anchor in sorted(REQUIRED_SUBCODES.items()):
            if anchor not in text:
                warnings.append(
                    f"error-codes doc missing anchor reference {anchor} for {subcode}"
                )

        if not diagnostics_page.exists():
            for _, anchor in sorted(REQUIRED_SUBCODES.items()):
                warnings.append(f"No troubleshooting anchor target available for {anchor}")

    if diagnostics_page.exists():
        for subcode, anchor in sorted(REQUIRED_SUBCODES.items()):
            if not has_anchor(diagnostics_page, anchor):
                issues.append(f"troubleshooting page missing anchor {anchor}")

    if c_cpp.exists():
        if "ddb_last_error_json" not in c_cpp.read_text(encoding="utf-8"):
            warnings.append("c-cpp docs do not reference ddb_last_error_json yet")
    else:
        issues.append("docs/api/c-cpp.md is missing")

    if future_wins.exists():
        fw_text = future_wins.read_text(encoding="utf-8")
        fw_text_lower = fw_text.lower()
        if roadmap_phase not in fw_text:
            issues.append("future wins roadmap missing structured diagnostics phase")
        status_map_index = fw_text_lower.find("## status map")
        status_scope = fw_text[status_map_index:] if status_map_index != -1 else fw_text
        pre_status_scope = fw_text[:status_map_index] if status_map_index != -1 else fw_text
        if roadmap_phase.lower() in status_scope.lower():
            warnings.append(
                f"FUTURE_WINS still lists {roadmap_phase} after the Status Map; keep completed work only in Delivered Context"
            )
        if roadmap_phase.lower() not in pre_status_scope.lower():
            warnings.append(
                f"FUTURE_WINS missing Delivered Context entry for {roadmap_phase}"
            )
        if "delivered context" not in fw_text_lower:
            warnings.append("FUTURE_WINS missing Delivered Context section")
    else:
        issues.append("design/FUTURE_WINS.md is missing")

    if not changelog.exists():
        warnings.append("docs/about/changelog.md is missing")

    for binding, info in MAINTAINED_BINDINGS.items():
        smoke_path = root / info["smoke_file"]
        if not smoke_path.exists():
            issues.append(f"smoke test file missing for {binding}: {info['smoke_file']}")
            continue

        status = info["status"]
        if status == "covered":
            if not any(
                key in smoke_path.read_text(encoding="utf-8")
                for key in ("subcode", "diagnostic", "retryable")
            ):
                issues.append(
                    f"{binding} marked covered, but smoke fixture has no diagnostic assertions"
                )
        else:
            warnings.append(f"{binding} uses unexpected status {status}")

    if issues:
        print("Validation failed:")
        for issue in issues:
            print(f"- {issue}")
        return 1

    if warnings:
        print("Validation warning:")
        for warning in warnings:
            print(f"- {warning}")
    else:
        print("Validation passed with no warnings.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=".",
        help="repository root (default: current directory)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="show stronger warnings for incomplete binding coverage",
    )
    return validate(parser.parse_args())


if __name__ == "__main__":
    sys.exit(main())
