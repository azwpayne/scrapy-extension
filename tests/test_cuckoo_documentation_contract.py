"""Contract checks for Cuckoo item-removal guidance."""

from __future__ import annotations

import re
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

_REQUIRED_GUIDANCE = {
    "README.md": (
        "| `cuckoo` | no (FP) | no | no (clear only) |",
        "If item-level removal is required, choose the exact `memory` or `set` "
        "strategy; Cuckoo supports only `clear()` for a whole-filter reset.",
    ),
    "docs/runbook.md": (
        "tolerates false positives; whole-filter clear only",
        "If item-level removal is required, choose the exact `memory` or `set` "
        "strategy.",
        "Cuckoo supports only `clear()` for a whole-filter reset.",
    ),
    "docs/migration-guide.md": (
        "Callers that require exact per-item removal should select the\n"
        "`memory` or `set` strategy instead.",
    ),
    "src/scrapy_extension/dupefilter/filters/factory.py": (
        "CUCKOO: Probabilistic, in-process, no item removal; clear only.",
    ),
    "src/scrapy_extension/dupefilter/filters/bloom_filter.py": (
        "Neither\nBloom nor Cuckoo supports item-level removal; use the exact memory "
        "or set\nstrategy when that is required.",
        "Cuckoo supports only a whole-filter ``clear()``.",
    ),
}

_STALE_DELETION_CLAIMS = (
    re.compile(r"supports\s+d[e]letion"),
    re.compile(r"needs\s+d[e]letion"),
    re.compile(r"use\s+(?:the\s+)?cuckoo\s+strategy\s+for\s+that"),
    re.compile(r"\|\s*`cuckoo`\s*\|\s*no\s*\(fp\)\s*\|\s*no\s*\|\s*y[e]s\s*\|"),
)


def test_active_cuckoo_guidance_requires_exact_filter_for_item_removal() -> None:
    """All active guidance must direct item removal to memory/set, not Cuckoo."""
    contents: dict[str, str] = {}

    for relative_path, required_snippets in _REQUIRED_GUIDANCE.items():
        contents[relative_path] = (_REPOSITORY_ROOT / relative_path).read_text(
            encoding="utf-8"
        )
        for snippet in required_snippets:
            assert snippet in contents[relative_path], (
                f"{relative_path} is missing Cuckoo removal guidance: {snippet!r}"
            )

    combined = "\n".join(contents.values()).lower()
    for stale_claim in _STALE_DELETION_CLAIMS:
        assert stale_claim.search(combined) is None
