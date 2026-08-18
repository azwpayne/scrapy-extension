"""Contract checks for the DynamoDB revision-token guidance."""

from __future__ import annotations

from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

_REQUIRED_GUIDANCE = {
    "README.md": (
        "Every `store()` generates `_scrapy_revision` with `uuid.uuid4().hex`; "
        "its required stored grammar is exactly 32 lowercase hexadecimal characters",
        "Direct table writers that replace package rows must set "
        "`_scrapy_revision` to a freshly generated `uuid.uuid4().hex` on every "
        "replacement",
    ),
    "docs/runbook.md": (
        "Every package `store()` generates `_scrapy_revision` with "
        "`uuid.uuid4().hex`; the required stored grammar is exactly 32 lowercase "
        "hexadecimal characters",
        "Direct writers must set `_scrapy_revision` to a freshly generated "
        "`uuid.uuid4().hex` on every replacement: exactly 32 lowercase hexadecimal "
        "characters",
    ),
    "docs/migration-guide.md": (
        "Package stores generate `_scrapy_revision` with `uuid.uuid4().hex`; the "
        "required stored grammar is exactly 32 lowercase hexadecimal characters",
        "DynamoDB package writes now include the reserved `_scrapy_revision` string "
        "attribute generated with `uuid.uuid4().hex`. Its value is exactly 32 "
        "lowercase hexadecimal characters",
        "Applications writing directly to the table must set `_scrapy_revision` "
        "to a freshly generated `uuid.uuid4().hex` on every replacement—exactly "
        "32 lowercase hexadecimal characters",
    ),
}

_STALE_WEAK_GUIDANCE = (
    "fresh opaque 32-character revision",
    "fresh opaque `_scrapy_revision`",
    "32-byte opaque value",
    "fresh opaque revision",
    "fresh opaque value",
)


def test_active_dynamodb_guidance_specifies_exact_revision_token_grammar() -> None:
    """Package and direct-writer guidance must match the runtime validator."""
    contents: dict[str, str] = {}

    for relative_path, required_snippets in _REQUIRED_GUIDANCE.items():
        content = (_REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
        normalized_content = " ".join(content.split())
        contents[relative_path] = normalized_content
        for snippet in required_snippets:
            assert snippet in normalized_content, (
                f"{relative_path} is missing revision-token guidance: {snippet!r}"
            )

    combined = "\n".join(contents.values()).lower()
    for stale_guidance in _STALE_WEAK_GUIDANCE:
        assert stale_guidance not in combined
