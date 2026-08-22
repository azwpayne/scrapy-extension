"""Versioned SQS physical queue-name mapping regressions."""

from __future__ import annotations

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from scrapy_extension.backends.sqs import _physical_queue_name, _v2_queue_owner
from scrapy_extension.settings import SqsQueueNameGeneration, SqsSettings

_V2_NAME_PATTERN = re.compile(r"^scrapyext-v2-[0-9a-f]{40}$")
_LOGICAL_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-",
    min_size=1,
    max_size=200,
)
_PREFIXES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-",
    max_size=200,
)


def test_v2_mapping_golden_vectors() -> None:
    """Freeze the complete v2 wire mapping, including tuple boundaries."""
    assert _physical_queue_name("", "q", SqsQueueNameGeneration.V2) == (
        "scrapyext-v2-15c9609ba1ac7d7a1e30bce265238ad71407ff45"
    )
    assert _physical_queue_name("scrapy-", "queue1", SqsQueueNameGeneration.V2) == (
        "scrapyext-v2-84c957fda86e68c72442ea2433d04183a0dd34b0"
    )
    assert (
        _physical_queue_name("scrapy-", "spider:queue", SqsQueueNameGeneration.V2)
        == "scrapyext-v2-a3a4dca453bc8c5527831789110a8ffbdd25c61a"
    )


def test_v2_separates_known_legacy_prefix_boundary_collision() -> None:
    """v1 concatenation aliases (a, bc) with (ab, c); v2 hashes the tuple."""
    assert _physical_queue_name("a", "bc", SqsQueueNameGeneration.LEGACY_V1) == (
        _physical_queue_name("ab", "c", SqsQueueNameGeneration.LEGACY_V1)
    )
    assert _physical_queue_name("a", "bc", SqsQueueNameGeneration.V2) != (
        _physical_queue_name("ab", "c", SqsQueueNameGeneration.V2)
    )


def test_v2_owner_is_stable_and_bound_to_complete_tuple() -> None:
    assert _v2_queue_owner("scrapy-", "queue1") == (
        "scrapy-extension:sqs:v2:84c957fda86e68c72442ea2433d04183a0dd34b0"
    )
    assert _v2_queue_owner("a", "bc") != _v2_queue_owner("ab", "c")


def test_v2_owner_distinguishes_a_legacy_direct_alias() -> None:
    v2_name = _physical_queue_name("a", "bc", SqsQueueNameGeneration.V2)

    assert (
        _physical_queue_name("", v2_name, SqsQueueNameGeneration.LEGACY_V1) == v2_name
    )
    assert _v2_queue_owner("a", "bc") != _v2_queue_owner("", v2_name)


def test_v2_separates_known_legacy_direct_vs_hash_namespace_collision() -> None:
    """Every v2 input uses one hashed namespace instead of v1's split routing."""
    invalid_source = "spider:queue"
    legacy_hash_name = _physical_queue_name(
        "", invalid_source, SqsQueueNameGeneration.LEGACY_V1
    )
    assert legacy_hash_name == "scrapyext-q-b96d441efb6db3b336511a7149d9cb4f"
    assert _physical_queue_name(
        "", legacy_hash_name, SqsQueueNameGeneration.LEGACY_V1
    ) == _physical_queue_name("", invalid_source, SqsQueueNameGeneration.LEGACY_V1)

    assert _physical_queue_name("", legacy_hash_name, SqsQueueNameGeneration.V2) != (
        _physical_queue_name("", invalid_source, SqsQueueNameGeneration.V2)
    )


@given(prefix=_PREFIXES, logical_name=_LOGICAL_NAMES)
def test_v2_outputs_one_valid_bounded_namespace(prefix: str, logical_name: str) -> None:
    physical_name = _physical_queue_name(
        prefix, logical_name, SqsQueueNameGeneration.V2
    )

    assert _V2_NAME_PATTERN.fullmatch(physical_name)
    assert 1 <= len(physical_name) <= 80


@given(prefix=_PREFIXES, logical_name=_LOGICAL_NAMES)
def test_v2_mapping_is_deterministic(prefix: str, logical_name: str) -> None:
    assert _physical_queue_name(prefix, logical_name, SqsQueueNameGeneration.V2) == (
        _physical_queue_name(prefix, logical_name, SqsQueueNameGeneration.V2)
    )


@pytest.mark.parametrize("prefix, logical_name", [(None, "q"), ("p", None), (1, "q")])
def test_physical_name_rejects_non_string_identity_parts(
    prefix: object, logical_name: object
) -> None:
    with pytest.raises(ValueError, match="inputs must be strings"):
        _physical_queue_name(
            prefix,
            logical_name,
            SqsQueueNameGeneration.V2,  # type: ignore[arg-type]
        )


def test_settings_select_v2_by_default_and_explicit_legacy_drain_mode() -> None:
    assert SqsSettings().queue_name_generation is SqsQueueNameGeneration.V2
    assert (
        SqsSettings(queue_name_generation="legacy_v1").queue_name_generation
        is SqsQueueNameGeneration.LEGACY_V1
    )


def test_queue_name_generation_loads_from_sqs_environment(monkeypatch) -> None:
    monkeypatch.setenv("SCRAPY_SQS_QUEUE_NAME_GENERATION", "legacy_v1")
    assert SqsSettings().queue_name_generation is SqsQueueNameGeneration.LEGACY_V1

    monkeypatch.setenv("SCRAPY_SQS_QUEUE_NAME_GENERATION", "v2")
    assert SqsSettings().queue_name_generation is SqsQueueNameGeneration.V2
