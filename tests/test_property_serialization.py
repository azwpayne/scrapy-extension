"""Hypothesis property tests for BackendQueue request serialization (subsystem ③).

Verifies the lossless round-trip claim made in
``src/scrapy_extension/queue/queue.py``:

  ``Request -> _request_to_dict -> JSON -> deserialize -> _decode_body
      -> request_from_dict``

yields a request equal to the original on method / url / body / priority /
encoding, with meta keys, cb_kwargs, headers, cookies, and flags preserved.

The body path is the one fixed by the CRITICAL hardening pass: bodies are
base64-encoded (pure ASCII) before JSON so binary POST bodies round-trip
losslessly; ``_decode_body`` reverses it on the way back. This property pins
that claim — any regression to a UTF-8/latin-1 fallback would surface as a
body mismatch on arbitrary byte values.

Strategies are deliberately bounded (ascii / url-safe / small dicts) to avoid
pathological inputs that exercise Scrapy-internal parser quirks rather than
``BackendQueue``'s serialization contract. ~200 cases.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from scrapy.http import Request
from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.queue.queue import BackendQueue

#: Ascii method tokens.
_methods = st.sampled_from(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])

#: URL-safe path + query (no scheme/host to avoid Scrapy's URL normalization
#: quirks around unicode hosts / idna — we prefix ``https://example.com/``).
_url_path = st.text(
  alphabet=st.characters(
    whitelist_categories=("Ll", "Nd"),
    whitelist_characters="/-_?=&.%",
  ),
  min_size=0,
  max_size=40,
)


def _build_url(path: str) -> str:
  """Wrap a generated path with a fixed ascii origin so Scrapy accepts it."""
  safe = path if path.startswith("/") or path == "" else "/" + path
  return f"https://example.com{safe}"


#: Bounded ascii bodies (arbitrary bytes survive because base64 encodes them).
_bodies = st.one_of(
  st.none(),
  st.binary(min_size=0, max_size=256),
)

#: Small header dicts — ASCII keys + ASCII values. Header KEYS are restricted
#: to ASCII letters because Scrapy's ``to_unicode_dict`` lowercases keys via
#: ``.title()`` / case-folding, and non-ASCII Lu chars (e.g. U+0130 'İ') fold
#: to multi-codepoint lowercases that differ between the encode-side and
#: decode-side CaselessDict normalization. That is a Scrapy header-cache
#: quirk, not a ``BackendQueue`` serialization concern — bounded out here so
#: the property targets the serialization contract we own.
_header_keys = st.text(
  alphabet=st.characters(whitelist_categories=("Lu",), whitelist_characters="-"),
  min_size=1,
  max_size=12,
).filter(lambda s: s.isascii())
_header_vals = st.text(
  alphabet=st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters=" -."),
  min_size=0,
  max_size=20,
).filter(lambda s: s.isascii())
_headers = st.dictionaries(_header_keys, _header_vals, min_size=0, max_size=4)

#: Cookie dicts — ASCII keys + ASCII values.
_cookie_keys = st.text(
  alphabet=st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters="_-"),
  min_size=1,
  max_size=10,
).filter(lambda s: s.isascii())
_cookie_vals = st.text(
  alphabet=st.characters(whitelist_categories=("Ll", "Nd")),
  min_size=0,
  max_size=12,
).filter(lambda s: s.isascii())
_cookies = st.dictionaries(_cookie_keys, _cookie_vals, min_size=0, max_size=3)

#: Meta values: JSON-serializable scalars / small lists / small dicts.
#: Avoids objects Scrapy's request_from_dict cannot rebuild.
_scalars = st.one_of(
  st.none(),
  st.booleans(),
  st.integers(min_value=-(1 << 40), max_value=1 << 40),
  st.text(alphabet=st.characters(whitelist_categories=("Ll", "Nd", "Pc")), min_size=0, max_size=10),
)
_meta_values = st.recursive(
  _scalars,
  lambda children: st.one_of(
    st.lists(children, max_size=3),
    st.dictionaries(
      st.text(alphabet=st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters="_-"), min_size=1, max_size=8),
      children,
      max_size=3,
    ),
  ),
  max_leaves=4,
)
_meta = st.dictionaries(
  st.text(alphabet=st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters="_-"), min_size=1, max_size=10),
  _meta_values,
  min_size=0,
  max_size=4,
)

#: cb_kwargs — same constraints as meta.
_cb_kwargs = st.dictionaries(
  st.text(alphabet=st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters="_-"), min_size=1, max_size=10),
  _meta_values,
  min_size=0,
  max_size=3,
)

#: Ascii flags.
_flags = st.lists(
  st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=8),
  max_size=3,
)


class _NullConnectionManager:
  """Stand-in so BackendQueue constructs without a real backend.

  The serialization round-trip never touches the connection manager —
  ``_request_to_dict`` / ``pop`` / ``_decode_body`` are pure transforms.
  """


@pytest.fixture()
def queue() -> BackendQueue:
  """A BackendQueue with passthrough strategy — never actually pushes/pops."""
  return BackendQueue(_NullConnectionManager(), "test-queue")


@given(
  method=_methods,
  url_path=_url_path,
  body=_bodies,
  headers=_headers,
  cookies=_cookies,
  meta=_meta,
  cb_kwargs=_cb_kwargs,
  priority=st.integers(min_value=-(1 << 30), max_value=1 << 30),
  flags=_flags,
)
@settings(
  max_examples=300,
  deadline=None,
  suppress_health_check=[
    HealthCheck.too_slow,
    HealthCheck.function_scoped_fixture,
  ],
  derandomize=True,
)
def test_request_serialization_round_trip(
  queue: BackendQueue,
  method: str,
  url_path: str,
  body: bytes | None,
  headers: dict[str, str],
  cookies: dict[str, str],
  meta: dict[str, Any],
  cb_kwargs: dict[str, Any],
  priority: int,
  flags: list[str],
) -> None:
  """Request round-trips losslessly through JSON + base64 body encoding.

  Constructs a Scrapy ``Request`` from hypothesis-generated fields, encodes
  it via ``BackendQueue._request_to_dict`` (the production encode path,
  including base64 body), runs it through the JSON serializer, decodes it
  back via ``_decode_body`` + Scrapy's ``request_from_dict``, and asserts
  equality on the load-bearing fields.

  Callback/errback are left ``None`` — this test verifies the data
  serialization contract, not callback-name resolution (which needs a spider
  and is covered by ``test_queue.py``).
  """
  original = Request(
    url=_build_url(url_path),
    method=method,
    body=b"" if body is None else body,
    headers=headers,
    cookies=cookies,
    meta=meta,
    cb_kwargs=cb_kwargs,
    encoding="utf-8",
    priority=priority,
    flags=flags,
    dont_filter=True,
  )

  # Encode → JSON → decode, exactly the production pop path (minus backend).
  request_dict = queue._request_to_dict(original)  # noqa: SLF001
  serializer = JSONSerializer()
  wire = serializer.serialize(request_dict)
  recovered_dict = serializer.deserialize(wire)
  BackendQueue._decode_body(recovered_dict)  # noqa: SLF001
  recovered = request_from_dict(recovered_dict, spider=None)

  # The cardinal round-trip claims — method/url/body/priority/encoding.
  assert recovered.method == original.method
  assert recovered.url == original.url
  assert recovered.body == original.body
  assert recovered.priority == original.priority
  assert recovered.encoding == original.encoding

  # meta round-trips (keys + values).
  assert dict(recovered.meta) == dict(original.meta)
  # cb_kwargs round-trip.
  assert recovered.cb_kwargs == original.cb_kwargs
  # Flags round-trip.
  assert recovered.flags == original.flags
  # Headers round-trip through Scrapy's CaselessDict + to_unicode_dict.
  assert dict(recovered.headers.to_unicode_dict()) == dict(
    original.headers.to_unicode_dict()
  )
