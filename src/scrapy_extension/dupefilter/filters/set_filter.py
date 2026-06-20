"""Set-backend membership filter — exact, cross-worker dedup (subsystem ①).

The default dedup strategy. Byte-identical to the previous hardcoded
``BackendDupeFilter`` behavior: it delegates to a ``SetBackend``'s atomic
``add``.
"""

from __future__ import annotations

__all__ = ["SetMembershipFilter"]

from typing import TYPE_CHECKING

from scrapy_extension.dupefilter.filters.base import MembershipFilter

if TYPE_CHECKING:
  from scrapy_extension.backends.base import SetBackend
  from scrapy_extension.backends.connectors import ConnectionManager


class SetMembershipFilter(MembershipFilter):
  """Exact membership filter backed by a distributed ``SetBackend``.

  Each operation delegates to the connection manager's set backend under
  a shared key, so state is visible to every worker using the same
  backend and key. This is the default dedup strategy and preserves the
  pre-strategy ``BackendDupeFilter`` semantics exactly.

  Attributes:
      _connection_manager: Source of the SetBackend.
      key: The backend set name all fingerprints are stored under.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key: str = "dupefilter",
  ) -> None:
    """Initialize the set filter.

    Args:
        connection_manager: Connection manager providing the SetBackend.
        key: Name of the backend set holding fingerprints.
    """
    self._connection_manager = connection_manager
    self.key = key

  def _set_backend(self) -> SetBackend:
    """Resolve the SetBackend lazily on each call.

    Lazy resolution mirrors the previous ``BackendDupeFilter`` behavior
    (which called ``get_set_backend()`` per request) and stays correct
    if the manager reconnects and replaces the backend instance.

    Returns:
        The current SetBackend from the connection manager.
    """
    return self._connection_manager.get_set_backend()

  def add(self, item: bytes) -> bool:
    """Atomically add and return whether the item was new.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if newly added, False if already present.
    """
    return self._set_backend().add(self.key, item)

  def __contains__(self, item: bytes) -> bool:
    """Check membership via ``SetBackend.contains``.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the item is in the set.
    """
    return self._set_backend().contains(self.key, item)

  def __len__(self) -> int:
    """Return the set size via ``SetBackend.set_len``.

    Returns:
        Number of fingerprints currently stored.
    """
    return self._set_backend().set_len(self.key)

  def clear(self) -> None:
    """Clear all fingerprints via ``SetBackend.clear_set``."""
    self._set_backend().clear_set(self.key)

  def remove(self, item: bytes) -> bool:
    """Remove a fingerprint via ``SetBackend.remove``.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the item was present and removed, False otherwise.
    """
    return self._set_backend().remove(self.key, item)
