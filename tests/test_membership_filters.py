"""Tests for MembershipFilter abstraction and SetMembershipFilter (subsystem ①).

TDD red phase: these imports will fail until
``scrapy_extension.dupefilter.filters.base`` and ``.set_filter`` exist.
"""

import pytest

from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter


class TestMembershipFilterContract:
  """The ABC is not instantiable; ``remove`` defaults to NotImplementedError."""

  def test_cannot_instantiate_abc(self) -> None:
    """Abstract methods block direct instantiation."""
    with pytest.raises(TypeError):
      MembershipFilter()  # type: ignore[abstract]

  def test_default_remove_raises_not_implemented(self) -> None:
    """A concrete filter that omits remove() inherits the unsupported-default."""

    class _NoRemove(MembershipFilter):
      def add(self, item: bytes) -> bool:
        return True

      def __contains__(self, item: bytes) -> bool:  # noqa: D401
        return False

      def __len__(self) -> int:
        return 0

      def clear(self) -> None:
        pass

    flt = _NoRemove()
    with pytest.raises(NotImplementedError):
      flt.remove(b"x")


class TestSetMembershipFilter:
  """SetMembershipFilter delegates to SetBackend, preserving dupefilter semantics."""

  def test_add_new_item_returns_true(self, mock_connection_manager) -> None:
    """Newly added → True; SetBackend.add receives (key, item)."""
    mock_set = mock_connection_manager.get_set_backend()
    mock_set.add.return_value = True

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    assert flt.add(b"fingerprint-1") is True
    mock_set.add.assert_called_once_with("dupe:test", b"fingerprint-1")

  def test_add_duplicate_returns_false(self, mock_connection_manager) -> None:
    """Already-present → False (the dupefilter's duplicate signal)."""
    mock_set = mock_connection_manager.get_set_backend()
    mock_set.add.return_value = False

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    assert flt.add(b"fingerprint-1") is False

  def test_contains_delegates(self, mock_connection_manager) -> None:
    """``in`` delegates to SetBackend.contains."""
    mock_set = mock_connection_manager.get_set_backend()
    mock_set.contains.return_value = True

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    assert (b"x" in flt) is True
    mock_set.contains.assert_called_once_with("dupe:test", b"x")

  def test_len_delegates(self, mock_connection_manager) -> None:
    """len() delegates to SetBackend.set_len."""
    mock_set = mock_connection_manager.get_set_backend()
    mock_set.set_len.return_value = 42

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    assert len(flt) == 42

  def test_clear_delegates(self, mock_connection_manager) -> None:
    """clear() delegates to SetBackend.clear_set."""
    mock_set = mock_connection_manager.get_set_backend()

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    flt.clear()
    mock_set.clear_set.assert_called_once_with("dupe:test")

  def test_remove_delegates(self, mock_connection_manager) -> None:
    """remove() delegates to SetBackend.remove and returns its bool."""
    mock_set = mock_connection_manager.get_set_backend()
    mock_set.remove.return_value = True

    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    assert flt.remove(b"x") is True
    mock_set.remove.assert_called_once_with("dupe:test", b"x")

  def test_default_key(self, mock_connection_manager) -> None:
    """Omitted key falls back to 'dupefilter' (matches BackendDupeFilter default)."""
    flt = SetMembershipFilter(mock_connection_manager)
    assert flt.key == "dupefilter"

  def test_open_close_are_noops(self, mock_connection_manager) -> None:
    """Lifecycle hooks are safe no-ops for the set strategy."""
    flt = SetMembershipFilter(mock_connection_manager, key="dupe:test")
    flt.open()
    flt.close()
