"""Request utility functions for scrapy-extension.

This module provides utility functions for working with Scrapy requests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from scrapy.http import Request


def request_fingerprint(request: Request) -> str:
  """Generate a unique fingerprint for a request.

  This is a wrapper around Scrapy's fingerprint function that returns
  a hex string representation of the fingerprint.

  Args:
      request: The Scrapy request to fingerprint.

  Returns:
      A unique fingerprint string in hexadecimal format.
  """
  from scrapy.utils.request import fingerprint as scrapy_fingerprint

  return scrapy_fingerprint(request).hex()
