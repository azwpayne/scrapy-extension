"""Scrapy components for scrapy-extension.

This module provides Scrapy components (queue, scheduler, dupefilter, pipeline)
that work with backend implementations.
"""

from scrapy_extension.components.dupefilter import BackendDupeFilter
from scrapy_extension.components.pipeline import BackendPipeline
from scrapy_extension.components.queue import BackendQueue
from scrapy_extension.components.scheduler import BackendScheduler

__all__ = [
    "BackendQueue",
    "BackendScheduler",
    "BackendDupeFilter",
    "BackendPipeline",
]
