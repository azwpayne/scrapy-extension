from __future__ import annotations


def test_storage_error_is_part_of_the_stable_top_level_api() -> None:
    import scrapy_extension
    from scrapy_extension.exceptions import StorageError

    assert "StorageError" in scrapy_extension.__all__
    assert scrapy_extension.StorageError is StorageError


def test_documented_core_surfaces_are_top_level_exports() -> None:
    import scrapy_extension
    from scrapy_extension.backends.connectors import resolve_backend_config
    from scrapy_extension.dupefilter.filters.base import FilterFull
    from scrapy_extension.monitor import Monitor, NullMonitor, ScrapyStatsMonitor

    expected = {
        "FilterFull": FilterFull,
        "Monitor": Monitor,
        "NullMonitor": NullMonitor,
        "ScrapyStatsMonitor": ScrapyStatsMonitor,
        "resolve_backend_config": resolve_backend_config,
    }
    for name, value in expected.items():
        assert name in scrapy_extension.__all__
        assert getattr(scrapy_extension, name) is value
