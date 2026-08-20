"""Stable project/spider identities used by durable component keys."""

from __future__ import annotations

from typing import Any

DEFAULT_PROJECT_NAME = "default"
DEFAULT_QUEUE_KEY_TEMPLATE = "scheduler-queue:{project}:{spider}"
DEFAULT_DUPEFILTER_KEY_TEMPLATE = "dupefilter:{project}:{spider}"


def project_name_from_settings(settings: Any) -> str:
    """Return the configured Scrapy project identity.

    ``BOT_NAME`` is Scrapy's canonical project identifier.  A small stable
    fallback keeps programmatic component construction deterministic when no
    crawler settings are attached.
    """
    try:
        value = settings.get("BOT_NAME")
    except (AttributeError, TypeError):
        value = None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return DEFAULT_PROJECT_NAME


def project_name_from_spider(spider: Any) -> str:
    """Resolve a spider's project identity without requiring a crawler."""
    crawler = getattr(spider, "crawler", None)
    settings = getattr(crawler, "settings", None) if crawler is not None else None
    return project_name_from_settings(settings)


def resolve_identity_template(
    template: str,
    *,
    spider_name: str | None = None,
    project_name: str | None = None,
) -> str:
    """Substitute known identity placeholders in a backend key template."""
    resolved = template
    if project_name is not None:
        resolved = resolved.replace("{project}", project_name)
    if spider_name is not None:
        resolved = resolved.replace("{spider}", spider_name)
    return resolved


__all__ = [
    "DEFAULT_DUPEFILTER_KEY_TEMPLATE",
    "DEFAULT_PROJECT_NAME",
    "DEFAULT_QUEUE_KEY_TEMPLATE",
    "project_name_from_settings",
    "project_name_from_spider",
    "resolve_identity_template",
]
