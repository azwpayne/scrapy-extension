import os

import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType

SENTINEL_CONFIG = {
  "mode": "sentinel",
  "sentinels": ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"],
  "sentinel_master_name": "mymaster",
  "sentinel_password": os.environ.get("REDIS_SENTINEL_PASSWORD", "changeme"),
  "password": os.environ.get("REDIS_PASSWORD", "changeme"),
  "db": 0,
}

CLUSTER_CONFIG = {
  "mode": "cluster",
  "cluster_startup_nodes": ["node1:7000", "node2:7000", "node3:7000"],
  "password": os.environ.get("REDIS_PASSWORD", None),
  "db": 0,
  "cluster_max_redirects": 5,
}

SELECTED_CONFIG = (
  CLUSTER_CONFIG
  if os.environ.get("SCRAPY_EXAMPLE_REDIS_MODE", "sentinel").lower() == "cluster"
  else SENTINEL_CONFIG
)


class QuotesMultiModeSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_multi_mode"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.REDIS
  backend_settings = SELECTED_CONFIG
  custom_settings = {
    "SCRAPY_BACKEND_TYPE": "redis",
    "SCRAPY_BACKEND_SETTINGS": SELECTED_CONFIG,
  }
