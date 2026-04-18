import hashlib

import scrapy
from scrapy import signals
from scrapy_extension import BackendType, ConnectionManager

from examples.items import QuoteItem


class QuotesConnectionManagerSpider(scrapy.Spider):
  name = "quotes_connection_manager"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self._manager = ConnectionManager.get_manager(
      backend_type=BackendType.REDIS,
      settings={"host": "localhost", "port": 6379, "db": 0},
    )
    self._set_backend = None
    self._storage_backend = None

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider._on_spider_closed, signals.spider_closed)  # noqa: SLF001
    return spider

  def _on_spider_closed(self, spider, reason=""):  # noqa: ARG002
    if spider is self:
      self._manager.close()

  def _get_set_backend(self):
    if self._set_backend is None:
      try:
        self._set_backend = self._manager.get_set_backend()
      except NotImplementedError:
        self.logger.warning("Backend does not support set operations; dedup disabled")
    return self._set_backend

  def _get_storage_backend(self):
    if self._storage_backend is None:
      try:
        self._storage_backend = self._manager.get_storage_backend()
      except NotImplementedError:
        self.logger.warning("Backend does not support storage operations")
    return self._storage_backend

  def parse(self, response):
    self.logger.info("Parsing %s", response.url)

    queue_backend = self._manager.get_queue_backend()
    set_backend = self._get_set_backend()
    storage_backend = self._get_storage_backend()

    for quote in response.css("div.quote"):
      item = QuoteItem()
      item["text"] = quote.css("span.text::text").get()
      item["author"] = quote.css("small.author::text").get()
      item["tags"] = quote.css("div.tags a.tag::text").getall()

      text_hash = hashlib.md5((item["text"] or "").encode()).hexdigest()[:8]
      item_key = f"quote:{item['author']}:{text_hash}"

      if set_backend is not None and not set_backend.add(
        "seen_quotes", item_key.encode()
      ):
        continue

      if storage_backend is not None:
        storage_backend.store(item_key, str(dict(item)).encode())

      queue_backend.push("quote_queue", item_key.encode(), priority=0.0)
      yield item

    next_page = response.css("li.next a::attr(href)").get()
    if next_page:
      yield response.follow(next_page, self.parse)
