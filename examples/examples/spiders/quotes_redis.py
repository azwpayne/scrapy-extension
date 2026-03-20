import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRedisSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_redis"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.REDIS

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.setup_backend()
