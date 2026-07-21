import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesProgrammaticSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_programmatic"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.REDIS
  backend_settings = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": None,
    "socket_timeout": 30.0,
    "socket_connect_timeout": 5.0,
  }
