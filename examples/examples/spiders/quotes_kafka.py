import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesKafkaSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_kafka"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.KAFKA
  kafka_bootstrap_servers = "localhost:9092"

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.setup_backend()
