import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesKafkaSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_kafka"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.KAFKA
  kafka_bootstrap_servers = "localhost:9092"
  custom_settings = {
    "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
    "SCRAPY_KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    "SCRAPY_SET_BACKEND_TYPE": "redis",
    "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
  }
