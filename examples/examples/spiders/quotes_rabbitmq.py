import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRabbitMQSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_rabbitmq"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.RABBITMQ
  rabbitmq_url = "amqp://guest:guest@localhost:5672/"
  custom_settings = {
    "SCRAPY_QUEUE_BACKEND_TYPE": "rabbitmq",
    "SCRAPY_RABBITMQ_URL": rabbitmq_url,
    "SCRAPY_SET_BACKEND_TYPE": "redis",
    "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
  }
