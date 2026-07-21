import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRabbitMQSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_rabbitmq"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.RABBITMQ
  rabbitmq_url = "amqp://localhost:5672/"
  backend_settings = {"username": "guest", "password": "guest"}
  custom_settings = {
    "SCRAPY_QUEUE_BACKEND_TYPE": "rabbitmq",
    "SCRAPY_RABBITMQ_URL": rabbitmq_url,
    "SCRAPY_RABBITMQ_USERNAME": "guest",
    "SCRAPY_RABBITMQ_PASSWORD": "guest",
    "SCRAPY_SET_BACKEND_TYPE": "redis",
    "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
  }
