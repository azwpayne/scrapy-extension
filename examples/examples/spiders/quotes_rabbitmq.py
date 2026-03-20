import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRabbitMQSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_rabbitmq"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.RABBITMQ
  rabbitmq_url = "amqp://guest:guest@localhost:5672/"

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.setup_backend()
