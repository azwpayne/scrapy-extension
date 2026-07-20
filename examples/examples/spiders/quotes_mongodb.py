import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesMongoDBSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
  name = "quotes_mongodb"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  backend_type = BackendType.MONGODB
  mongodb_uri = "mongodb://localhost:27017"
  mongodb_db = "scrapy_quotes"
  custom_settings = {
    "SCRAPY_BACKEND_TYPE": "mongodb",
    "SCRAPY_MONGO_URI": mongodb_uri,
    "SCRAPY_MONGO_DATABASE": mongodb_db,
  }
