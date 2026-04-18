"""ElasticSearch backend spider example."""

import scrapy

from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesElasticsearchSpider(BackendSpiderMixin, scrapy.Spider):
  """Distributed spider using ElasticSearch as the backend.

  Demonstrates BackendSpiderMixin with ElasticSearch for queue,
  duplicate filtering, and item storage.
  """

  backend_type = BackendType.ELASTICSEARCH
  name = "quotes_elasticsearch"
  start_urls = ["https://quotes.toscrape.com/"]

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.setup_backend()

  def parse(self, response):
    for quote in response.css("div.quote"):
      yield {
        "text": quote.css("span.text::text").get(),
        "author": quote.css("small.author::text").get(),
        "tags": quote.css("div.tags a.tag::text").getall(),
      }

    next_page = response.css("li.next a::attr(href)").get()
    if next_page:
      yield response.follow(next_page, self.parse)
