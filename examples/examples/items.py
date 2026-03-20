# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import logging

import scrapy

logger = logging.getLogger(__name__)


class QuoteItem(scrapy.Item):
  """Item representing a quote from quotes.toscrape.com.

  Fields:
      text: The full text of the quote.
      author: The name of the quote's author.
      tags: A list of tags associated with the quote.
  """

  text = scrapy.Field()
  author = scrapy.Field()
  tags = scrapy.Field()


class QuotesParsingMixin:
  """Shared parse() for all quotes.toscrape.com spiders.

  Mixin order: QuotesParsingMixin BEFORE BackendSpiderMixin, scrapy.Spider.
  """

  def parse(self, response):
    logger.info("Parsing %s", response.url)

    for quote in response.css("div.quote"):
      item = QuoteItem()
      item["text"] = quote.css("span.text::text").get()
      item["author"] = quote.css("small.author::text").get()
      item["tags"] = quote.css("div.tags a.tag::text").getall()
      yield item

    next_page = response.css("li.next a::attr(href)").get()
    if next_page:
      yield response.follow(next_page, self.parse)
