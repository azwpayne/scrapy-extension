import scrapy

from examples.items import QuoteItem


class QuotesSpider(scrapy.Spider):
  name = "quotes"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  def parse(self, response):
    self.logger.info("Parsing %s", response.url)

    for quote in response.css("div.quote"):
      item = QuoteItem()
      item["text"] = quote.css("span.text::text").get()
      item["author"] = quote.css("small.author::text").get()
      item["tags"] = quote.css("div.tags a.tag::text").getall()
      yield item

    next_page = response.css("li.next a::attr(href)").get()
    if next_page:
      yield response.follow(next_page, self.parse)
