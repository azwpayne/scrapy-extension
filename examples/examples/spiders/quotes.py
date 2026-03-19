import scrapy


class QuotesSpider(scrapy.Spider):
  name = "quotes"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  def start_requests(self):
    for url in self.start_urls:
      yield scrapy.Request(url=url, callback=self.parse)

  def parse(self, response):
    pass
