from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class QuotesCrawlSpider(CrawlSpider):
  name = "quotes_crawl"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]
  custom_settings = {
    "SCHEDULER": "scrapy.core.scheduler.Scheduler",
    "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",
    "ITEM_PIPELINES": {},
  }

  rules = (Rule(LinkExtractor(allow=r"/page/\d+"), callback="parse_item", follow=True),)

  def parse_item(self, response):
    self.logger.info("Parsing %s", response.url)

    for quote in response.css("div.quote"):
      yield {
        "text": quote.css("span.text::text").get(),
        "author": quote.css("small.author::text").get(),
        "tags": quote.css("div.tags a.tag::text").getall(),
      }
