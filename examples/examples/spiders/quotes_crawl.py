from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class QuotesCrawlSpider(CrawlSpider):
  name = "quotes_crawl"
  allowed_domains = ["quotes.toscrape.com"]
  start_urls = ["https://quotes.toscrape.com"]

  rules = (Rule(LinkExtractor(allow=r"Items/"), callback="parse_item", follow=True),)

  @staticmethod
  def parse_item(response):
    return {
      "domain_id": response.xpath('//input[@id="sid"]/@value').get(),
      "name": response.xpath('//div[@id="name"]').get(),
      "description": response.xpath('//div[@id="description"]').get(),
    }
