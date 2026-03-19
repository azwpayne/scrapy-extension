"""Example spider using Redis backend with BackendSpiderMixin.

This example demonstrates how to use the BackendSpiderMixin to add
distributed crawling capabilities to a Scrapy spider.

To run this example:
    1. Start Redis: docker run -d -p 6379:6379 redis:7-alpine
    2. Run the spider: python -m examples.redis_spider
"""

from scrapy import Field, Item, Request, Spider
from scrapy.crawler import CrawlerProcess
from scrapy_extension import BackendSpiderMixin, BackendType


class ExampleItem(Item):
  """Example item for demonstration."""

  url = Field()
  title = Field()


class RedisSpider(BackendSpiderMixin, Spider):
  """Example spider using Redis backend.

  This spider demonstrates:
  - Using BackendSpiderMixin for backend integration
  - Configuring Redis connection via class attributes
  - Using the backend queue for start URLs
  - Processing items through the pipeline

  Attributes:
      name: Spider name.
      backend_type: Type of backend to use.
      redis_host: Redis server hostname.
      redis_port: Redis server port.
  """

  name = "redis_spider"
  backend_type = BackendType.REDIS

  # Redis connection settings
  redis_host = "localhost"
  redis_port = 6379

  def __init__(self, **kwargs):
    """Initialize the spider.

    Args:
        **kwargs: Keyword arguments passed to the spider.
    """
    super().__init__(**kwargs)
    self.setup_backend()

  def start_requests(self):
    """Generate start requests.

    Yields:
        Scrapy Request objects.
    """
    # Example: Push URLs to queue and then process them
    urls = [
      "https://example.com/page1",
      "https://example.com/page2",
    ]

    queue = self.get_queue("start_urls")
    for url in urls:
      request = Request(url=url, callback=self.parse)
      queue.push(request, priority=1.0)

    # Now pop and yield requests from queue
    while True:
      request = queue.pop(timeout=0)
      if request is None:
        break
      yield request

  def parse(self, response):
    """Parse the response.

    Args:
        response: Scrapy response object.

    Yields:
        ExampleItem instances.
    """
    yield ExampleItem(
      url=response.url,
      title=response.css("h1::text").get(),
    )


class SimpleRedisSpider(BackendSpiderMixin, Spider):
  """Simpler example spider using BackendSpiderMixin.

  This spider shows the minimum required configuration.
  """

  name = "simple_redis_spider"
  backend_type = BackendType.REDIS

  start_urls = ["https://example.com"]

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.setup_backend()

  def parse(self, response):
    """Parse the response."""
    yield {
      "url": response.url,
      "status": response.status,
    }


def main():
  """Run the example spider."""
  process = CrawlerProcess(
    settings={
      "ITEM_PIPELINES": {
        "scrapy_extension.components.pipeline.BackendPipeline": 300,
      },
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_BACKEND_SETTINGS": {
        "host": "localhost",
        "port": 6379,
      },
    }
  )

  process.crawl(SimpleRedisSpider)
  process.start()


if __name__ == "__main__":
  main()
