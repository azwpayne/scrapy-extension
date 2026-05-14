# Item pipelines
#
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#
# The recommended approach is to use BackendPipeline via settings.py:
#
#     ITEM_PIPELINES = {
#         "scrapy_extension.pipeline.pipeline.BackendPipeline": 300,
#     }
#
# BackendPipeline automatically stores each item in the configured backend
# (Redis, MongoDB, etc.) with unique keys: items:{spider_name}:{timestamp}:{uuid}
#
# Settings:
#   SCRAPY_PIPELINE_KEY_PREFIX = "items"    # Key prefix (default: "items")
#   SCRAPY_PIPELINE_TTL = 3600              # TTL in seconds (default: None = no expiry)
#
# Example: Custom pipeline that enriches items before BackendPipeline stores them
#
# class QuoteEnrichmentPipeline:
#     def process_item(self, item, spider):
#         item["crawled_at"] = datetime.now(timezone.utc).isoformat()
#         item["spider_name"] = spider.name
#         return item
#
# Configure in settings.py:
#     ITEM_PIPELINES = {
#         "examples.pipelines.QuoteEnrichmentPipeline": 200,      # Run first
#         "scrapy_extension.pipeline.pipeline.BackendPipeline": 300,  # Then store
#     }
