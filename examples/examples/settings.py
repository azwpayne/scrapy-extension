# Scrapy settings for examples project
#
# This configuration demonstrates scrapy-extension with Redis as the default
# backend. Uncomment alternative backend blocks to switch to MongoDB, Kafka,
# or RabbitMQ.
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "examples"

SPIDER_MODULES = ["examples.spiders"]
NEWSPIDER_MODULE = "examples.spiders"

ADDONS = {}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "examples (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# =============================================================================
# scrapy-extension: Backend Configuration
# =============================================================================

# --- Scheduler & DupeFilter (required for distributed crawling) ---
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"

# --- Item Pipeline (stores items in backend storage) ---
ITEM_PIPELINES = {
  "scrapy_extension.pipeline.pipeline.BackendPipeline": 300,
}

# --- Pipeline options ---
SCRAPY_PIPELINE_KEY_PREFIX = "items"
# SCRAPY_PIPELINE_TTL = 3600  # Optional: items expire after 1 hour (seconds)

# =============================================================================
# Backend Selection (redis | mongodb | kafka | rabbitmq | elasticsearch | rocketmq)
# =============================================================================

SCRAPY_BACKEND_TYPE = "redis"

# =============================================================================
# Redis Configuration (Default)
# =============================================================================
# Effective modes: standalone (default), sentinel, cluster.
# master_slave is a deprecated primary-only alias with no replica routing.

SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
SCRAPY_REDIS_DB = 0
# SCRAPY_REDIS_PASSWORD = "secret"  # Optional

# For one static primary, keep standalone and set HOST/PORT. Use Sentinel for
# discovery/failover; replica-read configuration is intentionally unsupported.

# --- Redis Sentinel Mode (High Availability) ---
# SCRAPY_REDIS_MODE = "sentinel"
# SCRAPY_REDIS_SENTINELS = ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"]
# SCRAPY_REDIS_SENTINEL_MASTER_NAME = "mymaster"
# SCRAPY_REDIS_SENTINEL_PASSWORD = "sentinel_secret"
# SCRAPY_REDIS_PASSWORD = "redis_secret"

# --- Redis Cluster Mode ---
# SCRAPY_REDIS_MODE = "cluster"
# SCRAPY_REDIS_CLUSTER_STARTUP_NODES = ["node1:7000", "node2:7000", "node3:7000"]
# SCRAPY_REDIS_DB = 0  # Cluster supports DB0 only; use namespace for isolation
# SCRAPY_REDIS_CLUSTER_MAX_REDIRECTS = 5

# =============================================================================
# MongoDB Configuration (Uncomment to use)
# =============================================================================
# Supports modes: standalone (default), replica_set, sharded_cluster, atlas

# SCRAPY_BACKEND_TYPE = "mongodb"
# SCRAPY_MONGO_URI = "mongodb://localhost:27017"
# SCRAPY_MONGO_DATABASE = "scrapy"

# --- MongoDB Replica Set ---
# SCRAPY_MONGO_MODE = "replica_set"
# SCRAPY_MONGO_REPLICA_SET_NAME = "myReplicaSet"
# SCRAPY_MONGO_REPLICA_SET_MEMBERS = ["host1:27017", "host2:27017", "host3:27017"]
# SCRAPY_MONGO_TLS_ENABLED = True

# --- MongoDB Atlas (Cloud) ---
# SCRAPY_MONGO_MODE = "atlas"
# SCRAPY_MONGO_URI = "mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/scrapy?retryWrites=true&w=majority"

# =============================================================================
# Kafka Configuration (Uncomment to use)
# =============================================================================
# Note: Kafka only supports Queue operations (no Set/Storage for dedup/storage)

# SCRAPY_BACKEND_TYPE = "kafka"
# SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# SCRAPY_KAFKA_GROUP_ID = "scrapy-spiders"

# --- Kafka Cluster ---
# SCRAPY_KAFKA_MODE = "cluster"
# SCRAPY_KAFKA_CLUSTER_BROKERS = ["broker1:9092", "broker2:9092", "broker3:9092"]
# SCRAPY_KAFKA_REPLICATION_FACTOR = 3

# --- Confluent Cloud ---
# SCRAPY_KAFKA_MODE = "confluent"
# SCRAPY_KAFKA_CONFLUENT_BOOTSTRAP_SERVERS = "pkc-xxx.us-east-1.aws.confluent.cloud:9092"
# SCRAPY_KAFKA_CONFLUENT_API_KEY = "API_KEY"
# SCRAPY_KAFKA_CONFLUENT_API_SECRET = "API_SECRET"

# =============================================================================
# RabbitMQ Configuration (Uncomment to use)
# =============================================================================
# Note: RabbitMQ only supports Queue operations (no Set/Storage for dedup/storage)

# SCRAPY_BACKEND_TYPE = "rabbitmq"
# SCRAPY_RABBITMQ_HOST = "localhost"
# SCRAPY_RABBITMQ_PORT = 5672
# SCRAPY_RABBITMQ_USERNAME = "guest"
# SCRAPY_RABBITMQ_PASSWORD = "guest"
# SCRAPY_RABBITMQ_VIRTUAL_HOST = "/"

# --- RabbitMQ Cluster ---
# SCRAPY_RABBITMQ_MODE = "cluster"
# SCRAPY_RABBITMQ_CLUSTER_NODES = ["node2:5672", "node3:5672"]

# --- RabbitMQ Mirrored Queues (HA) ---
# SCRAPY_RABBITMQ_MODE = "mirrored_queues"
# SCRAPY_RABBITMQ_HA_MODE = "exactly"
# SCRAPY_RABBITMQ_HA_PARAMS = "2"
# SCRAPY_RABBITMQ_HA_SYNC_MODE = "automatic"

# --- RabbitMQ SSL/TLS ---
# SCRAPY_RABBITMQ_SSL_ENABLED = True
# SCRAPY_RABBITMQ_SSL_CAFILE = "/path/to/ca.pem"
# SCRAPY_RABBITMQ_SSL_CERTFILE = "/path/to/cert.pem"
# SCRAPY_RABBITMQ_SSL_KEYFILE = "/path/to/key.pem"

# =============================================================================
# ElasticSearch Configuration (Uncomment to use)
# =============================================================================
# Supports modes: standalone (default), cloud

# SCRAPY_BACKEND_TYPE = "elasticsearch"
# SCRAPY_ELASTICSEARCH_HOSTS = ["http://localhost:9200"]

# --- ElasticSearch with Auth ---
# SCRAPY_ELASTICSEARCH_USERNAME = "elastic"
# SCRAPY_ELASTICSEARCH_PASSWORD = "changeme"

# --- ElasticSearch with API Key ---
# SCRAPY_ELASTICSEARCH_API_KEY = "your-api-key"

# --- Elastic Cloud ---
# SCRAPY_ELASTICSEARCH_MODE = "cloud"
# SCRAPY_ELASTICSEARCH_CLOUD_ID = "your-cloud-id"
# SCRAPY_ELASTICSEARCH_API_KEY = "your-api-key"

# --- Custom Index Names ---
# SCRAPY_ELASTICSEARCH_QUEUE_INDEX = "scrapy_queue"
# SCRAPY_ELASTICSEARCH_SET_INDEX = "scrapy_set"
# SCRAPY_ELASTICSEARCH_STORAGE_INDEX = "scrapy_storage"

# =============================================================================
# RocketMQ Configuration (Uncomment to use)
# =============================================================================
# Note: RocketMQ only supports Queue operations (no Set/Storage for dedup/storage)

# SCRAPY_BACKEND_TYPE = "rocketmq"
# SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:9876"

# --- RocketMQ Cluster ---
# SCRAPY_ROCKETMQ_MODE = "cluster"
# SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "namesrv1:9876,namesrv2:9876"

# --- Alibaba Cloud RocketMQ ---
# SCRAPY_ROCKETMQ_MODE = "cloud"
# SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "your-namesrv.addr.aliyun.com:8080"
# SCRAPY_ROCKETMQ_ACCESS_KEY = "your_access_key"
# SCRAPY_ROCKETMQ_SECRET_KEY = "your_secret_key"

# --- RocketMQ Consumer/Producer Settings ---
# SCRAPY_ROCKETMQ_CONSUMER_GROUP = "scrapy-extension-consumer"
# SCRAPY_ROCKETMQ_PRODUCER_GROUP = "scrapy-extension-producer"
# SCRAPY_ROCKETMQ_SEND_TIMEOUT = 3000
