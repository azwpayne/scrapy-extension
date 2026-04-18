"""RocketMQ settings and configuration."""

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RocketMQMode(str, Enum):
    """RocketMQ deployment modes."""

    STANDALONE = "standalone"  # Single namesrv + broker
    CLUSTER = "cluster"  # Multi-broker HA
    CLOUD = "cloud"  # Alibaba Cloud RocketMQ


class RocketMQSettings(BaseSettings):
    """Configuration for RocketMQ backend."""

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_ROCKETMQ_",
        case_sensitive=False,
        extra="ignore",
    )

    # === Mode Selection ===
    mode: RocketMQMode = Field(default=RocketMQMode.STANDALONE)

    # === Connection ===
    namesrv_address: str = Field(default="localhost:9876")
    access_key: str | None = Field(default=None)
    secret_key: str | None = Field(default=None)

    # === Consumer Group ===
    consumer_group: str = Field(default="scrapy-extension-consumer")
    producer_group: str = Field(default="scrapy-extension-producer")

    # === Queue/Priority Settings ===
    max_message_size: int = Field(default=1024 * 1024, ge=0)  # 1MB default
    send_timeout: int = Field(default=3000, ge=0)  # ms

    # === Topic Settings ===
    topic: str = Field(default="scrapy-queue")
    set_topic_suffix: str = Field(default="scrapy-set")
    storage_topic_suffix: str = Field(default="scrapy-storage")
