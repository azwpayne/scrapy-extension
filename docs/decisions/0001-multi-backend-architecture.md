# ADR-001: Multi-Backend Architecture for Scrapy Extension

**Date:** 2026-05-01
**Status:** Accepted
**Supersedes:** None

## Context

We need to build a Scrapy extension providing distributed crawling capabilities that can work with multiple backend technologies. The original `scrapy-redis` only supported Redis, but users have diverse infrastructure requirements:

- Some have Redis clusters already
- Others use MongoDB for document storage
- High-throughput use cases need Kafka
- Reliability-focused deployments prefer RabbitMQ
- Search requirements call for ElasticSearch
- Alibaba Cloud users need RocketMQ support

**Key Requirements:**
1. Components must be backend-agnostic (scheduler, dupefilter, pipeline work with any backend)
2. Support multiple deployment modes per backend (standalone, cluster, cloud)
3. Type-safe configuration via pydantic-settings
4. Lazy singleton connection management to avoid duplicate connections
5. Compatible with standard Scrapy spiders via mixin

## Decision

We adopted a **Unified Backend Interface** pattern with abstract base classes defining contracts:

```
Backend (ABC) — connection lifecycle
├── QueueBackend (ABC) — push, pop, queue_len, clear_queue
├── SetBackend (ABC) — add, remove, contains, set_len, clear_set
└── StorageBackend (ABC) — store, retrieve, delete, exists, ttl, clear_storage
```

Each backend implements whichever interfaces it natively supports:

| Backend       | Implements                    | Notes                                      |
|---------------|-------------------------------|-------------------------------------------|
| Redis         | All three                     | Full-featured reference implementation     |
| MongoDB       | All three                     | Uses collection indexes for sets, TTL docs |
| ElasticSearch | All three                     | Uses sorted indices, unique IDs            |
| Kafka         | QueueBackend only             | Native pub/sub, no atomic sets            |
| RabbitMQ      | QueueBackend only             | Priority queues, no atomic sets            |
| RocketMQ      | QueueBackend only             | Alibaba Cloud RocketMQ, Set/Storage stubs  |

## Alternatives Considered

### Alternative 1: Adapter Pattern per Component
Each component (Scheduler, DupeFilter, Pipeline) would have its own adapter interface.

**Rejected because**: Creates 15+ interfaces (5 components × 3 backends) instead of 4. Violates Interface Segregation — backends implementing only QueueBackend would need empty implementations.

### Alternative 2: Single BackendInterface with All Methods
One interface with optional methods (push, add_fingerprint, store_item, etc.).

**Rejected because**: Violates Interface Segregation — calling `add_fingerprint()` on KafkaBackend would raise `NotImplementedError` at runtime. Better to have compile-time/type checking.

### Alternative 3: Generic Backend with Feature Flags
```python
class Backend:
    def supports_queues(self) -> bool: ...
    def supports_sets(self) -> bool: ...
    def supports_storage(self) -> bool: ...
```

**Rejected because**: Feature flags are runtime checks, not type-safe. Leads to defensive coding with lots of `if hasattr()` checks. The interface matrix approach is clearer.

## Consequences

**Positive:**
- Components are truly backend-agnostic — they only request the interfaces they need
- Type system catches missing implementations at development time
- Clear contract for implementing new backends (implement X interface, pass Y tests)
- ConnectionManager provides singleton-per-config without global state

**Negative:**
- Some backends (Kafka, RabbitMQ, RocketMQ) can't provide deduplication or storage — must document this clearly
- Interface gaps require users to combine backends (e.g., Kafka + Redis) for full features

## Implementation Notes

**Connection Management:**
- `ConnectionManager` uses class-level registry keyed by `backend_type:settings_hash`
- Lazy initialization — connection established on first use, not at import
- Exponential backoff retry configurable via `retry_attempts` and `retry_delay`

**Request Serialization:**
- `BackendQueue` manually serializes Scrapy requests to JSON
- Stores: url, method, headers, body, cookies, meta, encoding, priority, dont_filter, flags
- Callbacks stored by `__name__` (not function reference) for cross-process compatibility

**Key Name Validation:**
- Redis: `^[a-zA-Z0-9._:-]+$`
- Kafka topics: `^[a-zA-Z0-9._-]+$`
- Prevents injection attacks on key/queue/topic names
