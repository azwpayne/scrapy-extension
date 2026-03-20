# Complete Examples & README for scrapy-extension

## TL;DR

> **Quick Summary**: The `examples/` directory is currently a bare Scrapy scaffold with stub spiders that don't use any scrapy-extension features. This plan completes all example code to demonstrate every backend (Redis, MongoDB, Kafka, RabbitMQ), every component (Scheduler, DupeFilter, Pipeline, Queue), and every configuration approach (settings.py, programmatic, environment variables). Also writes a comprehensive `examples/README.md`.
>
> **Deliverables**: 7 spider files, updated settings.py, updated items.py, comprehensive README.md
>
> **Estimated Effort**: Medium
> **Parallel Execution**: YES - 3 waves
> **Critical Path**: Items/Settings foundation → Spider implementations → README

---

## Context

### Original Request
根据"src"目录下的源代码完善"examples/"目录下的代码，并编写相关README.md
(Complete the code in the "examples/" directory based on the source code in the "src" directory, and write a related README.md)

### Interview Summary
**Current State Analysis**:
- `examples/examples/spiders/quotes.py`: Stub spider with `parse(self, response): pass` - doesn't scrape anything
- `examples/examples/spiders/quotes_crawl.py`: Uses wrong XPath selectors for quotes.toscrape.com, doesn't use BackendSpiderMixin
- `examples/examples/settings.py`: Default Scrapy settings with no scrapy-extension configuration
- `examples/examples/items.py`: Empty `ExamplesItem` class with no fields
- `examples/examples/pipelines.py`: Default pipeline, no backend storage
- `examples/examples/middlewares.py`: Default middlewares, no customization
- `examples/README.md`: Only contains "examples ===" (2 lines)
- `examples/scrapy.cfg`: Standard Scrapy config

**Source Code Capabilities to Demonstrate**:
- 4 backends: Redis (4 modes), MongoDB (4 modes), Kafka (3 modes), RabbitMQ (3 modes)
- Components: `BackendScheduler`, `BackendDupeFilter`, `BackendPipeline`, `BackendQueue`
- Spider integration: `BackendSpiderMixin` with `setup_backend()`, `get_queue()`, `get_dupefilter()`, `get_scheduler()`
- Configuration: `Settings`, `RedisSettings`, `MongoDBSettings`, `KafkaSettings`, `RabbitMQSettings`
- Connection: `ConnectionManager` with lazy singleton, retry logic
- Exceptions: `BackendError`, `BackendConnectionError`, `QueueError`, `SerializationError`, `ConfigurationError`

---

## Work Objectives

### Core Objective
Transform the bare Scrapy scaffold in `examples/` into a comprehensive demonstration of all scrapy-extension features, with working spiders that actually scrape quotes.toscrape.com and use backend components.

### Concrete Deliverables
1. `examples/examples/items.py` - Proper QuoteItem with text, author, tags fields
2. `examples/examples/settings.py` - Backend-enabled settings with Redis as default + commented alternatives
3. `examples/examples/spiders/quotes_redis.py` - Spider using Redis backend (BackendSpiderMixin)
4. `examples/examples/spiders/quotes_mongodb.py` - Spider using MongoDB backend
5. `examples/examples/spiders/quotes_kafka.py` - Spider using Kafka backend (queue only)
6. `examples/examples/spiders/quotes_rabbitmq.py` - Spider using RabbitMQ backend (queue only)
7. `examples/examples/spiders/quotes_programmatic.py` - Spider using programmatic settings configuration
8. `examples/examples/spiders/quotes_multi_mode.py` - Spider demonstrating Redis Sentinel/Cluster modes
9. `examples/examples/spiders/quotes_connection_manager.py` - Spider using low-level ConnectionManager API
10. `examples/examples/pipelines.py` - Updated with BackendPipeline integration example
11. `examples/README.md` - Comprehensive guide with structure, prerequisites, running instructions, per-example explanations

### Definition of Done
- [ ] Every spider file actually scrapes quotes.toscrape.com and yields QuoteItem objects
- [ ] Every spider uses scrapy-extension features (not just plain Scrapy)
- [ ] settings.py shows backend configuration with Redis default
- [ ] README.md covers all examples with clear instructions
- [ ] No import errors - all imports reference actual public API from `__init__.py`

### Must Have
- All spiders import from `scrapy_extension` public API only
- Spiders use `BackendSpiderMixin` properly (inheritance order, `setup_backend()`)
- Settings demonstrate `SCRAPY_BACKEND_TYPE` configuration
- README explains how to run each example

### Must NOT Have (Guardrails)
- No direct imports from internal modules (e.g., `scrapy_extension.backends.redis_backend`)
- No invented APIs that don't exist in source code
- No hardcoded credentials (use placeholders like `localhost`)
- Don't delete `quotes.py` or `quotes_crawl.py` (keep original scaffolds for reference, rename them as "basic" examples)

---

## Verification Strategy

### QA Policy
Every spider file will be verified by:
- Import check: `python -c "from examples.spiders.quotes_redis import QuotesRedisSpider"` 
- Syntax check: `python -m py_compile examples/examples/spiders/<file>.py`
- Scrapy list: `cd examples && scrapy list` should show all spider names
- Code review: Verify imports match `scrapy_extension.__init__.__all__`

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — start immediately):
├── Task 1: items.py - Define QuoteItem with proper fields
├── Task 2: settings.py - Backend-enabled settings configuration
└── Task 3: pipelines.py - Updated pipeline example

Wave 2 (After Wave 1 — spider implementations, MAX PARALLEL):
├── Task 4: quotes_redis.py - Redis backend spider
├── Task 5: quotes_mongodb.py - MongoDB backend spider
├── Task 6: quotes_kafka.py - Kafka backend spider
├── Task 7: quotes_rabbitmq.py - RabbitMQ backend spider
├── Task 8: quotes_programmatic.py - Programmatic config spider
├── Task 9: quotes_multi_mode.py - Multi-mode Redis spider
└── Task 10: quotes_connection_manager.py - Low-level API spider

Wave 3 (After Wave 2 — documentation):
└── Task 11: README.md - Comprehensive examples guide
```

---

## TODOs

- [ ] 1. Define QuoteItem in items.py

  **What to do**:
  - Create `QuoteItem` scrapy.Item with fields: `text`, `author`, `tags`
  - Keep `ExamplesItem` as backward-compatible alias
  - Add docstring explaining each field

  **Must NOT do**:
  - Don't add fields not present on quotes.toscrape.com
  - Don't use pydantic models (stick to scrapy.Item for compatibility)

  **References**:
  - quotes.toscrape.com HTML structure: `.quote .text`, `.quote .author`, `.quote .tags .tag`
  - `src/scrapy_extension/components/pipeline.py:107-130` - Pipeline processes items via `dict(item)`

  **Acceptance Criteria**:
  - [ ] QuoteItem has text, author, tags fields
  - [ ] `python -c "from examples.items import QuoteItem"` succeeds

  **Commit**: YES
  - Message: `feat(examples): define QuoteItem with text/author/tags fields`
  - Files: `examples/examples/items.py`

---

- [ ] 2. Configure backend-enabled settings.py

  **What to do**:
  - Keep existing Scrapy settings (BOT_NAME, SPIDER_MODULES, etc.)
  - Add scrapy-extension settings: `SCHEDULER`, `DUPEFILTER_CLASS`, `ITEM_PIPELINES`
  - Set `SCRAPY_BACKEND_TYPE = "redis"` as default
  - Add Redis standalone config (host, port, db)
  - Add commented-out blocks for MongoDB, Kafka, RabbitMQ alternatives
  - Add `SCRAPY_BACKEND_SETTINGS` dict for connection manager

  **Must NOT do**:
  - Don't remove existing Scrapy settings
  - Don't hardcode passwords (use `None` or `"secret"` placeholder)

  **References**:
  - `src/scrapy_extension/settings/base.py` - `Settings` class, `SCRAPY_` prefix
  - `src/scrapy_extension/settings/redis.py:60-64` - `env_prefix="SCRAPY_REDIS_"`
  - `src/scrapy_extension/components/scheduler.py:64-86` - `from_settings` reads `SCRAPY_BACKEND_TYPE`
  - `src/scrapy_extension/components/pipeline.py:55-77` - `from_settings` reads `SCRAPY_PIPELINE_KEY_PREFIX`, `SCRAPY_PIPELINE_TTL`

  **Acceptance Criteria**:
  - [ ] `SCRAPY_BACKEND_TYPE` set to `"redis"`
  - [ ] `SCHEDULER` points to `scrapy_extension.components.scheduler.BackendScheduler`
  - [ ] `DUPEFILTER_CLASS` points to `scrapy_extension.components.dupefilter.BackendDupeFilter`
  - [ ] `ITEM_PIPELINES` includes `scrapy_extension.components.pipeline.BackendPipeline`
  - [ ] Commented MongoDB/Kafka/RabbitMQ blocks present

  **Commit**: YES
  - Message: `feat(examples): configure backend-enabled Scrapy settings`
  - Files: `examples/examples/settings.py`

---

- [ ] 3. Update pipelines.py with BackendPipeline example

  **What to do**:
  - Keep existing `ExamplesPipeline` as simple reference
  - Add comment block showing how BackendPipeline is configured via settings
  - Add optional custom pipeline class that demonstrates manual item processing with backend storage

  **Must NOT do**:
  - Don't remove existing `ExamplesPipeline`
  - Don't create pipeline that requires running backends

  **References**:
  - `src/scrapy_extension/components/pipeline.py:25-130` - BackendPipeline full implementation
  - `src/scrapy_extension/components/pipeline.py:107-130` - `process_item` stores via `get_storage_backend().store()`

  **Acceptance Criteria**:
  - [ ] Original ExamplesPipeline preserved
  - [ ] Comment block explains BackendPipeline integration
  - [ ] `python -m py_compile examples/examples/pipelines.py` passes

  **Commit**: YES
  - Message: `docs(examples): add BackendPipeline integration comments to pipelines.py`
  - Files: `examples/examples/pipelines.py`

---

- [ ] 4. Create quotes_redis.py - Redis backend spider

  **What to do**:
  - Create `QuotesRedisSpider(BackendSpiderMixin, scrapy.Spider)`
  - Set `backend_type = BackendType.REDIS`
  - Call `self.setup_backend()` in `__init__`
  - Implement `parse()` to extract quotes from quotes.toscrape.com
  - Yield `QuoteItem` with text, author, tags
  - Follow pagination (next page link)
  - Show `self.get_queue()` and `self.get_dupefilter()` usage in comments

  **Must NOT do**:
  - Don't set `redis_host`/`redis_port` on class (use settings.py config)
  - Don't hardcode any connection details

  **References**:
  - `src/scrapy_extension/spider_mixin.py:21-310` - BackendSpiderMixin full API
  - `src/scrapy_extension/spider_mixin.py:85-120` - `setup_backend()` method
  - `src/scrapy_extension/spider_mixin.py:193-222` - `get_queue()` method
  - `src/scrapy_extension/spider_mixin.py:224-248` - `get_dupefilter()` method
  - `src/scrapy_extension/spider_mixin.py:53-71` - Class-level attributes (backend_type, redis_host, etc.)
  - `src/scrapy_extension/__init__.py:7-86` - Public API exports

  **Acceptance Criteria**:
  - [ ] Spider inherits from `(BackendSpiderMixin, scrapy.Spider)` in correct order
  - [ ] `backend_type = BackendType.REDIS` set at class level
  - [ ] `setup_backend()` called in `__init__`
  - [ ] `parse()` yields QuoteItem objects from page
  - [ ] Pagination handled via next page link
  - [ ] `python -m py_compile examples/examples/spiders/quotes_redis.py` passes

  **Commit**: YES
  - Message: `feat(examples): add Redis backend spider with full quote scraping`
  - Files: `examples/examples/spiders/quotes_redis.py`

---

- [ ] 5. Create quotes_mongodb.py - MongoDB backend spider

  **What to do**:
  - Create `QuotesMongoDBSpider(BackendSpiderMixin, scrapy.Spider)`
  - Set `backend_type = BackendType.MONGODB`
  - Set `mongodb_uri = "mongodb://localhost:27017"` and `mongodb_db = "scrapy_quotes"`
  - Implement `parse()` to extract and yield QuoteItem
  - Show storage usage via comments

  **Must NOT do**:
  - Don't import pymongo directly (use scrapy_extension abstractions)

  **References**:
  - `src/scrapy_extension/spider_mixin.py:64-65` - mongodb_uri, mongodb_db shortcuts
  - `src/scrapy_extension/spider_mixin.py:151-155` - _build_backend_settings mongodb handling
  - `src/scrapy_extension/settings/mongodb.py:32-193` - MongoDBSettings full config

  **Acceptance Criteria**:
  - [ ] `backend_type = BackendType.MONGODB`
  - [ ] `mongodb_uri` and `mongodb_db` set
  - [ ] Yields QuoteItem objects
  - [ ] `python -m py_compile examples/examples/spiders/quotes_mongodb.py` passes

  **Commit**: YES
  - Message: `feat(examples): add MongoDB backend spider`
  - Files: `examples/examples/spiders/quotes_mongodb.py`

---

- [ ] 6. Create quotes_kafka.py - Kafka backend spider

  **What to do**:
  - Create `QuotesKafkaSpider(BackendSpiderMixin, scrapy.Spider)`
  - Set `backend_type = BackendType.KAFKA`
  - Set `kafka_bootstrap_servers = "localhost:9092"`
  - Implement `parse()` to extract and yield QuoteItem
  - Note in comments: Kafka only supports Queue (no Set/Storage)

  **Must NOT do**:
  - Don't call `get_dupefilter()` (Kafka has no SetBackend)
  - Don't call storage operations (Kafka has no StorageBackend)

  **References**:
  - `src/scrapy_extension/spider_mixin.py:67-68` - kafka_bootstrap_servers shortcut
  - `src/scrapy_extension/backends/base.py:15-28` - BackendType enum
  - `src/scrapy_extension/settings/kafka.py:29-205` - KafkaSettings full config

  **Acceptance Criteria**:
  - [ ] `backend_type = BackendType.KAFKA`
  - [ ] Yields QuoteItem objects
  - [ ] Comment explains Kafka queue-only limitation
  - [ ] `python -m py_compile examples/examples/spiders/quotes_kafka.py` passes

  **Commit**: YES
  - Message: `feat(examples): add Kafka backend spider`
  - Files: `examples/examples/spiders/quotes_kafka.py`

---

- [ ] 7. Create quotes_rabbitmq.py - RabbitMQ backend spider

  **What to do**:
  - Create `QuotesRabbitMQSpider(BackendSpiderMixin, scrapy.Spider)`
  - Set `backend_type = BackendType.RABBITMQ`
  - Set `rabbitmq_url = "amqp://guest:guest@localhost:5672/"`
  - Implement `parse()` to extract and yield QuoteItem
  - Note in comments: RabbitMQ only supports Queue (no Set/Storage)

  **Must NOT do**:
  - Don't call `get_dupefilter()` (RabbitMQ has no SetBackend)
  - Don't call storage operations

  **References**:
  - `src/scrapy_extension/spider_mixin.py:70-71` - rabbitmq_url shortcut
  - `src/scrapy_extension/settings/rabbitmq.py:29-181` - RabbitMQSettings full config

  **Acceptance Criteria**:
  - [ ] `backend_type = BackendType.RABBITMQ`
  - [ ] Yields QuoteItem objects
  - [ ] Comment explains RabbitMQ queue-only limitation
  - [ ] `python -m py_compile examples/examples/spiders/quotes_rabbitmq.py` passes

  **Commit**: YES
  - Message: `feat(examples): add RabbitMQ backend spider`
  - Files: `examples/examples/spiders/quotes_rabbitmq.py`

---

- [ ] 8. Create quotes_programmatic.py - Programmatic configuration spider

  **What to do**:
  - Create spider that configures backend programmatically (not via settings.py)
  - Use `backend_settings` dict attribute on spider class
  - Show `RedisSettings(mode=RedisMode.STANDALONE, ...)` usage in comments
  - Demonstrate how `backend_settings` overrides settings.py values
  - Implement `parse()` yielding QuoteItem

  **Must NOT do**:
  - Don't override settings.py - this shows spider-level config override
  - Don't use Sentinel/Cluster (keep simple standalone for clarity)

  **References**:
  - `src/scrapy_extension/spider_mixin.py:55` - `backend_settings: dict[str, Any] | None = None`
  - `src/scrapy_extension/spider_mixin.py:135-162` - `_build_backend_settings()` merges backend_settings + shortcuts
  - `src/scrapy_extension/settings/redis.py:29-174` - RedisSettings fields

  **Acceptance Criteria**:
  - [ ] `backend_settings` dict used for configuration
  - [ ] Comment shows RedisSettings alternative approach
  - [ ] Yields QuoteItem objects
  - [ ] `python -m py_compile examples/examples/spiders/quotes_programmatic.py` passes

  **Commit**: YES
  - Message: `feat(examples): add programmatic configuration spider`
  - Files: `examples/examples/spiders/quotes_programmatic.py`

---

- [ ] 9. Create quotes_multi_mode.py - Multi-mode deployment spider

  **What to do**:
  - Create spider showing Redis Sentinel configuration
  - Set `backend_settings` with sentinel config: sentinels list, master_name, password
  - Also show Cluster mode config in comments
  - Implement `parse()` yielding QuoteItem
  - Explain when to use each mode (standalone vs sentinel vs cluster)

  **Must NOT do**:
  - Don't actually connect to sentinel/cluster (just show config)
  - Don't show Master-Slave mode (less common, already in README)

  **References**:
  - `src/scrapy_extension/settings/redis.py:134-173` - Sentinel and Cluster settings
  - `src/scrapy_extension/backends/redis_backend.py:146-239` - Sentinel and Cluster connection logic
  - `src/scrapy_extension/settings/redis.py:13-26` - RedisMode enum values

  **Acceptance Criteria**:
  - [ ] Shows Redis Sentinel config with sentinels list
  - [ ] Shows Cluster config in comments
  - [ ] Explains mode selection rationale
  - [ ] `python -m py_compile examples/examples/spiders/quotes_multi_mode.py` passes

  **Commit**: YES
  - Message: `feat(examples): add multi-mode deployment spider (Sentinel/Cluster)`
  - Files: `examples/examples/spiders/quotes_multi_mode.py`

---

- [ ] 10. Create quotes_connection_manager.py - Low-level API spider

  **What to do**:
  - Create spider that directly uses `ConnectionManager` API
  - Show `ConnectionManager.get_manager()` usage
  - Demonstrate `get_queue_backend()`, `get_set_backend()`, `get_storage_backend()`
  - Show `queue_backend.push()`, `queue_backend.pop()`, `queue_backend.queue_len()`
  - Show `set_backend.add()`, `set_backend.contains()`
  - Show `storage_backend.store()`, `storage_backend.retrieve()`
  - Implement `parse()` yielding QuoteItem while also demonstrating direct backend ops

  **Must NOT do**:
  - Don't mix with BackendSpiderMixin (this is the manual/advanced approach)
  - Don't create permanent connections (clean up in spider_closed)

  **References**:
  - `src/scrapy_extension/connection/manager.py:34-263` - ConnectionManager full API
  - `src/scrapy_extension/connection/manager.py:70-101` - `get_manager()` singleton
  - `src/scrapy_extension/connection/manager.py:226-263` - `get_queue_backend()`, `get_set_backend()`, `get_storage_backend()`
  - `src/scrapy_extension/backends/base.py:143-195` - QueueBackend, SetBackend interfaces
  - `src/scrapy_extension/backends/base.py:259-326` - StorageBackend interface

  **Acceptance Criteria**:
  - [ ] Uses `ConnectionManager` directly (not mixin)
  - [ ] Shows all 3 backend interfaces (queue, set, storage)
  - [ ] Demonstrates push/pop/add/store operations
  - [ ] Proper connection cleanup
  - [ ] `python -m py_compile examples/examples/spiders/quotes_connection_manager.py` passes

  **Commit**: YES
  - Message: `feat(examples): add low-level ConnectionManager API spider`
  - Files: `examples/examples/spiders/quotes_connection_manager.py`

---

- [ ] 11. Write comprehensive examples/README.md

  **What to do**:
  - Write full README.md covering:
    - Project overview (what these examples demonstrate)
    - Prerequisites (Redis/MongoDB/Kafka/RabbitMQ, Python 3.10+, uv)
    - Directory structure explanation
    - Quick start (Redis example)
    - Per-example section with: description, what it demonstrates, how to run, key code snippets
    - Backend capabilities comparison table
    - Configuration reference (settings.py vs programmatic vs env vars)
    - Common issues / troubleshooting
    - Links to main project README

  **Must NOT do**:
  - Don't duplicate the main project README's full API docs
  - Don't include commands that require actual running backends (just explain how to run)

  **References**:
  - `README.md` (project root) - Overall project documentation for style reference
  - `src/scrapy_extension/__init__.py` - Public API to reference correctly
  - `src/scrapy_extension/backends/base.py:15-28` - BackendType enum for table
  - All spider files in examples/examples/spiders/ - Descriptions of each

  **Acceptance Criteria**:
  - [ ] Covers all 7+ example spiders
  - [ ] Has prerequisites section
  - [ ] Has quick start section
  - [ ] Has backend capabilities table
  - [ ] Has configuration reference
  - [ ] Correct import paths from public API
  - [ ] Valid markdown formatting

  **Commit**: YES
  - Message: `docs(examples): comprehensive README with all examples documented`
  - Files: `examples/README.md`

---

## Final Verification Wave

- [ ] F1. **Import & Syntax Check** - `unspecified-low`
  Run `python -m py_compile` on all modified files. Run `cd examples && scrapy list` to verify all spiders are discoverable. Check all imports reference `scrapy_extension.__init__.__all__` public API only.
  Output: `Files [N/N compile] | Spiders [N/N listed] | Imports [N/N public] | VERDICT`

- [ ] F2. **Code Quality Review** - `unspecified-high`
  Check for: proper docstrings, consistent naming, no hardcoded credentials, correct BackendSpiderMixin usage pattern (inheritance order, setup_backend() call).
  Output: `Files [N/N clean] | VERDICT`

---

## Commit Strategy

- **1**: `feat(examples): define QuoteItem with text/author/tags fields` - items.py
- **2**: `feat(examples): configure backend-enabled Scrapy settings` - settings.py
- **3**: `docs(examples): add BackendPipeline integration comments` - pipelines.py
- **4**: `feat(examples): add Redis backend spider` - quotes_redis.py
- **5**: `feat(examples): add MongoDB backend spider` - quotes_mongodb.py
- **6**: `feat(examples): add Kafka backend spider` - quotes_kafka.py
- **7**: `feat(examples): add RabbitMQ backend spider` - quotes_rabbitmq.py
- **8**: `feat(examples): add programmatic configuration spider` - quotes_programmatic.py
- **9**: `feat(examples): add multi-mode deployment spider` - quotes_multi_mode.py
- **10**: `feat(examples): add low-level ConnectionManager API spider` - quotes_connection_manager.py
- **11**: `docs(examples): comprehensive README with all examples documented` - README.md

---

## Success Criteria

### Verification Commands
```bash
cd examples && python -m py_compile examples/items.py
cd examples && python -m py_compile examples/settings.py
cd examples && python -m py_compile examples/pipelines.py
cd examples && python -m py_compile examples/spiders/quotes_redis.py
cd examples && python -m py_compile examples/spiders/quotes_mongodb.py
cd examples && python -m py_compile examples/spiders/quotes_kafka.py
cd examples && python -m py_compile examples/spiders/quotes_rabbitmq.py
cd examples && python -m py_compile examples/spiders/quotes_programmatic.py
cd examples && python -m py_compile examples/spiders/quotes_multi_mode.py
cd examples && python -m py_compile examples/spiders/quotes_connection_manager.py
cd examples && scrapy list
# Expected: quotes, quotes_crawl, quotes_redis, quotes_mongodb, quotes_kafka, quotes_rabbitmq, quotes_programmatic, quotes_multi_mode, quotes_connection_manager
```

### Final Checklist
- [ ] All spider files compile without errors
- [ ] All imports use public API only
- [ ] Every spider uses scrapy-extension features
- [ ] README covers all examples
- [ ] No hardcoded passwords
