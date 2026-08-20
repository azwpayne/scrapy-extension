# R139 PLAN

1. **RED/GREEN A (kafka F1)** — tests: mixed-mode bare ack with in-flight
   token offsets on the record's topic-partition → typed QueueError (no
   commit call); pure-legacy ack commits an explicit offset map for
   `_last_record`'s tp only (never `commit()` bare / never other partitions);
   other partitions' positions untouched. → implement per SPEC Fix A.
2. **RED/GREEN B (rabbitmq F2+F3)** — tests: `queue_len` on a fresh queue
   declares first and returns 0 (no 404, channel alive); `clear_queue` on a
   fresh queue is a no-op success (channel alive). → add
   `_ensure_queue_exists` gates per SPEC Fix B.
3. **RED/GREEN C (rabbitmq F4)** — tests: `RabbitMQSettings` rejects
   `prefetch_size != 0` with ConfigurationError; docstring contract pins for
   the prefetch_count scope note. → SPEC Fix C.
4. **RED/GREEN D (rabbitmq F5)** — docstring pin: timeout entries document
   deadline-honoring semantics (no "unused for RabbitMQ"). → SPEC Fix D.
5. **RED/GREEN E (pulsar F6)** — docstring/comment pin: no
   "unacked-timeout" claim; restart/disconnect redelivery stated. → SPEC
   Fix E.
6. **RED/GREEN F (batched F7)** — test: with a live oldest item the next wait
   interval equals the remaining budget (not the full age); empty-buffer
   wait stays a full age; worst-case flush latency ≈ age. → deadline-driven
   cadence per SPEC Fix F.
7. Full gate → atomic commits: fix(kafka) A, fix(rabbitmq) B+C+D, docs +
   fix(pulsar) E, fix(storage) F, docs(insight) + LEDGER → push HEAD:main.
