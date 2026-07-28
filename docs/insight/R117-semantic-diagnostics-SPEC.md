# R117 SPEC

After Scheduler, durable queue, or delayed-queue state has committed, pure diagnostics cannot suppress token abort, report an open scheduler or accepted item as failed, or undo an established drain/clear result. Direct backend, monitor, and lifecycle controls retain their existing semantics.
