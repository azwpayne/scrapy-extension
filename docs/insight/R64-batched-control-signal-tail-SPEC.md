# R64 SPEC — Batched storage control-signal tail preservation

When a batched storage backend raises a control exception while an in-memory
batch is flushing, the unwritten item and all later items from that snapshot
must be restored to the buffer before the original exception is re-raised.
The guarantee remains at-least-once for all in-process failures; a process
crash remains outside the scope of this in-memory buffer.
