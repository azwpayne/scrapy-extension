# R49 PLAN — Batched age-flusher start recovery

1. Add a deterministic first-start failure regression.
2. Roll back the provisional thread reference in a `BaseException` handler.
3. Verify focused storage tests, lint, strict types, full pytest, and make one
   atomic commit.
