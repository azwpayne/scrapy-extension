# R49 SPEC — Batched age-flusher start recovery

## Finding

`BatchedStorageStrategy._ensure_flusher()` records its thread before calling
`Thread.start()`. If startup raises, that unstarted object remains installed;
every later store sees a non-null `_flusher` and never retries. The promised
`max_buffer_age_s` flush bound is then permanently disabled.

## Required behavior

If thread startup raises any `BaseException`, clear the provisional flusher
while holding the existing lock and re-raise the original exception. A later
store may start a fresh thread. Successful startup keeps the existing
single-flusher concurrency invariant.

