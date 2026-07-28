# R46 SPEC — Local-delay timeout delivery

## Finding

`DelayQueueStrategy` and `TimeWheelQueueStrategy` drain local delayed items
only once, before passing the entire caller timeout to the backend. An item
that becomes locally ready while that backend call blocks is not published
until a later call, or until the full timeout returns. This violates the public
`pop(timeout)` promise for direct users (the scheduler normally uses `0`).

## Required behavior

For a positive timeout, each strategy must bound a backend wait by the earlier
of the caller deadline and the next local release deadline. After an empty
backend wait reaches a local deadline, it must drain again and retry with the
remaining caller budget. A backend item continues to win immediately, and
`pop_with_ack` must preserve its token. An early empty backend return with no
clock progress must return rather than busy-loop.

Time-wheel local release is bounded by its documented tick granularity: a
non-tick-aligned item is released on its next wheel tick, not before.

## Acceptance criteria

- Delay and time-wheel `pop` return an item that becomes locally ready inside
  the requested timeout.
- Their `pop_with_ack` variants preserve backend tokens under the same path.
- Backend wait durations never exceed the total caller deadline or the next
  local release time.
- Zero-timeout and no-local-item behavior are unchanged.

