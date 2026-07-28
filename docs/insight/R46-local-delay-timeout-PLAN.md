# R46 PLAN — Local-delay timeout delivery

1. Add deterministic clock-advancing backend doubles that reproduce an item
   becoming ready during one blocking backend wait.
2. Add one small deadline-aware polling loop per strategy, retaining their
   existing local drain and backend/ack mechanisms.
3. Keep time-wheel waits aligned to the next wheel tick; do not change its
   storage or drain algorithm.
4. Run strategy-focused tests, lint, strict type checks, full regression, then
   atomically commit the isolated R46 change.
