# Insight Ledger

This ledger prevents previously resolved or rejected findings from being proposed again.
Entries are keyed by `(file:line, root-class)` and use one of these states:
`LANDED`, `REFUTED`, `DEFERRED`, or `DUPLICATE`.

| Round | Finding | Location | Root class | State | Evidence |
|---|---|---|---|---|---|
| R35 | F7 scheduler-tail-after-basex | schedule/scheduler.py:1462-1541 | scheduler-tail-after-basex | LANDED-blocked-push | commit `5917d38`; push to origin failed (proxy 127.0.0.1:7890 unreachable); awaiting network retry |
| R35 | F8 batched-flusher-start-failure | storage/strategies/batched.py:358-370 | flusher-start-assign-before-start | DEFERRED | LOW; not the lifecycle theme; ship in dedicated R-round |
| R36 | F6 mixin-close-backend-basex | spider/spider_mixin.py:590-636 | mixin-basex-teardown-leak | DEFERRED | MED; complement of F7; ship in this round |
| R36 | F3 monitor-on-disconnect-reason | backends/connectors.py:1312 | monitor-on-disconnect-reason | DEFERRED | MED; separate subsystem (monitor); ship in dedicated R-round |
| R36 | F4 set-monitor-race | backends/connectors.py:1340-1357 | monitor-set-monitor-race | DEFERRED | LOW |
| R36 | F5 monitor-baseexception-propagation | backends/connectors.py:1359-1364 | monitor-baseexception-propagation | DEFERRED | LOW |
| R37 | (none) | — | — | BLOCKED-push | No new scan: R35-F7 push still unresolved; classifier denied push retry (auto-mode [Irreversible Local Destruction]); R-round pipeline stalled until explicit user push authorization or proxy restored |
| R38 | (none) | — | — | BLOCKED-push | Did not attempt push retry; preflight clean otherwise; awaiting explicit user push authorization (or restored origin proxy) to unblock the R-round pipeline |
