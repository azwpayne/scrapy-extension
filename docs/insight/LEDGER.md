# Insight Ledger

This ledger prevents previously resolved or rejected findings from being proposed again.
Entries are keyed by `(file:line, root-class)` and use one of these states:
`LANDED`, `REFUTED`, `DEFERRED`, or `DUPLICATE`.

| Round | Finding | Location | Root class | State | Evidence |
|---|---|---|---|---|---|
