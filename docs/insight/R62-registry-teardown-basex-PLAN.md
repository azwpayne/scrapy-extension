# R62 PLAN

Confirm forced registry teardown holds stale handles and aborts on control
exceptions, detach under the manager lock and disconnect outside it, add
clear-registry and LRU regressions, verify, and make one atomic commit.
