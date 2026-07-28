# R61 PLAN

Confirm an interrupted queue-close phase can permanently skip later cleanup,
make all close phases independent while retaining the first control exception,
add deterministic begin-close and snapshot regressions, verify, and make one
atomic commit.
