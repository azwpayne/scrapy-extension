# R94 SPEC

ConnectionManager lifecycle diagnostics must never alter recovery, publication, close, or monitor-callback control flow; genuine backend and monitor control exceptions remain observable.
