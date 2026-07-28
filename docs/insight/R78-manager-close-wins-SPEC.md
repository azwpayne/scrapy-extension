# R78 SPEC — ConnectionManager close-wins candidate cleanup

When ConnectionManager close wins while a backend connection is in flight, its
candidate cleanup is best effort. A cleanup control exception must not replace
the causal released/discarded connection result.
