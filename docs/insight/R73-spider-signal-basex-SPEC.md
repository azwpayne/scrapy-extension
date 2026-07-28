# R73 SPEC — Spider lifecycle signal teardown

Spider lifecycle signal teardown must attempt every registered handler after a
control exception, then re-raise the first. Registration rollback must retain
the original registration failure even if signal cleanup or logging fails.
