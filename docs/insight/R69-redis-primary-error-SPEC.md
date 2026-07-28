# R69 SPEC — Redis candidate cleanup error precedence

An unpublished Redis client candidate's cleanup must never replace the
exception that aborted its connection attempt. The candidate still closes
best-effort; normal disconnect of a published generation retains its existing
control-exception behavior.
