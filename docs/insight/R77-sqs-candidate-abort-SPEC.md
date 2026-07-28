# R77 SPEC — SQS candidate publication abort

An SQS client candidate interrupted after construction but before generation
publication must close best-effort and preserve the primary failure. A fully
published generation remains authoritative and is never treated as a private
candidate.
