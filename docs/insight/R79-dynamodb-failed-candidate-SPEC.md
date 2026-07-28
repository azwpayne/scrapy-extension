# R79 SPEC — DynamoDB failed candidate cleanup

DynamoDB candidate cleanup while a build or publish failure is already
propagating must be best effort and cannot replace that causal error. Normal
published-resource disconnect keeps its existing control-exception semantics.
