# R85 SPEC — DynamoDB public SDK diagnostic redaction

Public DynamoDB storage errors must expose stable operation and key context
without copying SDK diagnostic text, while retaining the original exception as
the chained cause for trusted diagnostics.
