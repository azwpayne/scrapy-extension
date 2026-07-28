# R68 SPEC — RabbitMQ candidate cleanup error precedence

When a RabbitMQ connection candidate fails before publication, its causal
exception remains the observed failure even if best-effort candidate cleanup
raises a control exception. Cleanup must still attempt both channel and
connection; normal published-client retirement keeps its existing control
exception behavior.
