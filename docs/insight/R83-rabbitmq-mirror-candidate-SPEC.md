# R83 SPEC — RabbitMQ mirrored candidate warning abort

A RabbitMQ mirrored-queue candidate that has not returned to outer publication
must close channel and connection if diagnostics raise a control exception,
while preserving that causal exception.
