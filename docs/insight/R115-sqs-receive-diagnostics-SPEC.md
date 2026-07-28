# R115 SPEC

Once SQS has received a message and created its acknowledgement token, the in-flight-capacity warning cannot make `pop_with_ack()` report failure or lose that token; tracking and broker controls remain observable.
