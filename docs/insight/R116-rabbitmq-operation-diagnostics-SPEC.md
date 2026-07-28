# R116 SPEC

RabbitMQ diagnostics cannot either leak a prepared private candidate after a successful QoS call or reclassify a broker-delivered, tokenized message as a failed receive; direct QoS, broker, and tracking controls remain observable.
