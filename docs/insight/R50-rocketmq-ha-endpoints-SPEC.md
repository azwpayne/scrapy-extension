# R50 SPEC — RocketMQ HA endpoint-list configuration

RocketMQ's client accepts semicolon-separated proxy endpoints for cluster HA,
but settings validate the whole value as one `host:port`; valid cluster lists
therefore fail before connect. Settings must validate each non-empty bare
`host:port` member, normalize surrounding whitespace, and pass a canonical
semicolon list to the client.
