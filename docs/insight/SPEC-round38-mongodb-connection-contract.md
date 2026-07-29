# Round 38 — SPEC / PLAN / TASK: MongoDB connection authority and transport contract

**Base:** `main` after the Round-37 queue-routing iteration. **Scope:** the
MongoDB settings-to-`MongoClient` boundary identified by independent security,
runtime, test, and CI reviewers.

## Audit conclusion

The prior URI guard protected `config.uri`, but replica-set members and mongos
routers were later concatenated into a second URI. Database and replica-set
names were also interpolated into its path/query. A mutable configuration could
therefore pass one validation set and reach the driver with later TLS/auth
values. The external authentication modes were treated as one ambient path even
though PyMongo has three different credential contracts.

## Specification

1. A MongoDB URI may describe endpoint/topology only. It must not contain
   userinfo, authentication options, TLS/SSL options, or a malformed authority.
   Raw URI fragments are rejected before standard-library parsing because
   PyMongo can still interpret fragment-suffixed options.
2. Replica and mongos seed lists accept only built-in-string `host`,
   `host:port`, or bracketed/bare IPv6 seeds. They are normalized before use;
   URI authority, userinfo, path, query, fragment, commas, and invalid ports
   fail without echoing input.
3. Generated seed URIs are authority-only (`mongodb://<seeds>/`). Database
   selection remains `client[database]`; replica-set selection remains the
   `replicaSet` driver kwarg. No untrusted value is interpolated into a URI
   path or query.
4. One connect attempt uses one frozen, fully revalidated snapshot of every
   value that reaches PyMongo. Post-snapshot config mutation cannot alter that
   client's endpoint, TLS, authentication, pool, write, or read policy.
5. Authentication is mechanism-specific: GSSAPI requires a username;
   MONGODB-X509 forbids a password; MONGODB-AWS accepts either an ambient
   identity or a complete key/secret pair. External mechanisms use
   `$external`; unsupported custom sources fail early.
6. Authenticated remote connections require verified TLS. The only plaintext
   compatibility path is one literal loopback standalone seed with no
   conflicting topology options; the backend passes `directConnection=True`
   for that narrow case. Replica/sharded topologies, multi-seed URIs, and
   `mongodb+srv://` require TLS because topology discovery can reach remote
   peers. `mongodb+srv://` counts as PyMongo's implicit TLS transport.

## Plan and tasks

1. Add construction-time and connect-time RED tests for URI/seed injection,
   malformed URI handling, topology TLS, external-auth contracts, IPv6, and
   no-SDK-I/O failures.
2. Implement shared settings validators and use their normalized output in a
   frozen backend connection snapshot.
3. Remove derived URI path/query interpolation; update the mode construction
   contract and Atlas example to use dedicated credential settings.
4. Verify focused suites, full non-integration coverage, lint, strict types,
   security audit, build/install smoke, and GitHub Actions before the atomic
   commit/push. A fresh swarm audit decides the next bounded iteration.

## Acceptance evidence

- All rejected authorities, fragments, and policy values raise
  `ConfigurationError` before `MongoClient`; errors do not contain supplied
  credential/token text.
- Plaintext local authentication passes `directConnection=True`; lookalike
  hostnames, multi-seed URIs, and replica/load-balanced URI controls require
  TLS before driver construction.
- Mutating the live settings after snapshot capture leaves the mocked driver's
  URI, TLS, username, and password equal to the validated snapshot.
- Real locked PyMongo constructors accept all three supported external-auth
  shapes with `connect=False`.
- Replica/sharded generated URI assertions contain neither database text nor a
  query; the database is selected through the client handle instead.

## Deferred boundaries

This round does not change MongoDB collection-generation leasing, primary-read
policy, connection-manager exception sanitation, or an atomic copy of the
entire settings mapping. Those remain separately tracked architecture work.
