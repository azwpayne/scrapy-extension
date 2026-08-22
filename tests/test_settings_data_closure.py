"""Semantic closure tests for settings and data-backend safety boundaries.

These tests exercise decisions at the public settings/backend seams: malformed
configuration is rejected before an SDK factory is called, ambiguous remote
responses are not treated as success, and cleanup/settlement keeps ownership
and generation identity exact.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from elasticsearch import (
    RequestError,
    TransportError,
)
from pydantic import SecretStr

from scrapy_extension.backends import dynamodb as dynamodb_mod
from scrapy_extension.backends import elasticsearch as es_mod
from scrapy_extension.backends import memcached as memcached_mod
from scrapy_extension.backends import mongodb as mongo_mod
from scrapy_extension.backends import redis as redis_mod
from scrapy_extension.backends import sqs as sqs_mod
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.backends.sqs import SqsBackend
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    QueueOutcomeIndeterminateError,
    StorageError,
)
from scrapy_extension.settings import (
    DynamoDBSettings,
    ElasticSearchMode,
    ElasticSearchSettings,
    MemcachedSettings,
    MongoDBMode,
    MongoDBSettings,
    RedisSettings,
    SqsQueueNameGeneration,
    SqsSettings,
)
from scrapy_extension.settings._aws import (
    is_remote_http_endpoint,
    normalize_allow_remote_http,
    validate_aws_credentials,
    validate_aws_endpoint,
    validate_aws_region_name,
)
from scrapy_extension.settings.elasticsearch import _has_safe_host_percent_encoding
from scrapy_extension.settings.mongodb import (
    is_mongodb_direct_loopback_uri,
    validate_mongodb_authentication,
    validate_mongodb_database,
    validate_mongodb_seed_endpoints,
    validate_mongodb_transport_security,
    validate_mongodb_uri,
    validate_mongodb_write_concern,
)

# ---------------------------------------------------------------------------
# Settings: exact type validation, transport policy, and hostile URL shapes.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value", [None, 123, "US-east-1", "us-east", "us--east-1", "us-east-x"]
)
def test_aws_region_rejects_noncanonical_values(value: object) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_aws_region_name(value)
    assert exc_info.value.setting_name == "region_name"


@pytest.mark.parametrize("value", ["us-east-1", "us-gov-west-1", "eusc-de-east-1"])
def test_aws_region_accepts_future_partition_shapes(value: str) -> None:
    assert validate_aws_region_name(value) == value


@pytest.mark.parametrize("value", [SecretStr("secret"), "secret"])
def test_aws_credentials_accept_secretstr_and_plaintext_pair(value: object) -> None:
    assert validate_aws_credentials(value, SecretStr("other")) == ("secret", "other")


@pytest.mark.parametrize(
    "access, secret, missing",
    [(None, "s", "aws_access_key_id"), ("a", None, "aws_secret_access_key")],
)
def test_aws_credentials_reject_partial_pairs(
    access: object, secret: object, missing: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_aws_credentials(access, secret)  # type: ignore[arg-type]
    assert exc_info.value.setting_name == missing


@pytest.mark.parametrize("value", ["", "  ", SecretStr("\t")])
def test_aws_credentials_reject_blank_values(value: object) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_aws_credentials(value, None)  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "aws_access_key_id"


@pytest.mark.parametrize("value", [True, False, " true ", "FALSE"])
def test_aws_remote_http_boolean_parser_accepts_canonical_values(value: object) -> None:
    assert normalize_allow_remote_http(value) is (
        value is True or (isinstance(value, str) and value.strip().lower() == "true")
    )


@pytest.mark.parametrize("value", [1, 0, "yes", None, object()])
def test_aws_remote_http_boolean_parser_rejects_truthy_lookalikes(
    value: object,
) -> None:
    with pytest.raises(ConfigurationError):
        normalize_allow_remote_http(value)


def test_aws_endpoint_rejects_policy_flag_types_and_unsafe_authority() -> None:
    with pytest.raises(ConfigurationError, match="policy flags"):
        validate_aws_endpoint("https://localhost:4566", cloud="false")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="credential policy"):
        validate_aws_endpoint(
            "https://localhost:4566", cloud=False, explicit_credentials="yes"
        )  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="userinfo"):
        validate_aws_endpoint("https://user:secret@localhost:4566", cloud=False)
    with pytest.raises(ConfigurationError, match="query"):
        validate_aws_endpoint("https://localhost:4566?x=1", cloud=False)
    with pytest.raises(ConfigurationError, match="HTTPS"):
        validate_aws_endpoint("http://localhost:4566", cloud=True)


def test_aws_endpoint_accepts_cloud_https_and_loopback_http() -> None:
    assert (
        validate_aws_endpoint("https://aws.example:443", cloud=True)
        == "https://aws.example:443"
    )
    assert (
        validate_aws_endpoint(
            "http://127.0.0.1:4566", cloud=False, explicit_credentials=True
        )
        == "http://127.0.0.1:4566"
    )
    assert is_remote_http_endpoint("http://127.0.0.1:4566") is False
    assert is_remote_http_endpoint("http://aws.example:4566") is True


@pytest.mark.parametrize(
    ("netloc", "hostname", "expected"),
    [
        ("[fe80::1%25en0]:9200", "fe80::1%25en0", True),
        ("[fe80::1%en0]:9200", "fe80::1%en0", False),
        ("[fe80::1%25bad%zone]:9200", "fe80::1%25bad%zone", False),
        ("[not-an-ip%25en0]:9200", "not-an-ip%25en0", False),
        ("example:9200", "example", True),
        (123, "example", False),
    ],
)
def test_elasticsearch_ipv6_zone_safelist_is_static(
    netloc: object, hostname: object, expected: bool
) -> None:
    assert _has_safe_host_percent_encoding(netloc, hostname) is expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"hosts": []},
        {"hosts": ["https://es.example:65536"]},
        {"hosts": ["https://es.example:9200"], "queue_index": "."},
        {"hosts": ["https://es.example:9200"], "queue_index": "Bad"},
        {
            "hosts": ["https://es.example:9200"],
            "queue_index": "same",
            "set_index": "same",
        },
    ],
)
def test_elasticsearch_settings_reject_empty_authority_and_unsafe_indices(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ConfigurationError):
        ElasticSearchSettings(**kwargs)  # type: ignore[arg-type]


def test_elasticsearch_cloud_tls_policy_rejects_ignored_or_unverified_options() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(
            mode=ElasticSearchMode.CLOUD,
            cloud_id="cloud:abc",
            api_key="key",
            ca_certs="/tmp/ca.pem",
        )
    assert exc_info.value.setting_name == "ca_certs"
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(
            mode=ElasticSearchMode.CLOUD,
            cloud_id="cloud:abc",
            api_key="key",
            verify_certs=False,
        )
    assert exc_info.value.setting_name == "verify_certs"
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(hosts=["http://localhost:9200"], ca_certs="/tmp/ca.pem")
    assert exc_info.value.setting_name == "ca_certs"
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(hosts=["http://localhost:9200"], verify_certs=False)
    assert exc_info.value.setting_name == "verify_certs"


def test_elasticsearch_remote_tls_requires_verified_certificates() -> None:
    with pytest.raises(ConfigurationError, match="Remote or authenticated"):
        ElasticSearchSettings(hosts=["https://es.example:9200"], verify_certs=False)
    with pytest.raises(ConfigurationError, match="cleartext"):
        ElasticSearchSettings(hosts=["http://localhost:9200"], api_key="secret")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("mongodb://localhost:27017", "mongodb://localhost:27017"),
        ("mongodb://[::1]:27017", "mongodb://[::1]:27017"),
    ],
)
def test_mongodb_uri_accepts_safe_authorities(value: str, expected: str) -> None:
    assert validate_mongodb_uri(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "mongodb://user:secret@host:27017",
        "mongodb://host:27017/?tlsInsecure=",
        "mongodb://host:27017/?proxyPassword=secret",
        "mongodb+srv://[::1]",
        "mongodb+srv://host:27017",
        "mongodb://host:notaport",
        "mongodb://host:27017#fragment",
    ],
)
def test_mongodb_uri_rejects_userinfo_security_options_and_ambiguous_authority(
    value: str,
) -> None:
    with pytest.raises(ConfigurationError):
        validate_mongodb_uri(value)


@pytest.mark.parametrize(
    "value", [None, "", "name/with/slash", "name\\with\\slash", "a" * 64, "\ud800"]
)
def test_mongodb_database_rejects_invalid_wire_names(value: object) -> None:
    with pytest.raises(ConfigurationError):
        validate_mongodb_database(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            ["127.0.0.1", "[::1]:27017", "DB.EXAMPLE.:27018"],
            ("127.0.0.1", "[::1]:27017", "db.example.:27018"),
        ),
        (("host", "host:0001"), ("host", "host:1")),
    ],
)
def test_mongodb_seed_endpoints_normalize_ip_dns_and_ports(
    value: object, expected: tuple[str, ...]
) -> None:
    assert validate_mongodb_seed_endpoints(value, "replica_set_members") == expected


@pytest.mark.parametrize(
    "value", [["host:0"], ["host:65536"], ["host/path"], ["[127.0.0.1]"], ["bad host"]]
)
def test_mongodb_seed_endpoints_reject_unsafe_generated_uri_values(
    value: object,
) -> None:
    with pytest.raises(ConfigurationError):
        validate_mongodb_seed_endpoints(value, "mongos_routers")


@pytest.mark.parametrize(
    ("w", "timeout", "expected"),
    [
        (1, None, (1, None)),
        (" majority ", 0, ("majority", 0)),
        (" 2 ", " 50 ", (2, 50)),
    ],
)
def test_mongodb_write_concern_normalizes_acknowledged_values(
    w: object, timeout: object, expected: tuple[object, object]
) -> None:
    assert validate_mongodb_write_concern(w, timeout) == expected


@pytest.mark.parametrize("w", [False, 0, -1, "0", "wat", [], object()])
def test_mongodb_write_concern_rejects_unacknowledged_or_hostile_w(w: object) -> None:
    with pytest.raises(ConfigurationError):
        validate_mongodb_write_concern(w, None)


@pytest.mark.parametrize("timeout", [True, -1, "-1", "wat", [], object()])
def test_mongodb_write_concern_rejects_hostile_timeout(timeout: object) -> None:
    with pytest.raises(ConfigurationError):
        validate_mongodb_write_concern(1, timeout)


@pytest.mark.parametrize(
    ("mechanism", "username", "password", "source", "expected"),
    [
        ("GSSAPI", "principal", None, "admin", True),
        ("MONGODB-X509", None, None, "$external", True),
        ("MONGODB-AWS", "access", SecretStr("secret"), "$external", True),
        ("SCRAM-SHA-256", "user", SecretStr("secret"), "admin", True),
        (None, None, None, "admin", False),
    ],
)
def test_mongodb_authentication_accepts_mechanism_specific_shapes(
    mechanism: object,
    username: object,
    password: object,
    source: object,
    expected: bool,
) -> None:
    assert (
        validate_mongodb_authentication(username, password, mechanism, source)
        is expected
    )


@pytest.mark.parametrize(
    ("mechanism", "kwargs", "setting"),
    [
        ("GSSAPI", {"username": None}, "username"),
        ("GSSAPI", {"username": "user", "auth_source": "other"}, "auth_source"),
        ("MONGODB-X509", {"password": "secret"}, "password"),
        ("MONGODB-AWS", {"username": "user"}, "password"),
        ("SCRAM-SHA-256", {"username": None, "password": None}, "username"),
        ("not-supported", {}, "auth_mechanism"),
    ],
)
def test_mongodb_authentication_rejects_ambiguous_or_unsupported_shapes(
    mechanism: object, kwargs: dict[str, object], setting: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_authentication(
            kwargs.get("username"),
            kwargs.get("password"),
            mechanism,
            kwargs.get("auth_source", "admin"),
        )
    assert exc_info.value.setting_name == setting


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("mongodb://localhost:27017", True),
        ("mongodb://[::1]:27017/?directConnection=true", True),
        ("mongodb://localhost:27017/?directConnection=false", False),
        ("mongodb://localhost:27017/?replicaSet=rs0", False),
        ("mongodb+srv://localhost", False),
        ("mongodb://localhost:27017#bad", False),
        ("not-a-uri", False),
    ],
)
def test_mongodb_loopback_classifier_requires_direct_topology(
    uri: str, expected: bool
) -> None:
    assert is_mongodb_direct_loopback_uri(uri) is expected


def test_mongodb_transport_security_covers_local_tls_and_remote_opt_in() -> None:
    validate_mongodb_transport_security(
        mode=MongoDBMode.STANDALONE,
        uri="mongodb://localhost:27017",
        replica_set_members=[],
        mongos_routers=[],
        tls_enabled=False,
        tls_allow_invalid_certificates=False,
        username="user",
        password="secret",
        auth_mechanism="SCRAM-SHA-256",
    )
    validate_mongodb_transport_security(
        mode=MongoDBMode.STANDALONE,
        uri="mongodb://remote.internal:27017",
        replica_set_members=[],
        mongos_routers=[],
        tls_enabled=False,
        tls_allow_invalid_certificates=False,
        username=None,
        password=None,
        auth_mechanism=None,
        allow_remote_plaintext=True,
    )
    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_transport_security(
            mode=MongoDBMode.STANDALONE,
            uri="mongodb://localhost:27017",
            replica_set_members=[],
            mongos_routers=[],
            tls_enabled=False,
            tls_allow_invalid_certificates=False,
            username=None,
            password=None,
            auth_mechanism=None,
            tls_ca_file="ca.pem",
        )
    assert exc_info.value.setting_name == "tls_ca_file"
    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_transport_security(
            mode=MongoDBMode.STANDALONE,
            uri="mongodb://remote.internal:27017",
            replica_set_members=[],
            mongos_routers=[],
            tls_enabled=False,
            tls_allow_invalid_certificates=False,
            username="user",
            password="secret",
            auth_mechanism="SCRAM-SHA-256",
        )
    assert exc_info.value.setting_name == "tls_enabled"


# ---------------------------------------------------------------------------
# Shared backend doubles.  They expose only SDK seams observed by public APIs.
# ---------------------------------------------------------------------------


def _mongo_backend(
    mocker, **settings_kwargs
) -> tuple[MongoDBBackend, MagicMock, MagicMock, MagicMock, MagicMock]:
    backend = MongoDBBackend(MongoDBSettings(**settings_kwargs))
    client = mocker.MagicMock(name="mongo-client")
    database = mocker.MagicMock(name="mongo-database")
    queue = mocker.MagicMock(name="queue-collection")
    sets = mocker.MagicMock(name="set-collection")
    storage = mocker.MagicMock(name="storage-collection")
    client.__getitem__.return_value = database
    database.__getitem__.side_effect = {
        "queues": queue,
        "sets": sets,
        "storage": storage,
    }.__getitem__
    backend._client, backend._db = client, database
    backend._queue_collection, backend._set_collection, backend._storage_collection = (
        queue,
        sets,
        storage,
    )
    return backend, client, queue, sets, storage


def _es_backend(mocker, **settings_kwargs) -> tuple[ElasticSearchBackend, MagicMock]:
    backend = ElasticSearchBackend(ElasticSearchSettings(**settings_kwargs))
    client = mocker.MagicMock(name="es-client")
    client.options.return_value = client
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()
    backend._generation = es_mod.ElasticSearchBackend._build_generation(
        client, backend._connection_snapshot
    )
    return backend, client


def _sqs_backend(mocker, **settings_kwargs) -> tuple[SqsBackend, MagicMock]:
    backend = SqsBackend(SqsSettings(**settings_kwargs))
    client = mocker.MagicMock(name="sqs-client")
    client.get_queue_url.return_value = {"QueueUrl": "https://sqs.example/queue"}
    client.list_queue_tags.return_value = {"Tags": {}}
    session = mocker.MagicMock(name="sqs-session")
    session.client.return_value = client
    mocker.patch.object(sqs_mod.boto3.session, "Session", return_value=session)
    backend.connect()
    return backend, client


def _ddb_backend(mocker, **settings_kwargs) -> tuple[DynamoDBBackend, MagicMock]:
    backend = DynamoDBBackend(DynamoDBSettings(**settings_kwargs))
    resource = mocker.MagicMock(name="ddb-resource")
    table = mocker.MagicMock(name="ddb-table")
    table.load.return_value = None
    table.table_status = "ACTIVE"
    resource.Table.return_value = table
    session = mocker.MagicMock(name="ddb-session")
    session.resource.return_value = resource
    mocker.patch.object(dynamodb_mod.boto3.session, "Session", return_value=session)
    backend.connect()
    return backend, table


def _redis_backend(mocker, **settings_kwargs) -> tuple[RedisBackend, MagicMock]:
    client = mocker.MagicMock(name="redis-client")
    client.ping.return_value = True
    mocker.patch.object(redis_mod, "Redis", return_value=client)
    backend = RedisBackend(RedisSettings(**settings_kwargs))
    backend.connect()
    return backend, client


def _memcached_backend(mocker, **settings_kwargs) -> tuple[MemcachedBackend, MagicMock]:
    client = mocker.MagicMock(name="memcached-client")
    client.stats.return_value = {}
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    backend = MemcachedBackend(MemcachedSettings(**settings_kwargs))
    backend.connect()
    return backend, client


# ---------------------------------------------------------------------------
# MongoDB: candidate graph, marker ownership, cleanup, and operation outcomes.
# ---------------------------------------------------------------------------


def test_mongodb_snapshot_rejects_mutated_types_before_client_factory(mocker) -> None:
    backend = MongoDBBackend(MongoDBSettings())
    backend.config.journal = "yes"  # type: ignore[assignment]
    factory = mocker.patch.object(mongo_mod, "MongoClient")
    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert exc_info.value.setting_name == "journal"
    factory.assert_not_called()


def test_mongodb_client_kwargs_cover_tls_auth_cache_and_direct_connection(
    mocker,
) -> None:
    backend = MongoDBBackend(
        MongoDBSettings(
            username="user",
            password="secret",
            auth_mechanism="SCRAM-SHA-256",
            tls_enabled=True,
            tls_ca_file="ca.pem",
            tls_key_file="client.key",
            w_timeout_ms=25,
            read_preference="secondary",
        )
    )
    snapshot = backend._capture_connection_snapshot()
    kwargs = backend._build_client_kwargs(snapshot)
    assert kwargs["tls"] is True
    assert kwargs["tlsCertificateKeyFile"] == "client.key"
    assert kwargs["authSource"] == "admin"
    assert kwargs["wtimeoutMS"] == 25
    assert kwargs["readPreference"] == "secondary"
    assert backend._build_client_kwargs() == kwargs
    external = MongoDBBackend(
        MongoDBSettings(auth_mechanism="MONGODB-AWS", username="a", password="s")
    )
    external_kwargs = external._build_client_kwargs(
        external._capture_connection_snapshot(), cache=False
    )
    assert external_kwargs["authSource"] == "$external"


def test_mongodb_collection_marker_claims_existing_conflict_and_poison_states(
    mocker,
) -> None:
    collection = mocker.MagicMock()
    collection.find.return_value.limit.return_value = iter(
        [{"scrapy_extension_capability_domain": [{"domain": "queue"}]}]
    )
    mongo_mod.MongoDBBackend._claim_collection_domain(collection, "queue")
    collection.insert_one.assert_not_called()
    with pytest.raises(ConfigurationError):
        mongo_mod.MongoDBBackend._require_matching_collection_domain(
            [{"domain": "storage"}], "queue"
        )
    collection.find.return_value.limit.return_value = iter([{}, {}])
    with pytest.raises(ConfigurationError, match="conflicting"):
        mongo_mod.MongoDBBackend._read_collection_domain_marker(collection)


def test_mongodb_marker_duplicate_without_winner_is_backend_failure(mocker) -> None:
    from pymongo.errors import DuplicateKeyError

    collection = mocker.MagicMock()
    collection.find.return_value.limit.return_value = iter([])
    collection.insert_one.side_effect = DuplicateKeyError("race")
    with pytest.raises(BackendConnectionError, match="domain marker"):
        mongo_mod.MongoDBBackend._claim_collection_domain(collection, "queue")


def test_mongodb_indexes_and_public_operations_keep_domains_and_cleanup(mocker) -> None:
    backend, client, queue, sets, storage = _mongo_backend(mocker)
    backend._create_indexes((queue, sets, storage))
    queue.create_index.assert_called_once()
    sets.create_index.assert_called_once_with(
        [("set_name", mongo_mod.ASCENDING), ("item_hash", mongo_mod.ASCENDING)],
        unique=True,
    )
    assert storage.create_index.call_count == 2
    queue.insert_one.return_value = None
    queue.find_one_and_delete.return_value = {
        "queue_name": "jobs",
        "item": b"x",
        "priority": 0,
        "created_at": datetime.now(timezone.utc),
    }
    sets.insert_one.return_value = None
    sets.delete_one.return_value.deleted_count = 1
    sets.find_one.return_value = {"item_hash": "x"}
    sets.count_documents.return_value = 1
    storage.replace_one.return_value = None
    storage.find_one.side_effect = [{"key": "k", "data": b"v"}, None]
    storage.delete_one.return_value.deleted_count = 1
    storage.count_documents.return_value = 1
    backend.push("jobs", b"x")
    assert backend.pop("jobs") == b"x"
    assert backend.add("s", b"x") is True
    assert backend.remove("s", b"x") is True
    assert backend.contains("s", b"x") is True
    assert backend.set_len("s") == 1
    backend.store("k", b"v")
    assert backend.retrieve("k") == b"v"
    assert backend.delete("k") is True
    client.admin.command.assert_not_called()


@pytest.mark.parametrize(
    "result",
    [
        None,
        {
            "queue_name": "other",
            "item": b"x",
            "priority": 0,
            "created_at": datetime.now(timezone.utc),
        },
        {
            "queue_name": "jobs",
            "item": "not-bytes",
            "priority": 0,
            "created_at": datetime.now(timezone.utc),
        },
    ],
)
def test_mongodb_pop_rejects_or_returns_ambiguous_driver_results(
    mocker, result
) -> None:
    backend, _client, queue, _sets, _storage = _mongo_backend(mocker)
    queue.find_one_and_delete.return_value = result
    if result is None:
        assert backend.pop("jobs") is None
    else:
        with pytest.raises(QueueError) as exc_info:
            backend.pop("jobs")
        assert exc_info.value.operation == "pop"
        assert str(exc_info.value) == "MongoDB queue pop failed."


# ---------------------------------------------------------------------------
# Elasticsearch: response proof, index cleanup, and outcome ambiguity.
# ---------------------------------------------------------------------------


def _es_shards(
    *, total: int = 1, successful: int = 1, failed: int = 0
) -> dict[str, int]:
    return {"total": total, "successful": successful, "failed": failed}


def _es_index_response(
    index: str, doc_id: str, result: str = "created"
) -> dict[str, object]:
    return {"result": result, "_index": index, "_id": doc_id, "_shards": _es_shards()}


def _es_delete_by_query_response() -> dict[str, object]:
    return {
        "timed_out": False,
        "failures": [],
        "total": 0,
        "deleted": 0,
        "version_conflicts": 0,
    }


def test_elasticsearch_response_validators_reject_ambiguous_proofs() -> None:
    assert (
        es_mod._validate_search_response(
            {
                "timed_out": False,
                "_shards": _es_shards(),
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
            }
        )
        == []
    )
    with pytest.raises(es_mod._ElasticSearchResponseError):
        es_mod._validate_search_response(
            {
                "timed_out": True,
                "_shards": _es_shards(),
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
            }
        )
    with pytest.raises(es_mod._ElasticSearchResponseError):
        es_mod._validate_shards(
            {"_shards": {"total": 2, "successful": 1, "failed": 1}},
            require_success=True,
        )
    with pytest.raises(es_mod._ElasticSearchResponseError):
        es_mod._validate_index_create_response(
            {"acknowledged": True, "shards_acknowledged": False, "index": "x"},
            expected_index="x",
        )
    with pytest.raises(es_mod._ElasticSearchResponseError):
        es_mod._validate_delete_response(
            {"_shards": _es_shards(), "_index": "x", "_id": "id", "result": "updated"},
            expected_index="x",
            expected_id="id",
        )
    assert (
        es_mod._validate_delete_response(
            {
                "_shards": _es_shards(),
                "_index": "x",
                "_id": "id",
                "result": "not_found",
            },
            expected_index="x",
            expected_id="id",
        )
        is False
    )


def test_elasticsearch_delete_by_query_validator_checks_optional_counters_and_rate() -> (
    None
):
    response = {
        **_es_delete_by_query_response(),
        "took": 1,
        "updated": 0,
        "batches": 1,
        "noops": 0,
        "retries": {"bulk": 0, "search": 0},
        "requests_per_second": -1.0,
        "task": "task/1",
    }
    es_mod._validate_delete_by_query_response(response)
    for field, value in [
        ("failures", [{}]),
        ("timed_out", True),
        ("requests_per_second", 0),
        ("task", ""),
    ]:
        bad = {**response, field: value}
        with pytest.raises(es_mod._ElasticSearchResponseError):
            es_mod._validate_delete_by_query_response(bad)


def test_elasticsearch_connect_try_create_accepts_existing_and_rejects_other_request_errors(
    mocker,
) -> None:
    backend = ElasticSearchBackend(
        ElasticSearchSettings(hosts=["https://localhost:9200"])
    )
    client = mocker.MagicMock()
    client.indices.create.side_effect = RequestError(
        "exists",
        mocker.MagicMock(status=400),
        {"error": {"type": "resource_already_exists_exception"}},
    )
    backend._ensure_indices(backend._capture_connection_snapshot(), client=client)
    assert client.indices.create.call_count == 3
    other = mocker.MagicMock()
    other.indices.create.side_effect = RequestError(
        "bad",
        mocker.MagicMock(status=400),
        {"error": {"type": "mapper_parsing_exception"}},
    )
    with pytest.raises(RequestError):
        backend._ensure_indices(backend._capture_connection_snapshot(), client=other)


def test_elasticsearch_queue_and_storage_operations_preserve_outcome_ambiguity(
    mocker,
) -> None:
    backend, client = _es_backend(mocker, hosts=["https://localhost:9200"])
    generation = backend._generation
    assert generation is not None
    client.indices.refresh.return_value = {"_shards": _es_shards()}
    client.index.side_effect = lambda **kwargs: _es_index_response(
        kwargs["index"], kwargs["id"]
    )
    client.delete.side_effect = lambda **kwargs: {
        **_es_index_response(kwargs["index"], kwargs["id"], "deleted")
    }
    client.count.return_value = {"count": 0, "_shards": _es_shards()}
    client.delete_by_query.return_value = _es_delete_by_query_response()
    backend.push("jobs", b"x")
    assert backend.add("seen", b"x") is True
    assert backend.remove("seen", b"x") is True
    backend.store("key", b"value")
    assert backend.delete("key") is True
    backend.clear_queue("jobs")
    backend.clear_set("seen")
    backend.clear_storage(prefix="tenant:")
    client.index.side_effect = TransportError("unknown write result")
    with pytest.raises(QueueOutcomeIndeterminateError):
        backend.push("jobs", b"x")


def test_elasticsearch_storage_schema_and_expiry_are_fail_closed(mocker) -> None:
    backend, client = _es_backend(mocker, hosts=["https://localhost:9200"])
    client.get.return_value = {
        "_source": {"data": "eA==", "expireAt": "2000-01-01T00:00:00"},
        "_seq_no": 1,
        "_primary_term": 1,
    }
    client.delete.return_value = _es_index_response("scrapy_storage", "key", "deleted")
    assert backend.retrieve("key") is None
    client.get.return_value = {"_source": {"data": "%%%"}}
    with pytest.raises(StorageError):
        backend.retrieve("key")
    client.get.return_value = {"_source": {"data": "eA==", "expireAt": "not-iso"}}
    with pytest.raises(StorageError):
        backend.ttl("key")


# ---------------------------------------------------------------------------
# SQS: ownership, payload proof, stale generations, and cleanup fencing.
# ---------------------------------------------------------------------------


def test_sqs_physical_name_generations_are_disjoint_and_type_safe() -> None:
    v2 = sqs_mod._physical_queue_name("scrapy-", "jobs", SqsQueueNameGeneration.V2)
    legacy = sqs_mod._physical_queue_name(
        "scrapy-", "jobs", SqsQueueNameGeneration.LEGACY_V1
    )
    assert v2 != legacy
    with pytest.raises(ValueError):
        sqs_mod._physical_queue_name("scrapy-", object(), SqsQueueNameGeneration.V2)  # type: ignore[arg-type]


def test_sqs_existing_v2_queue_requires_exact_owner(mocker) -> None:
    backend, client = _sqs_backend(mocker)
    client.list_queue_tags.return_value = {"Tags": {}}
    with pytest.raises(QueueError) as exc_info:
        backend.push("jobs", b"x")
    assert exc_info.value.operation == "push"
    client.get_queue_url.return_value = {"QueueUrl": ""}
    backend.disconnect()
    backend, client = _sqs_backend(mocker)
    client.get_queue_url.return_value = {"QueueUrl": ""}
    with pytest.raises(QueueError) as exc_info:
        backend.push("jobs", b"x")
    assert exc_info.value.operation == "push"


@pytest.mark.parametrize("body", ["%%%", "", 1])
def test_sqs_poison_body_is_deleted_once_without_retry(mocker, body: object) -> None:
    backend, client = _sqs_backend(mocker)
    client.list_queue_tags.return_value = {
        "Tags": {
            sqs_mod._V2_QUEUE_OWNER_TAG_KEY: sqs_mod._v2_queue_owner("scrapy-", "jobs")
        }
    }
    client.receive_message.return_value = {
        "Messages": [{"Body": body, "ReceiptHandle": "receipt"}]
    }
    with pytest.raises(QueueError) as exc_info:
        backend.pop("jobs")
    assert exc_info.value.operation == "pop"
    client.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.example/queue", ReceiptHandle="receipt"
    )


def test_sqs_token_settlement_is_exactly_once_and_stale_tokens_do_not_retry(
    mocker,
) -> None:
    backend, client = _sqs_backend(mocker)
    client.list_queue_tags.return_value = {
        "Tags": {
            sqs_mod._V2_QUEUE_OWNER_TAG_KEY: sqs_mod._v2_queue_owner("scrapy-", "jobs")
        }
    }
    client.receive_message.return_value = {
        "Messages": [
            {"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "receipt"}
        ]
    }
    body, token = backend.pop_with_ack("jobs")
    assert body == b"x" and token is not None
    backend.ack("jobs", token=token)
    backend.ack("jobs", token=token)
    assert client.delete_message.call_count == 1
    client.delete_message.reset_mock()
    backend.disconnect()
    backend.ack("jobs", token=token)
    client.delete_message.assert_not_called()


def test_sqs_queue_length_validates_all_depth_attributes(mocker) -> None:
    backend, client = _sqs_backend(mocker)
    client.list_queue_tags.return_value = {
        "Tags": {
            sqs_mod._V2_QUEUE_OWNER_TAG_KEY: sqs_mod._v2_queue_owner("scrapy-", "jobs")
        }
    }
    client.get_queue_attributes.return_value = {
        "Attributes": {name: "1" for name in sqs_mod._QUEUE_DEPTH_ATTRIBUTES}
    }
    assert backend.queue_len("jobs") == 3
    client.get_queue_attributes.return_value = {"Attributes": {}}
    with pytest.raises(QueueError):
        backend.queue_len("jobs")


# ---------------------------------------------------------------------------
# DynamoDB: response schema, conditional clear, TTL and remote signing.
# ---------------------------------------------------------------------------


def test_dynamodb_remote_http_snapshot_uses_unsigned_private_resource(mocker) -> None:
    backend, _table = _ddb_backend(
        mocker, endpoint_url="http://aws-proxy.example:4566", allow_remote_http=True
    )
    resource_call = dynamodb_mod.boto3.session.Session.return_value.resource.call_args
    assert resource_call.kwargs["config"].signature_version == dynamodb_mod.UNSIGNED
    assert backend._generation is not None
    assert backend._generation.snapshot.allow_remote_http is True


def test_dynamodb_response_helpers_reject_nonmappings_and_wrong_keys() -> None:
    with pytest.raises(StorageError):
        DynamoDBBackend._response_item([], "retrieve", "key")
    with pytest.raises(StorageError):
        DynamoDBBackend._response_item({"Item": []}, "retrieve", "key")
    assert DynamoDBBackend._response_item({}, "retrieve", "key") is None
    assert DynamoDBBackend._response_deleted({}, "key") is False
    with pytest.raises(StorageError):
        DynamoDBBackend._response_deleted({"Attributes": {"pk": "other"}}, "key")


@pytest.mark.parametrize(
    "expiry", [True, "tomorrow", Decimal("NaN"), Decimal("1E+10000")]
)
def test_dynamodb_expiry_schema_rejects_hostile_values(expiry: object) -> None:
    with pytest.raises(StorageError):
        DynamoDBBackend._validated_expiry({"expire_at": expiry}, "ttl", "key")


def test_dynamodb_store_missing_table_uses_typed_error_and_clear_uses_cas(
    mocker,
) -> None:
    backend, table = _ddb_backend(mocker)
    missing = Exception("missing")
    missing.response = {"Error": {"Code": "ResourceNotFoundException"}}  # type: ignore[attr-defined]
    table.put_item.side_effect = missing
    with pytest.raises(StorageError) as exc_info:
        backend.store("key", b"value")
    assert exc_info.value.operation == "store"
    revision = "a" * 32
    table.scan.side_effect = [{"Items": [{"pk": "key", "_scrapy_revision": revision}]}]
    table.delete_item.return_value = {
        "Attributes": {"pk": "key", "_scrapy_revision": revision}
    }
    backend.clear_storage()
    assert table.delete_item.call_args.kwargs["ExpressionAttributeValues"] == {
        ":revision": revision
    }


def test_dynamodb_clear_preserves_legacy_and_rejects_malformed_delete(mocker) -> None:
    backend, table = _ddb_backend(mocker)
    table.scan.return_value = {"Items": [{"pk": "legacy"}]}
    with pytest.raises(StorageError, match="legacy"):
        backend.clear_storage()
    table.scan.return_value = {"Items": [{"pk": "key", "_scrapy_revision": "a" * 32}]}
    table.delete_item.return_value = {
        "Attributes": {"pk": "wrong", "_scrapy_revision": "a" * 32}
    }
    with pytest.raises(StorageError, match="malformed"):
        backend.clear_storage()


# ---------------------------------------------------------------------------
# Redis and Memcached: no-replay data paths, hostile response types, cleanup.
# ---------------------------------------------------------------------------


def test_redis_response_decode_and_tls_kwargs_are_explicit() -> None:
    backend = RedisBackend(
        RedisSettings(decode_responses=True, ssl_enabled=True, ssl_cafile="ca.pem")
    )
    snapshot, _password, _sentinel_password = backend._capture_connection_plan()
    kwargs = backend._base_client_kwargs(snapshot, None)
    assert kwargs["encoding_errors"] == "surrogateescape"
    assert kwargs["ssl"] is True
    assert kwargs["ssl_cert_reqs"] == "required"
    assert redis_mod._new_sentinel_control_retry(True)._retries == 1
    assert redis_mod._new_sentinel_control_retry(False)._retries == 0


def test_redis_pop_response_contract_rejects_ambiguous_shapes(mocker) -> None:
    backend, client = _redis_backend(mocker)
    script = mocker.MagicMock()
    client.register_script.return_value = script
    for result, message in [
        ([1], "Malformed"),
        ([1, object()], "invalid type"),
        ([3, "corrupt"], "structural"),
    ]:
        script.return_value = result
        with pytest.raises(QueueError, match=message):
            backend.pop("jobs")
    script.return_value = [1, "text"]
    assert backend.pop("jobs") == b"text"


def test_redis_storage_rejects_failed_write_and_invalid_read(mocker) -> None:
    backend, client = _redis_backend(mocker)
    client.set.return_value = False
    with pytest.raises(StorageError, match="rejected"):
        backend.store("key", b"value")
    client.set.return_value = True
    client.get.return_value = object()
    with pytest.raises(StorageError, match="invalid response"):
        backend.retrieve("key")
    client.get.return_value = "text"
    assert backend.retrieve("key") == b"text"
    client.ttl.return_value = -2
    assert backend.ttl("key") is None


def test_memcached_ttl_conversion_and_exact_response_contract(mocker) -> None:
    backend, client = _memcached_backend(mocker)
    client.set.return_value = True
    backend.store("short", b"x", ttl=5)
    client.set.assert_called_with("short", b"x", expire=5)
    mocker.patch.object(memcached_mod.time, "time", return_value=1000)
    backend.store(
        "long", b"x", ttl=memcached_mod._MEMCACHED_MAX_RELATIVE_TTL_SECONDS + 1
    )
    assert (
        client.set.call_args.kwargs["expire"]
        == 1000 + memcached_mod._MEMCACHED_MAX_RELATIVE_TTL_SECONDS + 1
    )
    client.get.return_value = bytearray(b"x")
    assert backend.retrieve("key") == b"x"
    client.delete.return_value = 1
    with pytest.raises(StorageError):
        backend.delete("key")


def test_memcached_clear_capabilities_are_static_and_fail_closed(mocker) -> None:
    backend, client = _memcached_backend(mocker)
    with pytest.raises(NotImplementedError) as prefix_error:
        backend.clear_storage(prefix="tenant:")
    assert "tenant:" not in str(prefix_error.value)
    with pytest.raises(NotImplementedError):
        backend.clear_storage()
    backend.disconnect()
    with pytest.raises(StorageError) as exc_info:
        backend.clear_storage()
    assert exc_info.value.operation == "clear_storage"
    backend, client = _memcached_backend(mocker, allow_flush_all=True)
    client.flush_all.return_value = False
    with pytest.raises(StorageError) as exc_info:
        backend.clear_storage()
    assert exc_info.value.operation == "clear_storage"
    client.flush_all.return_value = True
    client.flush_all.reset_mock()
    backend.clear_storage()
    client.flush_all.assert_called_once()


def test_memcached_operation_reentry_and_disconnect_callback_are_rejected(
    mocker,
) -> None:
    backend, client = _memcached_backend(mocker)
    with backend._operation("outer"):
        with pytest.raises(BackendConnectionError, match="re-entrantly"):
            with backend._operation("inner"):
                pass
    client.close.side_effect = backend.disconnect
    backend.disconnect()
    client.close.assert_called_once_with()
