"""Error-path coverage for SqsBackend (≥98% coverage goal)."""

from __future__ import annotations

import boto3
import pytest

import scrapy_extension.backends.sqs as sqs_mod
from scrapy_extension.backends.sqs import SqsBackend, _physical_queue_name
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.settings import SqsQueueNameGeneration, SqsSettings


class _SqsClientError(Exception):
    """Minimal boto3 ClientError-shaped exception for queue lookup tests."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _patch_client(mocker, client):
    """Patch one private SQS Session and guard the shared default alias."""

    def list_queue_tags(*, QueueUrl):  # noqa: N803 - boto3 keyword
        del QueueUrl
        physical_name = client.get_queue_url.call_args.kwargs["QueueName"]
        digest = physical_name.removeprefix(sqs_mod._V2_QUEUE_NAME_PREFIX)
        return {
            "Tags": {
                sqs_mod._V2_QUEUE_OWNER_TAG_KEY: (
                    f"{sqs_mod._V2_QUEUE_OWNER_PREFIX}{digest}"
                )
            }
        }

    client.list_queue_tags.side_effect = list_queue_tags
    session = mocker.MagicMock(name="private-sqs-session")
    session.client.return_value = client
    session_factory = mocker.patch.object(
        boto3.session, "Session", return_value=session
    )
    mocker.patch.object(
        boto3,
        "client",
        side_effect=AssertionError("shared default Session must not be used"),
    )
    return session_factory, session.client


def _connected(mocker, **overrides):
    b = SqsBackend(SqsSettings(**overrides))
    client = mocker.MagicMock()
    client.get_queue_url.return_value = {"QueueUrl": "https://sqs/test"}
    _patch_client(mocker, client)
    b.connect()
    return b, client


class TestSqsErrorPaths:
    def test_connect_with_credentials(self, mocker) -> None:
        b, _ = _connected(mocker, aws_access_key_id="k", aws_secret_access_key="s")
        assert b.is_connected()

    def test_connect_rejects_unsupported_mode_as_configuration_error(
        self, mocker
    ) -> None:
        b = SqsBackend(SqsSettings())
        b.config.mode = "unsupported"  # type: ignore[assignment]
        session_factory, _client_factory = _patch_client(mocker, mocker.MagicMock())

        with pytest.raises(ConfigurationError) as exc_info:
            b.connect()

        assert exc_info.value.setting_name == "mode"
        session_factory.assert_not_called()

    def test_queue_url_create_fallback(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get_queue_url.side_effect = _SqsClientError("QueueDoesNotExist")
        client.create_queue.return_value = {"QueueUrl": "https://sqs/new"}
        # Only a genuine missing-queue response permits a create side effect.
        b.push("qnew", b"x")
        physical_name = _physical_queue_name(
            "scrapy-", "qnew", SqsQueueNameGeneration.V2
        )
        client.create_queue.assert_called_once_with(
            QueueName=physical_name,
            tags={
                sqs_mod._V2_QUEUE_OWNER_TAG_KEY: (
                    f"{sqs_mod._V2_QUEUE_OWNER_PREFIX}"
                    f"{physical_name.removeprefix(sqs_mod._V2_QUEUE_NAME_PREFIX)}"
                )
            },
        )
        client.list_queue_tags.assert_called_once_with(QueueUrl="https://sqs/new")

    def test_queue_url_create_response_loss_resolves_and_validates_race_winner(
        self, mocker
    ) -> None:
        b, client = _connected(mocker)
        client.get_queue_url.side_effect = [
            _SqsClientError("QueueDoesNotExist"),
            {"QueueUrl": "https://sqs/race-winner"},
        ]
        client.create_queue.side_effect = RuntimeError("response lost")

        b.push("qnew", b"x")

        assert client.get_queue_url.call_count == 2
        client.list_queue_tags.assert_called_once_with(
            QueueUrl="https://sqs/race-winner"
        )
        client.send_message.assert_called_once()

    def test_queue_url_operational_failure_does_not_create(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get_queue_url.side_effect = _SqsClientError("AccessDenied")

        with pytest.raises(QueueError) as exc_info:
            b.push("qbad", b"x")

        assert exc_info.value.operation == "push"
        client.create_queue.assert_not_called()

    def test_queue_url_total_failure_raises(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get_queue_url.side_effect = _SqsClientError("QueueDoesNotExist")
        client.create_queue.side_effect = RuntimeError("create fail")
        with pytest.raises(QueueError):
            b.push("qbad", b"x")

    def test_missing_receipt_handle_is_malformed_not_empty(self, mocker) -> None:
        b, client = _connected(mocker)
        client.receive_message.return_value = {"Messages": [{"Body": "eA=="}]}

        with pytest.raises(QueueError) as exc_info:
            b.pop("q")

        assert exc_info.value.operation == "pop"
        assert "ReceiptHandle" in str(exc_info.value)

    def test_fractional_timeout_rounds_up(self, mocker) -> None:
        b, client = _connected(mocker)
        client.receive_message.return_value = {}

        assert b.pop("q", timeout=0.1) is None

        assert client.receive_message.call_args.kwargs["WaitTimeSeconds"] == 1

    @pytest.mark.parametrize("method", ["push", "pop", "queue_len", "clear_queue"])
    def test_invalid_queue_name_preserves_value_error(self, mocker, method) -> None:
        b, _ = _connected(mocker)
        args = ("bad name!", b"x") if method == "push" else ("bad name!",)

        with pytest.raises(ValueError):
            getattr(b, method)(*args)

    def test_clear_queue_url_failure_reports_clear_operation(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get_queue_url.side_effect = _SqsClientError("AccessDenied")

        with pytest.raises(QueueError) as exc_info:
            b.clear_queue("q")

        assert exc_info.value.operation == "clear_queue"
        client.create_queue.assert_not_called()

    def test_pop_failure_raises_queue_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.receive_message.side_effect = RuntimeError("recv fail")
        with pytest.raises(QueueError):
            b.pop("q")

    def test_ack_failure_raises_queue_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.receive_message.return_value = {
            "Messages": [{"Body": "eA==", "ReceiptHandle": "rh"}]
        }
        client.delete_message.side_effect = [RuntimeError("del fail"), None]
        b.pop("q")
        with pytest.raises(QueueError) as exc_info:
            b.ack("q")
        assert exc_info.value.operation == "ack"
        assert b._last_receipt == ("https://sqs/test", "rh")

        b.ack("q")

        assert client.delete_message.call_count == 2
        assert b._last_receipt is None

    def test_clear_raises_on_failure(self, mocker) -> None:
        """R-clearq: clear_queue raises QueueError on purge failure (not swallow)."""
        b, client = _connected(mocker)
        client.purge_queue.side_effect = RuntimeError("purge fail")
        mocker.patch("scrapy_extension.backends.sqs.time.sleep")
        with pytest.raises(QueueError) as exc_info:
            b.clear_queue("q")
        assert exc_info.value.operation == "clear_queue"

    def test_disconnect_and_ping(self, mocker) -> None:
        b, _ = _connected(mocker)
        assert b.ping() is True
        b.disconnect()
        assert b.is_connected() is False

    def test_push_invalid_name_raises(self, mocker) -> None:
        b, _ = _connected(mocker)
        with pytest.raises(ValueError):
            b.push("bad name!", b"x")
