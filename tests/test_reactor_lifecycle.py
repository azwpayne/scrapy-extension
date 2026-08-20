"""Regression tests for reactor-safe adaptation of synchronous backend calls."""

from __future__ import annotations

import subprocess
import sys
import textwrap


def test_slow_pipeline_store_does_not_stop_reactor_heartbeat() -> None:
    """A Deferred-capable pipeline write runs off-reactor and preserves its result."""
    script = textwrap.dedent(
        """
        import time
        from types import SimpleNamespace

        from scrapy import Item, Field
        from scrapy_extension.pipeline.pipeline import BackendPipeline
        from twisted.internet import reactor
        from twisted.internet.defer import Deferred

        class Stored(Item):
            value = Field()

        class SlowStorage:
            def store(self, key, data, ttl=None):
                del key, data, ttl
                time.sleep(0.15)

        class Manager:
            def __init__(self):
                self.backend = SlowStorage()

            def get_storage_backend(self):
                return self.backend

            def close(self):
                return None

        spider = SimpleNamespace(name="heartbeat", crawler=None)
        pipeline = BackendPipeline(Manager(), reactor_io_timeout=1.0)
        pipeline._storage_supported = True
        heartbeat = []
        result = []

        def tick():
            heartbeat.append(time.monotonic())
            if len(heartbeat) < 3:
                reactor.callLater(0.02, tick)

        def start():
            deferred = pipeline.process_item(Stored(value="ok"), spider)
            assert isinstance(deferred, Deferred)
            deferred.addCallbacks(
                lambda value: (result.append(value), reactor.stop()),
                lambda failure: (reactor.stop(), failure),
            )
            reactor.callLater(0.02, tick)

        reactor.callWhenRunning(start)
        reactor.run(installSignalHandlers=False)
        assert len(heartbeat) >= 3
        assert result and result[0]["value"] == "ok"
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
