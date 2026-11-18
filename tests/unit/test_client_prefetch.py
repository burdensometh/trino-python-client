# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the optional bounded prefetch pipeline of spooled segments."""
import json
import threading
import time
from unittest import mock

import pytest

from trino.client import ClientSession
from trino.client import DecodableSegment
from trino.client import InlineSegment
from trino.client import PrefetchConfig
from trino.client import SegmentIterator
from trino.client import SpooledSegment
from trino.client import TrinoRequest
from trino.client import _SegmentPrefetcher
from trino.client import _SlotState


def _metadata(size: int) -> dict:
    return {"uncompressedSize": str(size), "segmentSize": str(size)}


class _ControlledSpooledSegment(SpooledSegment):
    """SpooledSegment whose download is scripted and can block on an event."""

    def __init__(self, name, rows, fail_times=0, gate=None, delay=0.0):
        payload = json.dumps(rows).encode("utf-8")
        super().__init__(
            {
                "type": "spooled",
                "uri": f"http://storage/{name}",
                "ackUri": f"http://storage/{name}/ack",
                "metadata": {
                    "uncompressedSize": str(len(payload)),
                    "segmentSize": str(len(payload)),
                },
            },
            request=None,
        )
        self._payload = payload
        self.fail_times = fail_times
        self.download_attempts = 0
        self.acknowledge_count = 0
        self._gate = gate
        self._delay = delay

    @property
    def data(self) -> bytes:
        self.download_attempts += 1
        if self._gate is not None:
            self._gate.wait(5.0)
        if self._delay:
            time.sleep(self._delay)
        if self.download_attempts <= self.fail_times:
            raise IOError(f"download failed (attempt {self.download_attempts})")
        return self._payload

    def acknowledge(self):
        self.acknowledge_count += 1


def _segment(name, rows, **kwargs):
    # The prefetcher sizes reservations from the DecodableSegment metadata, so
    # it must carry the real payload length (the inner segment has the same).
    payload = json.dumps(rows).encode("utf-8")
    metadata = {"uncompressedSize": str(len(payload)), "segmentSize": str(len(payload))}
    controlled = _ControlledSpooledSegment(name, rows, **kwargs)
    return DecodableSegment("json", metadata, controlled)


def _prefetch_iterator(segments, config, heartbeat_interval=None):
    request = None
    if heartbeat_interval is not None:
        request = TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(user="test"),
            http_scheme="http",
        )
    mapper = mock.Mock()
    mapper.map.side_effect = lambda rows: rows
    return SegmentIterator(
        segments,
        mapper,
        request=request,
        heartbeat_interval=heartbeat_interval,
        prefetch=config,
    )


class _IdentityMapper:
    def map(self, rows):
        return rows


def _drain_in_thread(iterator):
    """Consume an iterator in a daemon thread without leaking StopIteration."""
    def _run():
        try:
            while True:
                next(iterator)
        except StopIteration:
            pass

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


def test_prefetch_disabled_by_default_keeps_sequential_behavior():
    config = PrefetchConfig()
    assert config.enabled is False
    segments = [_segment(f"s{i}", [[i]]) for i in range(3)]
    iterator = SegmentIterator(segments, mapper=_IdentityMapper())
    assert iterator._prefetch_enabled() is False
    assert list(iterator) == [[0], [1], [2]]


def test_prefetch_delivers_rows_in_server_order():
    # Segments finish out of order (s0 is slowest); delivery must stay ordered.
    gate_s0 = threading.Event()
    segments = [
        _segment("s0", [[0]], gate=gate_s0, delay=0.0),
        _segment("s1", [[1]], delay=0.05),
        _segment("s2", [[2]], delay=0.02),
    ]
    config = PrefetchConfig(enabled=True, max_concurrency=3, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config)

    # Let s1/s2 finish first while s0 is blocked.
    time.sleep(0.15)
    gate_s0.set()

    rows = list(iterator)
    assert rows == [[0], [1], [2]]
    # Every segment was downloaded once.
    assert [seg.segment.download_attempts for seg in segments] == [1, 1, 1]


def test_prefetch_acknowledges_only_after_segment_consumed():
    segments = [_segment(f"s{i}", [[i]]) for i in range(3)]
    config = PrefetchConfig(enabled=True, max_concurrency=3, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config)

    # Consume the whole first segment; its ack is sent only when the iterator
    # advances to the next segment, never while later segments are still queued.
    assert next(iterator) == [0]
    assert segments[0].segment.acknowledge_count == 0

    # Give background workers time to buffer the remaining segments.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not all(
        seg.segment.download_attempts == 1 for seg in segments
    ):
        time.sleep(0.01)
    assert [seg.segment.download_attempts for seg in segments] == [1, 1, 1]
    # Even though every segment is downloaded, only consumed ones are acked.
    assert [seg.segment.acknowledge_count for seg in segments] == [0, 0, 0]

    # Fully draining acknowledges every segment, strictly in order.
    assert list(iterator) == [[1], [2]]
    assert [seg.segment.acknowledge_count for seg in segments] == [1, 1, 1]


def test_prefetch_failed_segment_is_retried_alone_while_later_segments_wait():
    failing = _segment("s0", [[0]], fail_times=1)
    s1 = _segment("s1", [[1]])
    s2 = _segment("s2", [[2]])
    config = PrefetchConfig(enabled=True, max_concurrency=3, max_bytes=10 ** 9)
    iterator = _prefetch_iterator([failing, s1, s2], config)

    with pytest.raises(IOError):
        next(iterator)
    # The failed segment was attempted once and not acknowledged.
    assert failing.segment.acknowledge_count == 0
    assert failing.segment.download_attempts == 1
    # Later segments were downloaded successfully and buffered.
    assert s1.segment.download_attempts == 1
    assert s2.segment.download_attempts == 1

    # Retry surfaces the failed segment's rows first, then the buffered ones.
    assert list(iterator) == [[0], [1], [2]]
    assert failing.segment.download_attempts == 2
    assert [seg.segment.acknowledge_count for seg in (failing, s1, s2)] == [1, 1, 1]


def test_prefetch_repeated_failure_keeps_retrying_same_segment():
    # Fails on the first two attempts, succeeds on the third.
    failing = _segment("s0", [[0]], fail_times=2)
    s1 = _segment("s1", [[1]])
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=10 ** 9)
    iterator = _prefetch_iterator([failing, s1], config)

    with pytest.raises(IOError):
        next(iterator)
    assert failing.segment.download_attempts == 1
    with pytest.raises(IOError):
        next(iterator)
    assert failing.segment.download_attempts == 2
    # The later segment never overtakes the failing head.
    assert s1.segment.acknowledge_count == 0
    assert list(iterator) == [[0], [1]]
    assert failing.segment.download_attempts == 3


def test_prefetch_respects_max_concurrency():
    active = 0
    max_active = 0
    lock = threading.Lock()
    entered = threading.Event()

    class _TrackingSegment(_ControlledSpooledSegment):
        @property
        def data(self):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
                entered.set()
            try:
                return super().data
            finally:
                with lock:
                    active -= 1

    segments = [
        DecodableSegment("json", _metadata(1), _TrackingSegment(f"s{i}", [[i]], delay=0.05))
        for i in range(6)
    ]
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config)
    entered.wait(2.0)
    # While the consumer is idle, at most max_concurrency downloads run.
    time.sleep(0.2)
    assert max_active <= 2
    assert list(iterator) == [[0], [1], [2], [3], [4], [5]]
    assert max_active == 2


def test_prefetch_byte_budget_back_pressures_scheduling():
    # Payloads are "[[i]]" (5 bytes); each running slot reserves 10 bytes
    # (downloaded + decoded) and each buffered done slot retains 5 (decoded).
    segments = [_segment(f"s{i}", [[i]]) for i in range(6)]
    config = PrefetchConfig(enabled=True, max_concurrency=6, max_bytes=20)
    prefetcher = _SegmentPrefetcher(
        iter(segments), mapper=_IdentityMapper(), config=config,
    )
    try:
        # Wait for the pipeline to stall against the budget (steady state:
        # the slot count and retained bytes stop changing).
        previous = None
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with prefetcher._condition:
                snapshot = (len(prefetcher._slots), prefetcher._used_bytes,
                            prefetcher._source_exhausted, prefetcher._in_flight)
            if snapshot == previous and snapshot[3] == 0:
                break
            previous = snapshot
            time.sleep(0.02)

        with prefetcher._condition:
            buffered = len(prefetcher._slots)
            used = prefetcher._used_bytes
        # The budget stopped the pipeline before all six segments were buffered.
        assert buffered < 6, (buffered, used)
        assert used <= config.max_bytes, (buffered, used)
        assert prefetcher._source_exhausted is False
    finally:
        prefetcher.close()

    # Releasing the pressure (a fresh pipeline, fully consumed) delivers all
    # segments in server-side order.
    fresh = [_segment(f"s{i}", [[i]]) for i in range(6)]
    iterator = _prefetch_iterator(fresh, PrefetchConfig(enabled=True, max_concurrency=6, max_bytes=20))
    assert list(iterator) == [[0], [1], [2], [3], [4], [5]]


def test_prefetch_oversized_segment_does_not_deadlock():
    huge = _segment("huge", [[0]])
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=1)
    iterator = _prefetch_iterator([huge], config)
    assert list(iterator) == [[0]]


def test_prefetch_close_cancels_inflight_and_buffered_downloads():
    gate = threading.Event()
    segments = [
        _segment("s0", [[0]], gate=gate),
        _segment("s1", [[1]], gate=gate),
    ]
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config, heartbeat_interval=30.0)

    # Trigger the lazy pipeline from a consumer thread; the first next() blocks
    # because both downloads are held behind the gate.
    consumer = _drain_in_thread(iterator)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and sum(
        seg.segment.download_attempts for seg in segments
    ) < 2:
        time.sleep(0.01)

    prefetcher = iterator._prefetcher
    assert prefetcher is not None
    heartbeat = prefetcher._heartbeat
    assert heartbeat is not None
    assert not heartbeat._stop_event.is_set()

    # Open the gate so the in-flight downloads can finish and the workers can be
    # joined; no further segment is scheduled once close() flips the closed flag.
    gate.set()
    iterator.close()
    consumer.join(timeout=2.0)

    # Heartbeat stopped, pipeline wound down and no acknowledgement was sent.
    assert heartbeat._stop_event.is_set()
    assert prefetcher._closed is True
    assert all(seg.segment.acknowledge_count == 0 for seg in segments)
    # Further iteration terminates instead of hanging.
    with pytest.raises(StopIteration):
        next(iterator)


def test_prefetch_close_is_idempotent():
    segments = [_segment("s0", [[0]])]
    config = PrefetchConfig(enabled=True)
    iterator = _prefetch_iterator(segments, config)
    iterator.close()
    iterator.close()


def test_prefetch_heartbeat_runs_for_pipeline_lifetime():
    segments = [_segment(f"s{i}", [[i]]) for i in range(2)]
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config, heartbeat_interval=0.01)
    assert iterator._prefetcher is None  # lazily started
    list(iterator)
    # The pipeline tore itself down after draining; heartbeat must be stopped.
    assert iterator._prefetcher is None


def test_prefetch_without_request_runs_without_heartbeat():
    # Heartbeats are optional: prefetch still works when no request is given.
    segments = [_segment("s0", [[0]])]
    iterator = SegmentIterator(
        segments,
        mapper=_IdentityMapper(),
        prefetch=PrefetchConfig(enabled=True),
    )
    assert list(iterator) == [[0]]
    assert iterator._prefetcher is None  # torn down on drain


def test_prefetch_request_requires_matching_heartbeat_interval():
    # The existing request/interval pairing rule still applies.
    segments = [_segment("s0", [[0]])]
    with pytest.raises(ValueError):
        SegmentIterator(
            segments,
            mapper=_IdentityMapper(),
            request=TrinoRequest(
                host="coordinator",
                port=8080,
                client_session=ClientSession(user="test"),
                http_scheme="http",
            ),
            heartbeat_interval=None,
            prefetch=PrefetchConfig(enabled=True),
        )


def test_prefetch_inline_segments_need_no_acknowledgement():
    inline = DecodableSegment(
        "json",
        None,
        InlineSegment(
            {
                "type": "inline",
                "metadata": _metadata(5),
                "data": "W1sxXV0=",  # [[1]]
            }
        ),
    )
    config = PrefetchConfig(enabled=True, max_concurrency=1, max_bytes=10 ** 9)
    iterator = _prefetch_iterator([inline], config)
    assert list(iterator) == [[1]]
    assert iterator._finished is True


def test_prefetch_drained_iterator_releases_pipeline():
    segments = [_segment("s0", [[0]])]
    config = PrefetchConfig(enabled=True)
    iterator = _prefetch_iterator(segments, config)
    list(iterator)
    assert iterator._closed is True
    assert iterator._prefetcher is None
    with pytest.raises(StopIteration):
        next(iterator)


def test_prefetch_invalid_config_rejected():
    with pytest.raises(ValueError):
        PrefetchConfig(enabled=True, max_concurrency=0)
    with pytest.raises(ValueError):
        PrefetchConfig(enabled=True, max_bytes=0)


def test_prefetch_query_cancel_closes_pipeline():
    # TrinoQuery.cancel() delegates pipeline reclamation to SegmentIterator.close().
    gate = threading.Event()
    segments = [_segment("s0", [[0]], gate=gate), _segment("s1", [[1]], gate=gate)]
    config = PrefetchConfig(enabled=True, max_concurrency=2, max_bytes=10 ** 9)
    iterator = _prefetch_iterator(segments, config)

    consumer = _drain_in_thread(iterator)

    # Wait until both downloads are blocked on the gate.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and sum(
        seg.segment.download_attempts for seg in segments
    ) < 2:
        time.sleep(0.01)
    assert iterator._prefetcher is not None

    gate.set()
    iterator.close()
    consumer.join(timeout=2.0)
    assert all(seg.segment.acknowledge_count == 0 for seg in segments)
    with pytest.raises(StopIteration):
        next(iterator)


def test_prefetcher_direct_take_returns_none_when_exhausted():
    config = PrefetchConfig(enabled=True, max_concurrency=1, max_bytes=10 ** 9)
    prefetcher = _SegmentPrefetcher(
        iter([_segment("s0", [[0]])]), mapper=_IdentityMapper(), config=config,
    )
    slot = prefetcher.take()
    assert slot.state == _SlotState.DONE
    assert list(slot.rows) == [[0]]
    assert prefetcher.take() is None
    assert prefetcher._closed is True
