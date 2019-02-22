from abc import ABC, abstractmethod
from math import inf

from hypothesis import strategies as st
from hypothesis.strategies import composite
from typing import NamedTuple, Any, List, Optional, Set, Union, Iterable, Callable, Tuple

from sortedcontainers import SortedListWithKey

ErrorEvent = NamedTuple("ErrorEvent", [("reason", str)])
AnyEvent = Union[ErrorEvent, Any]

SimEvent = NamedTuple("SimEvent", [("timestamp", int), ("payload", AnyEvent)])


@composite
def sim_events(draw, payload, min_size=0, max_size=20, min_interval=1, max_interval=100, start_ts=0):
    st_intervals = st.integers(min_value=min_interval, max_value=max_interval)
    st_event = st.tuples(st_intervals, payload)
    events = draw(st.lists(elements=st_event, min_size=min_size, max_size=max_size))

    ts = start_ts
    result = []
    for ev in events:
        ts += ev[0]
        result.append(SimEvent(timestamp=ts, payload=ev[1]))
    return result


class SimEventStream(ABC):
    @abstractmethod
    def peek(self) -> Optional[SimEvent]:
        pass

    @abstractmethod
    def pop(self) -> Optional[SimEvent]:
        pass


class ListEventStream(SimEventStream):
    def __init__(self, events: Iterable[SimEvent] = ()):
        self._events = SortedListWithKey(iterable=events, key=lambda ev: ev.timestamp)

    def add(self, event):
        self._events.add(event)

    def extend(self, events):
        self._events.update(events)

    def remove_all(self, predicate: Callable):
        indexes = [i for i, ev in enumerate(self._events) if predicate(ev)]
        for i in reversed(indexes):
            del self._events[i]

    @property
    def events(self):
        return self._events[:]

    def peek(self) -> Optional[SimEvent]:
        if len(self._events) > 0:
            return self._events[0]

    def pop(self) -> Optional[SimEvent]:
        if len(self._events) > 0:
            return self._events.pop(0)


class CompositeEventStream(SimEventStream):
    def __init__(self, *args):
        self._streams = [s for s in args]

    def add_stream(self, stream):
        self._streams.append(stream)

    def peek(self) -> Optional[SimEvent]:
        return self._next_stream_event()[1]

    def pop(self) -> Optional[SimEvent]:
        stream, _ = self._next_stream_event()
        if stream is not None:
            return stream.pop()

    def _next_stream_event(self) -> Tuple[SimEventStream, SimEvent]:
        stream = None
        event = None
        for s in self._streams:
            ev = s.peek()
            if ev is None:
                continue
            if event is None or ev.timestamp < event.timestamp:
                stream = s
                event = ev
        return stream, event
