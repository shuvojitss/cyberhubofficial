"""
event_bus.py
============
Lightweight in-process async event bus.

Producers call ``emit(event_type, data)`` to broadcast events.
Consumers call ``subscribe(event_type)`` to receive an async generator.

Thread-safe: ``emit`` can be called from background (non-async) threads;
it uses ``call_soon_threadsafe`` to schedule delivery on the event loop.
"""

import asyncio
import logging
from collections import defaultdict
from typing import Any, AsyncGenerator, Dict, Optional, Set

logger = logging.getLogger("event_bus")


class EventBus:
    """Process-local pub/sub with asyncio queues."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind an event loop so that threaded emitters can schedule safely."""
        self._loop = loop

    async def emit(self, event_type: str, data: Any = None) -> None:
        """Emit from an async context."""
        payload = {"event": event_type, "data": data}
        dead: list[asyncio.Queue] = []
        for queue in self._subscribers.get(event_type, set()):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(queue)
        # Also emit to wildcard subscribers ("*")
        for queue in self._subscribers.get("*", set()):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(queue)
        # Remove overflowed queues (dead consumers)
        for queue in dead:
            self._remove_queue(queue)

    def emit_threadsafe(self, event_type: str, data: Any = None) -> None:
        """Emit from a synchronous / background-thread context."""
        if self._loop is None or self._loop.is_closed():
            logger.warning("EventBus.emit_threadsafe called but no event loop bound")
            return
        self._loop.call_soon_threadsafe(
            lambda: self._loop.create_task(self.emit(event_type, data))
        )

    async def subscribe(self, event_type: str = "*") -> AsyncGenerator[dict, None]:
        """Yield events of the given type (or ``*`` for all)."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subscribers[event_type].add(queue)
        try:
            while True:
                payload = await queue.get()
                yield payload
        finally:
            self._subscribers[event_type].discard(queue)

    def _remove_queue(self, queue: asyncio.Queue) -> None:
        for queues in self._subscribers.values():
            queues.discard(queue)


# Module-level singleton
bus = EventBus()
