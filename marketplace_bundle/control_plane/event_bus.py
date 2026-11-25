from __future__ import annotations
import asyncio
import json
from collections import deque
from typing import Any, AsyncGenerator, Dict, List


class LiveEventBus:
    """Decoupled async pub/sub bus for streaming agent handoffs and state to the Admin UI."""

    def __init__(self, max_history: int = 100):
        self._subscribers: List[asyncio.Queue[str]] = []
        self._history: deque[Dict[str, Any]] = deque(maxlen=max_history)

    async def publish(self, event_type: str, payload: Dict[str, Any]) -> None:
        envelope = {"type": event_type, "data": payload}
        self._history.append(envelope)
        serialized = json.dumps(envelope)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(serialized)
            except asyncio.QueueFull:
                pass

    def get_history(self) -> List[Dict[str, Any]]:
        return list(self._history)

    def clear_history(self) -> None:
        self._history.clear()

    async def subscribe(self) -> AsyncGenerator[str, None]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=200)
        self._subscribers.append(queue)
        try:
            # Replay recent events on initial connect so refresh never loses state
            for item in list(self._history):
                yield f"data: {json.dumps(item)}\n\n"
            while True:
                msg = await queue.get()
                yield f"data: {msg}\n\n"
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)


event_bus = LiveEventBus()