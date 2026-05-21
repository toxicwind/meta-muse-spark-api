from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator


class ConversationLockRegistry:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def get(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[None]:
        lock = self.get(key)
        await asyncio.to_thread(lock.acquire)
        try:
            yield
        finally:
            lock.release()
