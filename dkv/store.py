"""Thread-safe state machine applied only from committed Raft entries."""
from __future__ import annotations

from threading import RLock


class KeyValueStore:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._lock = RLock()

    def apply(self, command: dict[str, str]) -> None:
        with self._lock:
            if command["op"] == "set":
                self._values[command["key"]] = command["value"]
            elif command["op"] == "delete":
                self._values.pop(command["key"], None)
            else:
                raise ValueError(f"unknown state-machine operation: {command['op']}")

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._values.get(key)

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._values)
