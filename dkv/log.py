"""Crash-safe append-only Raft log with one fsynced JSON record per line."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: dict[str, str]


class DurableLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.entries: list[LogEntry] = []
        if self.path.exists():
            for line_number, line in enumerate(self.path.read_text().splitlines(), 1):
                record = json.loads(line)
                payload = json.dumps(record["entry"], sort_keys=True, separators=(",", ":"))
                if hashlib.sha256(payload.encode()).hexdigest() != record["checksum"]:
                    raise ValueError(f"corrupt log record at line {line_number}")
                self.entries.append(LogEntry(**record["entry"]))

    def append(self, entry: LogEntry) -> None:
        record_entry = asdict(entry)
        payload = json.dumps(record_entry, sort_keys=True, separators=(",", ":"))
        record = {"entry": record_entry, "checksum": hashlib.sha256(payload.encode()).hexdigest()}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self.entries.append(entry)

    def truncate_and_append(self, index: int, entries: list[LogEntry]) -> None:
        """Retain entries through 1-based index, then append and atomically replace."""
        replacement = self.entries[:index] + entries
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            for entry in replacement:
                raw = asdict(entry)
                payload = json.dumps(raw, sort_keys=True, separators=(",", ":"))
                handle.write(json.dumps({"entry": raw, "checksum": hashlib.sha256(payload.encode()).hexdigest()}, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(self.path)
        self.entries = replacement

    def term_at(self, index: int) -> int:
        return 0 if index == 0 else self.entries[index - 1].term
