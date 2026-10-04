"""Persistent state for a Raft node.

Per the paper (Figure 2), currentTerm, votedFor and the log must be persisted
to stable storage before the node responds to an RPC, so that a crash and
restart can never cause the node to forget a vote it already cast or an
entry it already acknowledged.
"""

from __future__ import annotations

import json
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from raft.types import LogEntry


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)


class Storage(ABC):
    @abstractmethod
    def load(self) -> PersistentState: ...

    @abstractmethod
    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None: ...

    @abstractmethod
    def append_entries(self, entries: List[LogEntry]) -> None: ...

    @abstractmethod
    def truncate_from(self, index: int) -> None:
        """Delete entry `index` and every entry after it (1-based)."""

    @abstractmethod
    def get_entry(self, index: int) -> Optional[LogEntry]: ...

    def get_entries_from(self, index: int) -> List[LogEntry]:
        state = self.load()
        return [e for e in state.log if e.index >= index]

    def last_index(self) -> int:
        state = self.load()
        return state.log[-1].index if state.log else 0

    def last_term(self) -> int:
        state = self.load()
        return state.log[-1].term if state.log else 0


class InMemoryStorage(Storage):
    """Used by tests and by the in-process Raft simulation harness."""

    def __init__(self) -> None:
        self._state = PersistentState()

    def load(self) -> PersistentState:
        return self._state

    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self._state.current_term = term
        self._state.voted_for = voted_for

    def append_entries(self, entries: List[LogEntry]) -> None:
        self._state.log.extend(entries)

    def truncate_from(self, index: int) -> None:
        self._state.log = [e for e in self._state.log if e.index < index]

    def get_entry(self, index: int) -> Optional[LogEntry]:
        for e in self._state.log:
            if e.index == index:
                return e
        return None


class FileStorage(Storage):
    """JSON-file backed storage, one file per node, for the Docker/HTTP deployment.

    Every mutation is written with a write-to-temp-file-then-rename so a crash
    mid-write can never leave a torn/partial file behind.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        if os.path.exists(path):
            with open(path, "r") as f:
                raw = json.load(f)
            self._state = PersistentState(
                current_term=raw.get("current_term", 0),
                voted_for=raw.get("voted_for"),
                log=[LogEntry.from_dict(e) for e in raw.get("log", [])],
            )
        else:
            self._state = PersistentState()
            self._flush()

    def _flush(self) -> None:
        raw = {
            "current_term": self._state.current_term,
            "voted_for": self._state.voted_for,
            "log": [e.to_dict() for e in self._state.log],
        }
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=directory)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(raw, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def load(self) -> PersistentState:
        return self._state

    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self._state.current_term = term
        self._state.voted_for = voted_for
        self._flush()

    def append_entries(self, entries: List[LogEntry]) -> None:
        self._state.log.extend(entries)
        self._flush()

    def truncate_from(self, index: int) -> None:
        self._state.log = [e for e in self._state.log if e.index < index]
        self._flush()

    def get_entry(self, index: int) -> Optional[LogEntry]:
        for e in self._state.log:
            if e.index == index:
                return e
        return None
