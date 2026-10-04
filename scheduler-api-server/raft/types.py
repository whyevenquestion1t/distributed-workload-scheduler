"""Wire types for the Raft consensus protocol (Ongaro & Ousterhout, 2014)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    index: int
    term: int
    command: Any

    def to_dict(self) -> dict:
        return {"index": self.index, "term": self.term, "command": self.command}

    @staticmethod
    def from_dict(data: dict) -> "LogEntry":
        return LogEntry(index=data["index"], term=data["term"], command=data["command"])


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResult:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResult:
    term: int
    success: bool
    # Index of the first slot the follower has for the conflicting term, used
    # by the leader to back up next_index by more than one entry per round.
    conflict_index: Optional[int] = None
    conflict_term: Optional[int] = None


class NotLeaderError(Exception):
    """Raised by propose() when the local node is not (or no longer) the leader."""

    def __init__(self, leader_hint: Optional[str]):
        self.leader_hint = leader_hint
        super().__init__(f"not leader; leader hint={leader_hint!r}")
