from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_ledger_path(state_path: Union[Path, str]) -> Path:
    path = Path(state_path)
    return path.with_name("ledger.sqlite3")


class RequestLedger:
    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    request_hash TEXT NOT NULL,
                    client_conversation_id TEXT NOT NULL,
                    meta_conversation_id TEXT,
                    turn_index INTEGER,
                    phase TEXT NOT NULL,
                    response_json TEXT,
                    response_text TEXT,
                    response_hash TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_requests_idempotency_key ON requests(idempotency_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_requests_conversation_turn ON requests(client_conversation_id, turn_index)"
            )

    @staticmethod
    def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        return dict(row)

    def get_by_idempotency_key(self, idempotency_key: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._row_to_dict(row)

    def next_turn_index(self, client_conversation_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(turn_index) AS max_turn FROM requests WHERE client_conversation_id = ?",
                (client_conversation_id,),
            ).fetchone()
        current = row["max_turn"] if row and row["max_turn"] is not None else 0
        return int(current) + 1

    def get_active_for_conversation(
        self,
        client_conversation_id: str,
        *,
        exclude_idempotency_key: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        active_phases = ("bootstrap_turn_sent", "user_turn_sent", "streaming")
        query = """
            SELECT * FROM requests
            WHERE client_conversation_id = ?
              AND phase IN (?, ?, ?)
        """
        values: list[Any] = [client_conversation_id, *active_phases]
        if exclude_idempotency_key:
            query += " AND idempotency_key != ?"
            values.append(exclude_idempotency_key)
        query += " ORDER BY updated_at DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(query, values).fetchone()
        return self._row_to_dict(row)

    def create_pending(
        self,
        *,
        request_id: str,
        idempotency_key: str,
        request_hash: str,
        client_conversation_id: str,
        meta_conversation_id: Optional[str],
        turn_index: Optional[int] = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO requests (
                    id, idempotency_key, request_hash, client_conversation_id,
                    meta_conversation_id, turn_index, phase, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    idempotency_key,
                    request_hash,
                    client_conversation_id,
                    meta_conversation_id,
                    turn_index,
                    "pending",
                    now,
                    now,
                ),
            )
        record = self.get_by_idempotency_key(idempotency_key)
        assert record is not None
        return record

    def mark_phase(self, idempotency_key: str, phase: str, **updates: Any) -> None:
        allowed = {
            "client_conversation_id",
            "meta_conversation_id",
            "turn_index",
            "response_json",
            "response_text",
            "response_hash",
            "error_type",
            "error_message",
        }
        assignments = ["phase = ?", "updated_at = ?"]
        values: list[Any] = [phase, utc_now()]
        for key, value in updates.items():
            if key not in allowed:
                raise ValueError(f"unsupported ledger update: {key}")
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            assignments.append(f"{key} = ?")
            values.append(value)
        values.append(idempotency_key)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE requests SET {', '.join(assignments)} WHERE idempotency_key = ?",
                values,
            )

    def complete(self, idempotency_key: str, *, response_json: dict[str, Any], response_text: str, response_hash: str) -> None:
        self.mark_phase(
            idempotency_key,
            "completed",
            response_json=response_json,
            response_text=response_text,
            response_hash=response_hash,
            error_type=None,
            error_message=None,
        )

    def fail(self, idempotency_key: str, *, error_type: str, error_message: str, phase: str = "failed") -> None:
        self.mark_phase(idempotency_key, phase, error_type=error_type, error_message=error_message)
