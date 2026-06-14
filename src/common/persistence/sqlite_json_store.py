import json
import logging
import sqlite3
from pathlib import Path
from typing import Any


class SQLiteJsonStore:
    """Generic JSON key-value store backed by SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.create_tables()
        
        logging.debug("[SQLiteJsonStore] Initialized successfully")

    def create_tables(self) -> None:

        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    namespace TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    PRIMARY KEY (namespace, key)
                )
                """)
        logging.debug("[SQLiteJsonStore] CREATE_TABLES | Success")

    def save(self, namespace: str, key: str, value: dict[str, Any]) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO state (
                    namespace,
                    key,
                    value_json
                )
                VALUES (?, ?, ?)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value_json = excluded.value_json
                """,
                (namespace, key, json.dumps(value)),
            )
        logging.debug("[SQLiteJsonStore] SAVE | namespace=%s key=%s size_bytes=%s", namespace, key, len(value))

    def get(self, namespace: str, key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT value_json
            FROM state
            WHERE namespace = ? AND key = ?
            """,
            (namespace, key),
        ).fetchone()

        if not row:
            return None

        value = json.loads(row["value_json"])
        logging.debug("[SQLiteJsonStore] GET | namespace=%s key=%s size_bytes=%s", namespace, key, len(value))

        return value

    def delete(self, namespace: str, key: str) -> None:
        with self.connection:
            self.connection.execute(
                """
                DELETE FROM state
                WHERE namespace = ? AND key = ?
                """,
                (namespace, key),
            )

        logging.debug("[SQLiteJsonStore] DELETE | namespace=%s key=%s", namespace, key)

    def list(self, namespace: str) -> dict[str, dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT key, value_json
            FROM state
            WHERE namespace = ?
            """,
            (namespace,),
        ).fetchall()

        logging.debug("[SQLiteJsonStore] LIST | namespace=%s rows=%s", namespace, len(rows))

        return {row["key"]: json.loads(row["value_json"]) for row in rows}
