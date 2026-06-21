from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

from coordinator.state.node import Node

class NodeStorage:
    """SQLite store for coordinator worker nodes."""

    TABLE_COLUMNS = """
        node_id, rule_id, stage_id, next_stage_id, control_queue, status, last_seen
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row

        self.create_tables()
        logging.debug("[NodeStorage] Initialized | db_path=%s", db_path)

    def create_tables(self) -> None:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    next_stage_id TEXT,
                    control_queue TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_seen REAL NOT NULL
                )
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_nodes_rule_stage_status
                ON nodes (rule_id, stage_id, status)
            """)

        logging.debug("[NodeStorage] CREATE_TABLES | Success")

    def save(self, node: Node) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO nodes (
                    node_id, rule_id, stage_id, next_stage_id,
                    control_queue, status, last_seen
                )
                VALUES (
                    :node_id, :rule_id, :stage_id, :next_stage_id,
                    :control_queue, :status, :last_seen
                )
                ON CONFLICT(node_id) DO UPDATE SET
                    rule_id = excluded.rule_id,
                    stage_id = excluded.stage_id,
                    next_stage_id = excluded.next_stage_id,
                    control_queue = excluded.control_queue,
                    status = excluded.status,
                    last_seen = excluded.last_seen
                """,
                node.to_dict(),
            )

        logging.debug(
            "[NodeStorage] SAVE | node_id=%s rule_id=%s stage_id=%s status=%s",
            node.node_id, node.rule_id, node.stage_id, node.status,
        )

    def get(self, node_id: str) -> Node | None:
        row = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM nodes WHERE node_id = ?",
            (node_id,),
        ).fetchone()

        return self._row_to_node(row)

    def touch(self, node_id: str) -> None:
        now = time.time()

        with self.connection:
            self.connection.execute(
                """
                UPDATE nodes
                SET last_seen = ?, status = 'ACTIVE'
                WHERE node_id = ?
                """,
                (now, node_id),
            )

        logging.debug("[NodeStorage] TOUCH | node_id=%s", node_id)

    def stop(self, node_id: str) -> None:
        now = time.time()

        with self.connection:
            self.connection.execute(
                """
                UPDATE nodes
                SET last_seen = ?, status = 'STOPPED'
                WHERE node_id = ?
                """,
                (now, node_id),
            )

        logging.debug("[NodeStorage] TOUCH | node_id=%s", node_id)

    def update_status(self, node_id: str, status: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE nodes SET status = ? WHERE node_id = ?",
                (status, node_id),
            )

        logging.debug("[NodeStorage] UPDATE_STATUS | node_id=%s status=%s", node_id, status)

    def find_active_by_stage(self, rule_id: str, stage_id: str) -> set[str]:
        rows = self.connection.execute(
            """
            SELECT node_id
            FROM nodes
            WHERE rule_id = ? AND stage_id = ? AND status = 'ACTIVE'
            """,
            (rule_id, stage_id),
        ).fetchall()

        return {row["node_id"] for row in rows}

    def find_by_stage(
        self,
        rule_id: str,
        stage_id: str,
        status: str | None = None,
    ) -> dict[str, Node]:
        query = f"SELECT {self.TABLE_COLUMNS} FROM nodes WHERE rule_id = ? AND stage_id = ?"
        params: list[object] = [rule_id, stage_id]

        if status is not None:
            query += " AND status = ?"
            params.append(status)

        rows = self.connection.execute(query, params).fetchall()
        return self._rows_to_nodes(rows)

    def find_stale_active_nodes(self, older_than: float) -> dict[str, Node]:
        rows = self.connection.execute(
            f"""
            SELECT {self.TABLE_COLUMNS}
            FROM nodes
            WHERE status = 'ACTIVE' AND last_seen < ?
            """,
            (older_than,),
        ).fetchall()

        return self._rows_to_nodes(rows)

    def get_next_stage_id(self, rule_id: str, stage_id: str) -> str | None:
        row = self.connection.execute(
            """
            SELECT next_stage_id
            FROM nodes
            WHERE rule_id = ? AND stage_id = ? AND next_stage_id IS NOT NULL
            LIMIT 1
            """,
            (rule_id, stage_id),
        ).fetchone()

        return None if row is None else row["next_stage_id"]

    def delete(self, node_id: str) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))

        logging.debug("[NodeStorage] DELETE | node_id=%s", node_id)

    def close(self) -> None:
        self.connection.close()
        logging.debug("[NodeStorage] CLOSE | Success")

    @staticmethod
    def _row_to_node(row: sqlite3.Row | None) -> Node | None:
        return None if row is None else Node.from_dict(dict(row))

    @classmethod
    def _rows_to_nodes(cls, rows: list[sqlite3.Row]) -> dict[str, Node]:
        nodes = {}

        for row in rows:
            node = cls._row_to_node(row)
            if node is not None:
                nodes[node.node_id] = node

        return nodes