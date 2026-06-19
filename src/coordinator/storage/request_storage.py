import json
import logging
import sqlite3
from pathlib import Path

from coordinator.state.request import Request


class RequestStorage:
    """SQLite store for EOF coordination requests."""

    TABLE_COLUMNS = """
        request_id, client_id, rule_id, stage_id, expected_input,
        expected_nodes, status, retry_count, last_retry_at, created_at
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row

        self.create_tables()
        logging.debug("[RequestStorage] Initialized | db_path=%s", db_path)

    def create_tables(self) -> None:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    expected_input INTEGER NOT NULL,
                    expected_nodes TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    last_retry_at REAL NOT NULL,
                    created_at REAL NOT NULL
                )
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_requests_client_rule_stage
                ON requests (client_id, rule_id, stage_id)
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_requests_status
                ON requests (status)
            """)

        logging.debug("[RequestStorage] CREATE_TABLES | Success")

    def save(self, request: Request) -> None:
        data = self._request_to_row_data(request)

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO requests (
                    request_id, client_id, rule_id, stage_id, expected_input,
                    expected_nodes, status, retry_count, last_retry_at, created_at
                )
                VALUES (
                    :request_id, :client_id, :rule_id, :stage_id, :expected_input,
                    :expected_nodes, :status, :retry_count, :last_retry_at, :created_at
                )
                ON CONFLICT(request_id) DO UPDATE SET
                    client_id = excluded.client_id,
                    rule_id = excluded.rule_id,
                    stage_id = excluded.stage_id,
                    expected_input = excluded.expected_input,
                    expected_nodes = excluded.expected_nodes,
                    status = excluded.status,
                    retry_count = excluded.retry_count,
                    last_retry_at = excluded.last_retry_at,
                    created_at = excluded.created_at
                """,
                data,
            )

        logging.debug(
            "[RequestStorage] SAVE | request_id=%s client_id=%s rule_id=%s stage_id=%s status=%s",
            request.request_id, request.client_id, request.rule_id, request.stage_id, request.status,
        )

    def get(self, request_id: str) -> Request | None:
        row = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()

        return self._row_to_request(row)

    def find_open_by_stage(self, client_id: str, rule_id: str, stage_id: str) -> Request | None:
        row = self.connection.execute(
            f"""
            SELECT {self.TABLE_COLUMNS}
            FROM requests
            WHERE client_id = ? AND rule_id = ? AND stage_id = ? AND status = 'WAITING'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (client_id, rule_id, stage_id),
        ).fetchone()

        return self._row_to_request(row)

    def list_by_status(self, status: str) -> dict[str, Request]:
        rows = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM requests WHERE status = ?",
            (status,),
        ).fetchall()

        return self._rows_to_requests(rows)

    def list_waiting(self) -> dict[str, Request]:
        return self.list_by_status("WAITING")

    def update_status(self, request_id: str, status: str) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE requests SET status = ? WHERE request_id = ?",
                (status, request_id),
            )

        updated = cursor.rowcount > 0
        logging.debug(
            "[RequestStorage] UPDATE_STATUS | request_id=%s status=%s updated=%s",
            request_id, status, updated,
        )

        return updated

    def mark_retry(self, request_id: str) -> Request | None:
        request = self.get(request_id)

        if request is None:
            logging.warning("[RequestStorage] MARK_RETRY_IGNORED | request_id=%s reason=not_found", request_id)
            return None

        request.mark_retry()
        self.save(request)
        return request

    def add_report(self, request_id: str, node_id: str, processed: int, emitted: int) -> Request | None:
        request = self.get(request_id)

        if request is None:
            logging.warning("[RequestStorage] ADD_REPORT_IGNORED | request_id=%s node_id=%s reason=not_found", request_id, node_id)
            return None

        request.add_report(node_id=node_id, processed=processed, emitted=emitted)
        self.save(request)
        return request

    def delete(self, request_id: str) -> bool:
        with self.connection:
            cursor = self.connection.execute("DELETE FROM requests WHERE request_id = ?", (request_id,))

        deleted = cursor.rowcount > 0
        logging.debug("[RequestStorage] DELETE | request_id=%s deleted=%s", request_id, deleted)

        return deleted

    def list_by_status(self, status: str) -> dict[str, Request]:
        rows = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM requests WHERE status = ?",
            (status,),
        ).fetchall()

        return self._rows_to_requests(rows)

    def list_waiting(self) -> dict[str, Request]:
        return self.list_by_status("WAITING")

    def close(self) -> None:
        self.connection.close()
        logging.debug("[RequestStorage] CLOSE | Success")

    @staticmethod
    def _request_to_row_data(request: Request) -> dict[str, object]:
        return {
            "request_id": request.request_id,
            "client_id": request.client_id,
            "rule_id": request.rule_id,
            "stage_id": request.stage_id,
            "expected_input": request.expected_input,
            "expected_nodes": json.dumps(sorted(request.expected_nodes)),
            "status": request.status,
            "retry_count": request.retry_count,
            "last_retry_at": request.last_retry_at,
            "created_at": request.created_at,
        }

    @staticmethod
    def _row_to_request(row: sqlite3.Row | None) -> Request | None:
        if row is None:
            return None

        data = dict(row)
        data["expected_nodes"] = json.loads(data["expected_nodes"])

        return Request.from_dict(data)

    @classmethod
    def _rows_to_requests(cls, rows: list[sqlite3.Row]) -> dict[str, Request]:
        requests: dict[str, Request] = {}

        for row in rows:
            request = cls._row_to_request(row)
            if request is not None:
                requests[request.request_id] = request

        return requests