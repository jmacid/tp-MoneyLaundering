import logging
import sqlite3
from pathlib import Path

from coordinator.state.report import Report


class ReportStorage:
    """SQLite store for EOF reports sent by worker nodes."""

    TABLE_COLUMNS = "request_id, node_id, client_id, processed, emitted"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row

        self.create_tables()
        logging.debug("[ReportStorage] Initialized | db_path=%s", db_path)

    def create_tables(self) -> None:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    request_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    processed INTEGER NOT NULL,
                    emitted INTEGER NOT NULL,
                    PRIMARY KEY (request_id, node_id)
                )
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_reports_request
                ON reports (request_id)
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_reports_client
                ON reports (client_id)
            """)

        logging.debug("[ReportStorage] CREATE_TABLES | Success")

    def save(self, report: Report) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO reports (request_id, node_id, client_id, processed, emitted)
                VALUES (:request_id, :node_id, :client_id, :processed, :emitted)
                ON CONFLICT(request_id, node_id) DO UPDATE SET
                    client_id = excluded.client_id,
                    processed = excluded.processed,
                    emitted = excluded.emitted
                """,
                report.to_dict(),
            )

        logging.debug(
            "[ReportStorage] SAVE | request_id=%s node_id=%s client_id=%s processed=%s emitted=%s",
            report.request_id, report.node_id, report.client_id, report.processed, report.emitted,
        )

    def get(self, request_id: str, node_id: str) -> Report | None:
        row = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM reports WHERE request_id = ? AND node_id = ?",
            (request_id, node_id),
        ).fetchone()

        return self._row_to_report(row)

    def list_by_request(self, request_id: str) -> dict[str, Report]:
        rows = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM reports WHERE request_id = ?",
            (request_id,),
        ).fetchall()

        return self._rows_to_reports(rows)

    def list_by_client(self, client_id: str) -> dict[tuple[str, str], Report]:
        rows = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM reports WHERE client_id = ?",
            (client_id,),
        ).fetchall()

        reports: dict[tuple[str, str], Report] = {}

        for row in rows:
            report = Report.from_dict(dict(row))
            reports[(report.request_id, report.node_id)] = report

        return reports

    def sum_by_request(self, request_id: str) -> tuple[int, int]:
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(processed), 0) AS total_processed,
                   COALESCE(SUM(emitted), 0) AS total_emitted
            FROM reports
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()

        return int(row["total_processed"]), int(row["total_emitted"])

    def count_by_request(self, request_id: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS report_count FROM reports WHERE request_id = ?",
            (request_id,),
        ).fetchone()

        return int(row["report_count"])

    def delete(self, request_id: str, node_id: str) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM reports WHERE request_id = ? AND node_id = ?",
                (request_id, node_id),
            )

        deleted = cursor.rowcount > 0
        logging.debug(
            "[ReportStorage] DELETE | request_id=%s node_id=%s deleted=%s",
            request_id, node_id, deleted,
        )

        return deleted

    def delete_by_request(self, request_id: str) -> int:
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM reports WHERE request_id = ?",
                (request_id,),
            )

        deleted_count = cursor.rowcount
        logging.debug(
            "[ReportStorage] DELETE_BY_REQUEST | request_id=%s deleted_count=%s",
            request_id, deleted_count,
        )

        return deleted_count

    def close(self) -> None:
        self.connection.close()
        logging.debug("[ReportStorage] CLOSE | Success")

    @staticmethod
    def _row_to_report(row: sqlite3.Row | None) -> Report | None:
        return None if row is None else Report.from_dict(dict(row))

    @classmethod
    def _rows_to_reports(cls, rows: list[sqlite3.Row]) -> dict[str, Report]:
        reports: dict[str, Report] = {}

        for row in rows:
            report = cls._row_to_report(row)
            if report is not None:
                reports[report.node_id] = report

        return reports