import logging
import sqlite3
from pathlib import Path

from coordinator.state.stage import Stage


class StageStorage:
    """SQLite store for client EOF stage state."""

    TABLE_COLUMNS = "client_id, rule_id, stage_id, expected_input"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row

        self.create_tables()
        logging.debug("[StageStorage] Initialized | db_path=%s", db_path)

    def create_tables(self) -> None:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS stages (
                    client_id TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    expected_input INTEGER NOT NULL,
                    PRIMARY KEY (client_id, rule_id, stage_id)
                )
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_stages_rule_stage
                ON stages (rule_id, stage_id)
            """)

        logging.debug("[StageStorage] CREATE_TABLES | Success")

    def save(self, stage: Stage) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO stages (client_id, rule_id, stage_id, expected_input)
                VALUES (:client_id, :rule_id, :stage_id, :expected_input)
                ON CONFLICT(client_id, rule_id, stage_id) DO UPDATE SET
                    expected_input = excluded.expected_input
                """,
                stage.to_dict(),
            )

        logging.debug(
            "[StageStorage] SAVE | client_id=%s rule_id=%s stage_id=%s expected_input=%s",
            stage.client_id, stage.rule_id, stage.stage_id, stage.expected_input,
        )

    def get(self, client_id: str, rule_id: str, stage_id: str) -> Stage | None:
        row = self.connection.execute(
            f"""
            SELECT {self.TABLE_COLUMNS}
            FROM stages
            WHERE client_id = ? AND rule_id = ? AND stage_id = ?
            """,
            (client_id, rule_id, stage_id),
        ).fetchone()

        return self._row_to_stage(row)

    def find_by_client(self, client_id: str) -> dict[tuple[str, str], Stage]:
        rows = self.connection.execute(
            f"SELECT {self.TABLE_COLUMNS} FROM stages WHERE client_id = ?",
            (client_id,),
        ).fetchall()

        return {(stage.rule_id, stage.stage_id): stage for stage in self._rows_to_stages(rows).values()}

    def find_by_stage(self, rule_id: str, stage_id: str) -> dict[str, Stage]:
        rows = self.connection.execute(
            f"""
            SELECT {self.TABLE_COLUMNS}
            FROM stages
            WHERE rule_id = ? AND stage_id = ?
            """,
            (rule_id, stage_id),
        ).fetchall()

        return self._rows_to_stages(rows)

    def delete(self, client_id: str, rule_id: str, stage_id: str) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM stages WHERE client_id = ? AND rule_id = ? AND stage_id = ?",
                (client_id, rule_id, stage_id),
            )

        deleted = cursor.rowcount > 0
        logging.debug(
            "[StageStorage] DELETE | client_id=%s rule_id=%s stage_id=%s deleted=%s",
            client_id, rule_id, stage_id, deleted,
        )

        return deleted

    def close(self) -> None:
        self.connection.close()
        logging.debug("[StageStorage] CLOSE | Success")

    @staticmethod
    def _row_to_stage(row: sqlite3.Row | None) -> Stage | None:
        return None if row is None else Stage.from_dict(dict(row))

    @classmethod
    def _rows_to_stages(cls, rows: list[sqlite3.Row]) -> dict[str, Stage]:
        stages: dict[str, Stage] = {}

        for row in rows:
            stage = cls._row_to_stage(row)
            if stage is not None:
                stages[stage.client_id] = stage

        return stages