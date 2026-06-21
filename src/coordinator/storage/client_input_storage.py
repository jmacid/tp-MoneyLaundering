import sqlite3
from pathlib import Path

from coordinator.state.client_input import ClientInput

class ClientInputStorage:
    TABLE_COLUMNS = "client_id, expected_input"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self) -> None:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS client_inputs (
                    client_id TEXT PRIMARY KEY,
                    expected_input INTEGER NOT NULL
                )
            """)

    def save(self, client_input: ClientInput) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO client_inputs (client_id, expected_input)
                VALUES (?, ?)
                ON CONFLICT(client_id) DO UPDATE SET
                    expected_input = excluded.expected_input
                """,
                (client_input.client_id, client_input.expected_input),
            )

    def get_expected_input(self, client_id: str) -> int | None:
        row = self.connection.execute(
            "SELECT expected_input FROM client_inputs WHERE client_id = ?",
            (client_id,),
        ).fetchone()

        return None if row is None else int(row["expected_input"])