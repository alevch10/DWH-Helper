import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from uuid import UUID

class UUIDEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles UUID objects."""
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)

DB_PATH = Path("./dwh.db")


class DWHStorage:
    def __init__(self, db_path: Path | str = DB_PATH):
        self.db_path = str(db_path)
        self._ensure_tables()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _ensure_tables(self):
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tables (
                    name TEXT PRIMARY KEY,
                    spec TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS table_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    pk_value TEXT,
                    row_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def save_table(self, name: str, spec: Dict[str, Any]) -> None:
        spec_json = json.dumps(spec, ensure_ascii=False, cls=UUIDEncoder)
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO tables(name, spec) VALUES (?, ?)",
                (name, spec_json),
            )

    def list_tables(self) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            cur = conn.execute("SELECT name, spec FROM tables")
            rows = cur.fetchall()
        result = []
        for name, spec_json in rows:
            try:
                spec = json.loads(spec_json)
            except Exception:
                spec = {}
            result.append({"name": name, "spec": spec})
        return result

    def get_table(self, name: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            cur = conn.execute("SELECT spec FROM tables WHERE name = ?", (name,))
            row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return {}

    def delete_table(self, name: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM tables WHERE name = ?", (name,))
            return cur.rowcount > 0

    # row operations
    def insert_row(self, table_name: str, row: Dict[str, Any], pk_value: Optional[str] = None) -> int:
        row_json = json.dumps(row, ensure_ascii=False, cls=UUIDEncoder)
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO table_rows(table_name, pk_value, row_json) VALUES (?, ?, ?)",
                (table_name, pk_value, row_json),
            )
            return cur.lastrowid

    def query_rows(
        self,
        table_name: str,
        pk: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]]:
        sql = "SELECT id, pk_value, row_json, created_at FROM table_rows WHERE table_name = ?"
        params: list = [table_name]
        if pk is not None:
            sql += " AND pk_value = ?"
            params.append(pk)
        if sort_by:
            # sort_by applies to json content; for simplicity, sort by created_at or id only
            if sort_by in ("created_at", "id"):
                sql += f" ORDER BY {sort_by} {sort_dir.upper()}"
        else:
            sql += " ORDER BY id DESC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        with self._conn() as conn:
            cur = conn.execute(sql, tuple(params))
            rows = cur.fetchall()

        result = []
        for _id, pk_value, row_json, created_at in rows:
            try:
                row = json.loads(row_json)
            except Exception:
                row = {}
            result.append({"id": _id, "pk": pk_value, "row": row, "created_at": created_at})
        return result


storage = DWHStorage()
