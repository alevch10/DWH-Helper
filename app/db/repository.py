from typing import List, Dict, Any, Optional, Set, Tuple
from uuid import UUID

import psycopg2
from psycopg2 import pool, extras
from psycopg2.sql import SQL, Identifier, Placeholder
from psycopg2.extras import register_uuid

from app.config.settings import settings
from app.config.logger import get_logger
from app.db import schemas

logger = get_logger(__name__)
register_uuid()


class DBRepository:
    """
    Единый репозиторий для работы с PostgreSQL.
    Использует пул соединений с autocommit = True.
    """

    TABLE_MODEL_MAP = {
        "events_part": schemas.EventsPart,
        "mobile_devices": schemas.MobileDevices,
        "permanent_user_properties": schemas.PermanentUserProperties,
        "technical_data": schemas.TechnicalData,
        "tmp_event_properties": schemas.TmpEventProperties,
        "tmp_user_properties": schemas.TmpUserProperties,
        "user_locations": schemas.UserLocations,
        "changeable_user_properties": schemas.ChangeableUserProperties,
    }

    def __init__(self):
        self.pool = pool.ThreadedConnectionPool(
            minconn=settings.db.minconn,
            maxconn=settings.db.maxconn,
            dbname=settings.db.name,
            user=settings.db.user,
            password=settings.db.password,
            host=settings.db.host,
            port=settings.db.port,
            cursor_factory=extras.RealDictCursor,
        )
        self._column_counts: Dict[str, int] = {}
        self._precompute_column_counts()
        logger.info(f"DBRepository initialized. Column counts: {self._column_counts}")

    def _precompute_column_counts(self):
        for table_name, model_class in self.TABLE_MODEL_MAP.items():
            self._column_counts[table_name] = len(model_class.model_fields)

    def _max_rows_for_table(self, table_name: str) -> int:
        """Максимальное количество строк в одном INSERT с учётом протокола."""
        if table_name not in self._column_counts:
            logger.warning(
                f"Unknown table '{table_name}', using fallback column count = 20"
            )
            col_count = 20
        else:
            col_count = self._column_counts[table_name]

        theoretical_max = settings.db.max_params_per_query // col_count
        safe_max = int(theoretical_max * settings.db.safety_factor)
        configured_max = getattr(settings.db, "max_rows_per_insert")
        return min(safe_max, configured_max)

    # ---------- Управление соединениями ----------
    def _get_conn(self):
        """Получить соединение из пула с включённым autocommit."""
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def _put_conn(self, conn):
        """Вернуть соединение в пул."""
        self.pool.putconn(conn)

    def execute(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Выполнить запрос и вернуть результат.
        При SELECT возвращает список словарей, при UPDATE/INSERT — пустой список.
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:  # SELECT
                    return cur.fetchall()
                return []
        finally:
            self._put_conn(conn)

    # ---------- Вспомогательный метод для as_string ----------
    def _to_sql_string(self, sql_obj: SQL) -> str:
        """
        Безопасно преобразует объект SQL в строку запроса,
        гарантированно возвращая соединение в пул.
        """
        conn = self._get_conn()
        try:
            return sql_obj.as_string(conn)
        finally:
            self._put_conn(conn)

    # ---------- Вставка данных ----------
    def insert_one(
        self,
        table: str,
        data: Dict[str, Any],
        on_conflict: Optional[str] = None,
        conflict_target: Optional[str] = None,
    ) -> None:
        columns = list(data.keys())
        values = [data[col] for col in columns]
        placeholders = [Placeholder()] * len(columns)

        logger.info("insert_one")
        query = SQL("INSERT INTO {} ({}) VALUES ({})").format(
            Identifier(table),
            SQL(", ").join(map(Identifier, columns)),
            SQL(", ").join(placeholders),
        )
        if on_conflict:
            conflict_clause = SQL(" ON CONFLICT {} {}").format(
                SQL(conflict_target) if conflict_target else SQL(""),
                SQL(on_conflict),
            )
            query += conflict_clause

        query_str = self._to_sql_string(query)
        self.execute(query_str, tuple(values))
        logger.info("insert_one fihished")

    def insert_permanent(self, record: schemas.PermanentUserProperties) -> None:
        """Вставка или игнорирование записи в permanent_user_properties (ON CONFLICT DO NOTHING)."""
        data = record.model_dump()
        self.insert_one(
            table="permanent_user_properties",
            data=data,
            on_conflict="DO NOTHING",
            conflict_target="(ehr_id)",
        )

    def insert_batch(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        on_conflict: Optional[str] = None,
        conflict_target: Optional[str] = None,
        returning_column: Optional[str] = None,
    ) -> Tuple[List[str], int]:
        """
        Вставка множества строк с автоматическим разбиением по лимиту параметров.
        Возвращает кортеж: (список первичных ключей, количество использованных батчей)
        """
        if not rows:
            return [], 0

        max_rows_per_batch = self._max_rows_for_table(table)
        all_inserted_ids: List[str] = []
        batches_used = 0
        logger.info("insert_batch")
        for i in range(0, len(rows), max_rows_per_batch):
            chunk = rows[i : i + max_rows_per_batch]
            ids = self._insert_batch_query(
                table, chunk, on_conflict, conflict_target, returning_column
            )
            all_inserted_ids.extend(ids)
            batches_used += 1
        logger.info("batch inserted")
        return all_inserted_ids, batches_used

    def _insert_batch_query(
        self,
        table: str,
        rows: List[Dict],
        on_conflict: Optional[str],
        conflict_target: Optional[str],
        returning_column: Optional[str] = None,
    ) -> List[str]:
        """
        Выполняет INSERT ... VALUES (...), (...) ...
        Если указан returning_column — добавляет RETURNING и возвращает список значений.
        Возвращает список строк (значения приведены к str).
        """
        if not rows:
            return []

        columns = list(rows[0].keys())
        value_rows = []
        params = []

        for row in rows:
            placeholders = []
            for col in columns:
                params.append(row[col])
                placeholders.append("%s")
            value_rows.append(f"({', '.join(placeholders)})")

        query = f"""
            INSERT INTO {table} ({", ".join(columns)})
            VALUES {", ".join(value_rows)}
        """
        if on_conflict:
            query += f" ON CONFLICT {conflict_target or ''} {on_conflict}"
        if returning_column:
            query += f" RETURNING {returning_column}"

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if returning_column:
                    rows = cur.fetchall()
                    inserted_ids = [str(row[0]) for row in rows]
                else:
                    inserted_ids = []
                return inserted_ids
        finally:
            self._put_conn(conn)

    # ---------- Выборка данных ----------
    def select(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        query = SQL("SELECT * FROM {}").format(Identifier(table))
        params = []

        if where:
            conditions = []
            for col, val in where.items():
                conditions.append(SQL("{} = %s").format(Identifier(col)))
                params.append(val)
            query += SQL(" WHERE ") + SQL(" AND ").join(conditions)

        if order_by:
            order_parts = []
            for o in order_by:
                if o.startswith("-"):
                    order_parts.append(SQL("{} DESC").format(Identifier(o[1:])))
                else:
                    order_parts.append(SQL("{} ASC").format(Identifier(o)))
            query += SQL(" ORDER BY ") + SQL(", ").join(order_parts)

        if limit:
            query += SQL(" LIMIT %s")
            params.append(limit)
        if offset:
            query += SQL(" OFFSET %s")
            params.append(offset)

        query_str = self._to_sql_string(query)
        return self.execute(query_str, tuple(params))

    def get_by_pk(self, table: str, pk_column: str, pk_value: Any) -> Optional[Dict]:
        rows = self.select(table, where={pk_column: pk_value}, limit=1)
        return rows[0] if rows else None

    # ---------- Специфические методы ----------
    def get_all_permanent_ehr_ids(self) -> Set[int]:
        logger.info("ehr list select")
        rows = self.execute("SELECT ehr_id FROM permanent_user_properties")
        logger.info("ehr list selected")
        return {row["ehr_id"] for row in rows}

    def get_latest_changeable_for_ehrs(
        self, ehr_ids: List[Optional[int]]
    ) -> Dict[Optional[int], schemas.ChangeableUserProperties]:
        if not ehr_ids:
            return {}

        non_null = [eid for eid in ehr_ids if eid is not None]
        has_null = any(eid is None for eid in ehr_ids)
        result = {}
        logger.info("last changeable_user_propertie select")
        if non_null:
            placeholders = ",".join(["%s"] * len(non_null))
            query = f"""
                WITH ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY ehr_id ORDER BY event_time DESC) AS rn
                    FROM changeable_user_properties
                    WHERE ehr_id IN ({placeholders})
                )
                SELECT * FROM ranked WHERE rn = 1
            """
            rows = self.execute(query, tuple(non_null))
            for row in rows:
                row.pop("rn", None)
                result[row["ehr_id"]] = schemas.ChangeableUserProperties(**row)

        if has_null:
            query = """
                SELECT *
                FROM changeable_user_properties
                WHERE ehr_id IS NULL
                ORDER BY event_time DESC
                LIMIT 1
            """
            row = self.execute(query)
            if row:
                result[None] = schemas.ChangeableUserProperties(**row[0])
        logger.info("last changeable_user_propertie selected")
        return result

    def insert_changeable(self, record: schemas.ChangeableUserProperties) -> None:
        """Вставить новую запись в changeable_user_properties (история изменений)."""
        if record.ehr_id is None:
            logger.debug(
                f"Ignoring changeable record with ehr_id=None (uuid={record.uuid})"
            )
            return

        data = record.model_dump()
        columns = list(data.keys())
        placeholders = ["%s"] * len(columns)
        query = f"""
            INSERT INTO changeable_user_properties ({", ".join(columns)})
            VALUES ({", ".join(placeholders)})
        """
        self.execute(query, tuple(data.values()))

    def update_migrated_tmp(self, uuid: UUID, migrated: bool = True) -> None:
        """Пометить запись как обработанную. Принимает UUID (не строку)."""
        query = "UPDATE tmp_user_properties SET migrated = %s WHERE uuid = %s"
        self.execute(query, (migrated, uuid))


# ---------- Глобальный синглтон ----------
_repository_instance = None


def get_repository() -> DBRepository:
    """Возвращает глобальный экземпляр DBRepository (синглтон)."""
    global _repository_instance
    if _repository_instance is None:
        _repository_instance = DBRepository()
        logger.info("Created global DBRepository instance")
    return _repository_instance


def close_repository():
    """Закрыть все соединения в пуле (вызвать при завершении приложения)."""
    global _repository_instance
    if _repository_instance:
        _repository_instance.pool.closeall()
        _repository_instance = None
        logger.info("Closed DBRepository connection pool")
