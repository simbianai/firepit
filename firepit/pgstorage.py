import logging
import os
import re
from functools import lru_cache
import threading

import psycopg2
import psycopg2.extras
import psycopg2.pool
import ujson
from psycopg2.pool import ThreadedConnectionPool

from firepit.exceptions import DuplicateTable
from firepit.exceptions import InvalidAttr
from firepit.exceptions import UnexpectedError
from firepit.exceptions import UnknownViewname
from firepit.pgcommon import (
    CHECK_FOR_COMMON_SCHEMA,
    COLUMNS_TABLE,
    CHECK_FOR_QUERIES_TABLE,
    INTERNAL_TABLES,
    LIKE_BIN,
    MATCH_BIN,
    MATCH_FUN,
    SUBNET_FUN,
    _rewrite_view_def,
    _infer_type,
    pg_shorten,
)
from firepit.splitter import SqlWriter
from firepit.sqlstorage import DB_VERSION
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import validate_name

logger = logging.getLogger(__name__)


def get_storage(url, session_id):
    dbname = url.path.lstrip("/")
    return PgStorage(dbname, url.geturl(), session_id)


# PostgreSQL defaults for COPY text format
SEP = "\t"
TEXT_ESCAPE_TABLE = str.maketrans(
    {"\\": "\\\\", "\n": "\\n", "\r": "\\r", SEP: f"\\{SEP}"}
)


@lru_cache(maxsize=256, typed=True)
def _text_encode(value):
    if value is None:
        return r"\N"
    elif not isinstance(value, str):
        return str(value)
    # MUST "escape" special chars
    return value.translate(TEXT_ESCAPE_TABLE)


class ListToTextIO:
    """
    Convert an iterable of lists into a file-like object with
    PostgreSQL TEXT formatting
    """

    def __init__(self, objs, cols, sep=SEP):
        self.it = iter(objs)
        self.cols = cols
        self.sep = sep
        self.buf = ""

    def read(self, n):
        result = ""
        try:
            while n > len(self.buf):
                obj = next(self.it)
                vals = [
                    (
                        ujson.dumps(val, ensure_ascii=False)
                        if isinstance(val, (list, dict))
                        else _text_encode(val)
                    )
                    for val in obj
                ]
                self.buf += self.sep.join(vals) + "\n"
            result = self.buf[:n]
            self.buf = self.buf[n:]
        except StopIteration:
            result = self.buf
            self.buf = ""
        return result


class TuplesToTextIO:
    """
    Convert an iterable of tuples into a file-like object
    """

    def __init__(self, objs, cols, sep=SEP):
        self.it = iter(objs)
        self.cols = cols
        self.sep = sep
        self.buf = ""

    def read(self, n):
        result = ""
        try:
            while n > len(self.buf):
                obj = next(self.it)
                self.buf += self.sep.join(obj)
                self.buf += "\n"
            result = self.buf[:n]
            self.buf = self.buf[n:]
        except StopIteration:
            result = self.buf
            self.buf = ""
        return result


class PgStorage(SqlStorage):
    _connection_pool = None
    _pool_lock = threading.Lock()

    def __init__(self, dbname, url, session_id=None):
        super().__init__()
        self.placeholder = "%s"
        self.dialect = "postgresql"
        self.text_min = "LEAST"
        self.text_max = "GREATEST"
        self.ifnull = "COALESCE"
        self.dbname = dbname
        self.infer_type = _infer_type
        self.defer_index = False
        if not session_id:
            session_id = "firepit"
        self.session_id = session_id
        options = f"options=--search-path%3D{session_id}"
        sep = "&" if "?" in url else "?"
        connstring = f"{url}{sep}{options}"
        max_pool = os.getenv("FIREPIT_MAX_DB_POOL", 100)
        logger.debug("Max DB pool size: %s", max_pool)

        with self._pool_lock:
            if self._connection_pool is None:
                self._connection_pool = ThreadedConnectionPool(
                    minconn=1,
                    maxconn=max_pool,
                    dsn=connstring,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                )

        self.connection = self._connection_pool.getconn()
        self.connection.autocommit = False

        self._create_firepit_common_schema()
        if session_id:
            try:
                with self.connection.cursor() as cursor:
                    self._execute(
                        f'CREATE SCHEMA IF NOT EXISTS "{session_id}";', cursor=cursor
                    )
            except psycopg2.errors.UniqueViolation:
                self.connection.rollback()

        with self.connection.cursor() as cursor:
            self._execute(
                f'SET search_path TO "{session_id}", firepit_common;', cursor=cursor
            )
            res = self._query(
                CHECK_FOR_QUERIES_TABLE, (session_id,), cursor=cursor
            ).fetchone()

        done = list(res.values())[0] if res else False
        if not done:
            self._setup()
        else:
            self._checkdb()

        logger.debug("Connection to PostgreSQL DB %s successful", dbname)

    def close(self):
        if self.connection:
            try:
                self.connection.rollback()
            except Exception:
                pass
            self._connection_pool.putconn(self.connection)
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _create_firepit_common_schema(self):
        try:
            with self.connection.cursor() as cursor:
                res = self._query(CHECK_FOR_COMMON_SCHEMA, cursor=cursor).fetchall()
                if not res:
                    self._execute(
                        'CREATE SCHEMA IF NOT EXISTS "firepit_common";', cursor=cursor
                    )
                    self._execute(MATCH_FUN, cursor=cursor)
                    self._execute(MATCH_BIN, cursor=cursor)
                    self._execute(LIKE_BIN, cursor=cursor)
                    self._execute(SUBNET_FUN, cursor=cursor)
                elif len(res) < 4:
                    funcs = [r["routine_name"] for r in res]
                    if "match_bin" not in funcs:
                        self._execute(MATCH_BIN, cursor=cursor)
                    if "like_bin" not in funcs:
                        self._execute(LIKE_BIN, cursor=cursor)
        except psycopg2.errors.DuplicateFunction:
            self.connection.rollback()

    def _setup(self):
        try:
            with self.connection.cursor() as cursor:
                for stmt in INTERNAL_TABLES:
                    self._execute(stmt, cursor=cursor)
                self._create_indexes(cursor)
                self._set_meta(cursor, "dbversion", DB_VERSION)
                self.connection.commit()
        except (psycopg2.errors.DuplicateFunction, psycopg2.errors.UniqueViolation):
            self.connection.rollback()

    def _create_indexes(self, cursor):
        try:
            index_definitions = [
                'CREATE INDEX IF NOT EXISTS contains_source_idx ON "__contains" (source_ref);',
                'CREATE INDEX IF NOT EXISTS contains_target_idx ON "__contains" (target_ref);',
                'CREATE INDEX IF NOT EXISTS contains_rank_idx ON "__contains" (x_firepit_rank);',
                'CREATE INDEX IF NOT EXISTS queries_sco_idx ON "__queries" (sco_id);',
                'CREATE INDEX IF NOT EXISTS queries_query_idx ON "__queries" (query_id);',
                'CREATE INDEX IF NOT EXISTS columns_otype_idx ON "__columns" (otype);',
                'CREATE INDEX IF NOT EXISTS columns_path_idx ON "__columns" (path);',
                'CREATE INDEX IF NOT EXISTS identity_name_idx ON "identity" (name);',
                'CREATE INDEX IF NOT EXISTS identity_class_idx ON "identity" (identity_class);',
                'CREATE INDEX IF NOT EXISTS identity_created_idx ON "identity" (created);',
                'CREATE INDEX IF NOT EXISTS identity_modified_idx ON "identity" (modified);',
                'CREATE INDEX IF NOT EXISTS od_created_by_ref_idx ON "observed-data" (created_by_ref);',
                'CREATE INDEX IF NOT EXISTS od_created_idx ON "observed-data" (created);',
                'CREATE INDEX IF NOT EXISTS od_modified_idx ON "observed-data" (modified);',
                'CREATE INDEX IF NOT EXISTS od_first_observed_idx ON "observed-data" (first_observed);',
                'CREATE INDEX IF NOT EXISTS od_last_observed_idx ON "observed-data" (last_observed);',
                'CREATE INDEX IF NOT EXISTS od_observed_range_idx ON "observed-data" (first_observed, last_observed);',
                'CREATE INDEX IF NOT EXISTS od_created_modified_idx ON "observed-data" (created, modified);',
            ]

            for index_stmt in index_definitions:
                logging.info(f"Creating index: {index_stmt}")
                try:
                    self._execute(index_stmt, cursor)
                except psycopg2.Error as e:
                    logging.warning(f"Failed to create index: {str(e)}")
                    continue

            logging.info("Successfully created database indexes")
        except Exception as e:
            logging.exception(f"Error creating indexes: {str(e)}")
            raise

    def _migrate(self, version, cursor):
        if version == "2":
            self._execute(COLUMNS_TABLE, cursor)
            version = "2.1"
        if version == "2.1":
            data = self.get_view_data()
            views = {}
            for row in data:
                views[row["name"]] = row

            self._execute("DROP TABLE __symtable", cursor)
            stmt = (
                'CREATE UNLOGGED TABLE IF NOT EXISTS "__symtable" '
                "(name TEXT, type TEXT, appdata TEXT,"
                " UNIQUE(name));"
            )
            self._execute(stmt, cursor)

            for view in views.values():
                stmt = (
                    'INSERT INTO "__symtable" (name, type, appdata)'
                    f" VALUES ({self.placeholder}, {self.placeholder}, {self.placeholder})"
                )
                cursor.execute(stmt, (view["name"], view["type"], view["appdata"]))
            self.connection.commit()
            version = "2.2"
        return version == DB_VERSION

    def _get_writer(self, **kwargs):
        self.defer_index = kwargs.get("defer_index", self.defer_index)
        filedir = os.path.dirname(self.dbname)
        return SqlWriter(
            filedir,
            self,
            placeholder=self.placeholder,
            infer_type=_infer_type,
            shorten=pg_shorten,
            **kwargs,
        )

    def _query(self, query, values=None, cursor=None):
        logger.debug("Executing query: %s", query)
        cursor_provided = cursor is not None
        if not cursor_provided:
            cursor = self.connection.cursor()

        if not values:
            values = ()

        try:
            try:
                cursor.execute(query, values)
            except Exception:
                logger.error("Error happened, retrying with connection rollback")
                self.connection.rollback()
                cursor.execute(query, values)
        except psycopg2.errors.UndefinedColumn as e:
            self.connection.rollback()
            if not cursor_provided:
                cursor.close()
            raise InvalidAttr(str(e)) from e
        except psycopg2.errors.UndefinedTable as e:
            self.connection.rollback()
            if not cursor_provided:
                cursor.close()
            raise UnknownViewname(str(e)) from e
        except Exception as e:
            self.connection.rollback()
            if not cursor_provided:
                cursor.close()
            logger.error("%s: %s", query, e, exc_info=e)
            raise UnexpectedError(str(e)) from e

        self.connection.commit()
        return cursor

    def _create_table(self, tablename, columns):
        stmt = f'CREATE UNLOGGED TABLE "{tablename}" ('
        stmt += ",".join(
            [f'"{colname}" {coltype}' for colname, coltype in columns.items()]
        )
        stmt += ");"
        logger.debug('_create_table: "%s"', stmt)

        try:
            with self.connection.cursor() as cursor:
                self._execute(stmt, cursor=cursor)
                if not self.defer_index:
                    logger.info("Creating index for %s", tablename)
                    self._create_index(tablename, cursor)
                self.connection.commit()
        except (
            psycopg2.errors.DuplicateTable,
            psycopg2.errors.DuplicateObject,
            psycopg2.errors.UniqueViolation,
        ) as e:
            self.connection.rollback()
            raise DuplicateTable(tablename) from e

    def _create_empty_view(self, viewname, cursor):
        cursor.execute(f'CREATE VIEW "{viewname}" AS SELECT NULL as id WHERE 1<>1;')

    def _create_view(self, viewname, select, sco_type, deps=None, cursor=None):
        validate_name(viewname)
        cursor_provided = cursor is not None
        if not cursor_provided:
            cursor = self._execute("BEGIN;")

        is_new = True
        if not deps:
            deps = []
        elif viewname in deps:
            is_new = False
            slct = self._get_view_def(viewname)
            if not self._is_sql_view(viewname, cursor):
                self._execute(
                    f'ALTER TABLE "{viewname}" RENAME TO "_{viewname}"', cursor
                )
                slct = slct.replace(viewname, f"_{viewname}")
            select = re.sub(
                f'FROM "{viewname}"', f"FROM ({slct}) AS tmp", select, count=1
            )
            select = re.sub(f'"{viewname}"', "tmp", select)

        try:
            self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {select}', cursor)
        except psycopg2.errors.UndefinedTable as e:
            self.connection.rollback()
            logger.error(e, exc_info=e)
            if not cursor_provided:
                cursor = self._execute("BEGIN;")
            self._create_empty_view(viewname, cursor)
        except psycopg2.errors.InvalidTableDefinition:
            self.connection.rollback()
            if not cursor_provided:
                cursor = self._execute("BEGIN;")
            self._execute(f'DROP VIEW IF EXISTS "{viewname}";', cursor)
            self._execute(f'CREATE VIEW "{viewname}" AS {select}', cursor)
            is_new = False
        except psycopg2.errors.SyntaxError as e:
            raise UnexpectedError(e.args[0]) from e
        except psycopg2.errors.UndefinedColumn as e:
            m = re.search(r"^column (.*) does not exist", e.args[0])
            raise InvalidAttr(m.group(1)) from e

        if is_new:
            self._new_name(cursor, viewname, sco_type)
        return cursor

    def _recreate_view(self, viewname, viewdef, cursor):
        self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {viewdef}', cursor)

    def _get_view_def(self, viewname):
        with self.connection.cursor() as cursor:
            cursor = self._query(
                "SELECT definition"
                " FROM pg_views"
                " WHERE schemaname = %s"
                " AND viewname = %s",
                (self.session_id, viewname),
                cursor=cursor,
            )
            viewdef = cursor.fetchone()
            return _rewrite_view_def(viewname, viewdef)

    def _is_sql_view(self, name, cursor=None):
        cursor_provided = cursor is not None
        if not cursor_provided:
            cursor = self.connection.cursor()

        try:
            cursor = self._query(
                "SELECT definition"
                " FROM pg_views"
                " WHERE schemaname = %s"
                " AND viewname = %s",
                (self.session_id, name),
                cursor=cursor,
            )
            viewdef = cursor.fetchone()
            return viewdef is not None
        finally:
            if not cursor_provided:
                cursor.close()

    def tables(self):
        with self.connection.cursor() as cursor:
            cursor = self._query(
                "SELECT table_name"
                " FROM information_schema.tables"
                " WHERE table_schema = %s"
                "   AND table_type != 'VIEW'",
                (self.session_id,),
                cursor=cursor,
            )
            rows = cursor.fetchall()
            return [
                i["table_name"] for i in rows if not i["table_name"].startswith("__")
            ]

    def types(self, private=False):
        stmt = (
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = %s AND table_type != 'VIEW'"
            "  EXCEPT SELECT name as table_name FROM __symtable"
        )

        with self.connection.cursor() as cursor:
            cursor = self._query(stmt, (self.session_id,), cursor=cursor)
            rows = cursor.fetchall()

        if private:
            return [i["table_name"] for i in rows]
        return [i["table_name"] for i in rows if not i["table_name"].startswith("_")]

    def columns(self, viewname):
        validate_name(viewname)
        with self.connection.cursor() as cursor:
            cursor = self._query(
                "SELECT column_name"
                " FROM information_schema.columns"
                " WHERE table_schema = %s"
                " AND table_name = %s",
                (self.session_id, viewname),
                cursor=cursor,
            )
            rows = cursor.fetchall()
            return [i["column_name"] for i in rows]

    def schema(self, viewname=None):
        with self.connection.cursor() as cursor:
            if viewname:
                validate_name(viewname)
                cursor = self._query(
                    "SELECT column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = %s"
                    " AND table_name = %s",
                    (self.session_id, viewname),
                    cursor=cursor,
                )
            else:
                cursor = self._query(
                    "SELECT table_name AS table, column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = %s",
                    (self.session_id,),
                    cursor=cursor,
                )
            return cursor.fetchall()

    def delete(self):
        """Delete ALL data in this store"""
        with self.connection.cursor() as cursor:
            self._execute(f'DROP SCHEMA "{self.session_id}" CASCADE;', cursor=cursor)
            self.connection.commit()

    def upsert_many(self, cursor, tablename, objs, query_id, schema, **kwargs):
        use_copy = kwargs.get("use_copy")
        if use_copy:
            self.upsert_copy(cursor, tablename, objs, query_id, schema)
        else:
            self.upsert_multirow(cursor, tablename, objs, query_id, schema)

    def upsert_multirow(self, cursor, tablename, objs, query_id, schema):
        colnames = list(schema.keys())
        quoted_colnames = [f'"{x}"' for x in colnames]
        valnames = ", ".join(quoted_colnames)

        placeholders = ", ".join(
            [f"({', '.join([self.placeholder] * len(colnames))})"] * len(objs)
        )
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES {placeholders}'
        idx = None
        if "id" in colnames:
            idx = colnames.index("id")
            action = "NOTHING"
            if tablename != "identity":
                excluded = self._get_excluded(colnames, tablename)
                if excluded:
                    action = f"UPDATE SET {excluded}"
            stmt += f" ON CONFLICT (id) DO {action}"
        else:
            stmt += " ON CONFLICT DO NOTHING"

        values = []
        query_values = []
        for obj in objs:
            if query_id and idx is not None:
                query_values.append(obj[idx])
                query_values.append(query_id)
            values.extend(
                [
                    (
                        ujson.dumps(value, ensure_ascii=False)
                        if isinstance(value, (list, dict))
                        else value
                    )
                    for value in obj
                ]
            )

        cursor.execute(stmt, values)

        if query_id and "id" in colnames:
            placeholders = ", ".join(
                [f"({self.placeholder}, {self.placeholder})"] * len(objs)
            )
            stmt = (
                f'INSERT INTO "__queries" (sco_id, query_id)' f" VALUES {placeholders}"
            )
            cursor.execute(stmt, query_values)

    def upsert_copy(self, cursor, tablename, objs, query_id, schema):
        colnames = list(schema.keys())
        quoted_colnames = [f'"{x}"' for x in colnames]
        valnames = ", ".join(quoted_colnames)

        s = ", ".join([f'"{name}" {ctype}' for name, ctype in schema.items()])
        stmt = f"CREATE TEMP TABLE tmp ({s})"
        logger.debug("%s", stmt)
        cursor.execute(stmt)

        copy_stmt = f"COPY tmp({valnames}) FROM STDIN WITH DELIMITER '{SEP}'"
        cursor.copy_expert(copy_stmt, ListToTextIO(objs, colnames, sep=SEP))

        stmt = f'INSERT INTO "{tablename}" ({valnames}) SELECT {valnames} FROM tmp'
        if "id" in colnames:
            stmt += " ORDER BY id"
            action = "NOTHING"
            if tablename != "identity":
                excluded = self._get_excluded(colnames, tablename)
                if excluded:
                    action = f"UPDATE SET {excluded}"
            stmt += f" ON CONFLICT (id) DO {action}"
        else:
            stmt += " ON CONFLICT DO NOTHING"

        cursor.execute(stmt)
        cursor.execute("DROP TABLE tmp")

        if query_id and "id" in colnames:
            idx = colnames.index("id")
            copy_stmt = (
                f'COPY "__queries"(sco_id, query_id)'
                f" FROM STDIN WITH DELIMITER '{SEP}'"
            )
            qobjs = [(obj[idx], query_id) for obj in objs]
            cursor.copy_expert(
                copy_stmt, TuplesToTextIO(qobjs, ["sco_id", "query_id"], sep=SEP)
            )

    def finish(self, index=True):
        if index:
            with self.connection.cursor() as cursor:
                cursor = self._query(
                    "SELECT table_name"
                    " FROM information_schema.tables"
                    " WHERE table_schema = %s"
                    "   AND table_name IN (%s, %s)",
                    (self.session_id, "__contains", "__reflist"),
                    cursor=cursor,
                )
                rows = cursor.fetchall()
                tables = [i["table_name"] for i in rows]

                if "relationship" in self.tables():
                    tables.append("relationship")

                for tablename in tables:
                    self._create_index(tablename, cursor)
                self.connection.commit()
