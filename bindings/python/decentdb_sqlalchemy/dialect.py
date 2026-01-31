from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import util
from sqlalchemy import exc

import decentdb

from sqlalchemy.sql import compiler

class DecentDbCompiler(compiler.SQLCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " % " + self.process(binary.right, **kw)

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                # DecentDB might require LIMIT if OFFSET is present?
                # Standard SQL often doesn't, but some DBs do.
                # Assuming standard behavior.
                text += "\n OFFSET " + self.process(select._offset_clause, **kw)
            else:
                text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

class DecentDbTypeCompiler(compiler.GenericTypeCompiler):
    def visit_integer(self, type_, **kw):
        return "INT64"

    def visit_big_integer(self, type_, **kw):
        return "INT64"

    def visit_small_integer(self, type_, **kw):
        return "INT64"

    def visit_boolean(self, type_, **kw):
        return "BOOL"

    def visit_float(self, type_, **kw):
        return "FLOAT64"
    
    def visit_numeric(self, type_, **kw):
        # DecentDB stores Decimal as TEXT per requirements
        return "TEXT"

    def visit_string(self, type_, **kw):
        return "TEXT"

    def visit_text(self, type_, **kw):
        return "TEXT"

    def visit_large_binary(self, type_, **kw):
        return "BLOB"
        
    def visit_date(self, type_, **kw):
        return "INT64"

    def visit_datetime(self, type_, **kw):
        return "INT64"

    def visit_time(self, type_, **kw):
        return "INT64"
        
    def visit_uuid(self, type_, **kw):
        return "BLOB"

class DecentDbDialect(default.DefaultDialect):
    name = "decentdb"
    driver = "pysql"
    supports_alter = False
    supports_pk_autoincrement = False # Use explicit IDs or nextval logic if available?
    # DecentDB doesn't have AUTOINCREMENT keyword in CREATE TABLE yet (based on parsing logic)
    # Actually check parseColumnType in catalog.nim - no AUTOINCREMENT flag.
    # But it has `nextRowId` in TableMeta.
    # Insert logic might handle it?
    # "MVP: SELECT/INSERT/UPDATE/DELETE"
    # User likely needs to provide ID or we rely on engine.
    
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_statement_cache = True
    supports_native_boolean = True
    
    default_paramstyle = "qmark"

    statement_compiler = DecentDbCompiler
    type_compiler = DecentDbTypeCompiler
    
    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    @classmethod
    def import_dbapi(cls):
        return decentdb

    def create_connect_args(self, url):
        # url is decentdb+pysql:////path/to.db
        # path is url.database
        # query options
        
        opts = dict(url.query) # Convert to mutable dict
        path = url.database
        
        if path is None:
             path = ":memory:" # Does DecentDB support in-memory? Maybe not.
        
        return ([path], opts)

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_close(self, dbapi_connection):
        dbapi_connection.close()
        
    def get_isolation_level(self, dbapi_connection):
        return "SNAPSHOT"

    def set_isolation_level(self, dbapi_connection, level):
        if level != "SNAPSHOT":
            raise exc.ArgumentError(f"Invalid isolation level: {level}. DecentDB only supports SNAPSHOT.")

    # Introspection methods (stubbed for now)
    def get_table_names(self, connection, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, **kw):
        return False
