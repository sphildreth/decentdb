import json
import os
import tempfile
import unittest
from pathlib import Path
import subprocess
import sys
import shutil
import random


class RunnerTests(unittest.TestCase):
    def test_open_close_scenario(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        runner = repo_root / "tests" / "harness" / "runner.py"
        engine = repo_root / "tests" / "harness" / "fake_engine.py"
        scenario = repo_root / "tests" / "harness" / "scenarios" / "open_close.json"
        with tempfile.TemporaryDirectory() as temp_dir:
            env = os.environ.copy()
            env["TMPDIR"] = temp_dir
            proc = subprocess.run(
                [sys.executable, str(runner), "--engine", str(engine), "--scenario", str(scenario)],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(proc.stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["scenario"], "open_close")
            self.assertIn("seed", payload)


class DifferentialLikeTests(unittest.TestCase):
    def test_like_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        random.seed(7)
        rows = [(i, "".join(chr(ord("A") + random.randint(0, 25)) for _ in range(8))) for i in range(1, 6)]
        patterns = [rows[0][1][1:4], rows[1][1][2:6], "XYZ"]
        schema = f"decentdb_test_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            create_sql = f"CREATE SCHEMA {schema}; CREATE TABLE {schema}.docs (id INT PRIMARY KEY, body TEXT);"
            insert_sql = "INSERT INTO {schema}.docs (id, body) VALUES {values};".format(
                schema=schema,
                values=",".join(f"({row[0]}, '{row[1]}')" for row in rows),
            )
            run_psql(create_sql + " " + insert_sql)

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_like.ddb"
                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                payload = run_cli("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                for row in rows:
                    payload = run_cli(f"INSERT INTO docs (id, body) VALUES ({row[0]}, '{row[1]}')")
                    self.assertTrue(payload.get("ok"), msg=payload.get("error"))

                for pattern in patterns:
                    pg_rows = run_psql(
                        f"SELECT id FROM {schema}.docs WHERE body LIKE '%{pattern}%' ORDER BY id;"
                    )
                    payload = run_cli(f"SELECT id FROM docs WHERE body LIKE '%{pattern}%' ORDER BY id")
                    self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                    decent_rows = payload.get("rows", [])
                    self.assertEqual(decent_rows, pg_rows)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_nested_view_matches_sqlite_and_duckdb(self) -> None:
        sqlite = shutil.which("sqlite3")
        duckdb = shutil.which("duckdb")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not sqlite or not duckdb or not cli:
            self.skipTest("sqlite3, duckdb, or decentdb not available")

        schema_sql = [
            "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)",
            "INSERT INTO docs VALUES (1, 'alpha'), (2, 'beta'), (3, 'almanac')",
            "CREATE VIEW v1 AS SELECT id, body FROM docs",
            "CREATE VIEW v2 AS SELECT id FROM v1 WHERE body LIKE '%al%'",
        ]
        query_sql = "SELECT id FROM v2 ORDER BY id"
        dml_sql = "INSERT INTO v1 VALUES (9, 'x')"

        with tempfile.TemporaryDirectory() as temp_dir:
            decent_db = Path(temp_dir) / "decent_view.ddb"
            sqlite_db = Path(temp_dir) / "sqlite_view.db"
            duckdb_db = Path(temp_dir) / "duck_view.db"

            def run_decent(sql: str) -> dict:
                proc = subprocess.run(
                    [cli, "exec", "--db", str(decent_db), "--sql", sql],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                try:
                    payload = json.loads(proc.stdout.strip() or "{}")
                except json.JSONDecodeError:
                    payload = {}
                return payload

            def run_sqlite(sql: str) -> tuple[int, list[str]]:
                proc = subprocess.run(
                    [sqlite, str(sqlite_db), "-cmd", ".mode list", sql],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                rows = [line for line in proc.stdout.strip().splitlines() if line]
                return proc.returncode, rows

            def run_duckdb(sql: str) -> tuple[int, list[str]]:
                proc = subprocess.run(
                    [duckdb, str(duckdb_db), "-csv", "-c", sql],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                rows = [line for line in proc.stdout.strip().splitlines() if line]
                # Drop CSV header if present.
                if rows and rows[0].lower() == "id":
                    rows = rows[1:]
                return proc.returncode, rows

            for stmt in schema_sql:
                dec = run_decent(stmt)
                if "ok" not in dec:
                    self.skipTest("decentdb CLI did not return JSON payload with ok field")
                self.assertTrue(dec.get("ok"), msg=dec.get("error"))
                sqlite_rc, _ = run_sqlite(stmt)
                self.assertEqual(sqlite_rc, 0, msg=f"sqlite failed: {stmt}")
                duck_rc, _ = run_duckdb(stmt)
                self.assertEqual(duck_rc, 0, msg=f"duckdb failed: {stmt}")

            dec = run_decent(query_sql)
            if "ok" not in dec:
                self.skipTest("decentdb CLI did not return JSON payload with ok field")
            self.assertTrue(dec.get("ok"), msg=dec.get("error"))
            sqlite_rc, sqlite_rows = run_sqlite(query_sql)
            self.assertEqual(sqlite_rc, 0)
            duck_rc, duck_rows = run_duckdb(query_sql)
            self.assertEqual(duck_rc, 0)

            decent_rows = dec.get("rows", [])
            self.assertEqual(decent_rows, sqlite_rows)
            self.assertEqual(decent_rows, duck_rows)

            dec_dml = run_decent(dml_sql)
            self.assertFalse(dec_dml.get("ok", False))
            sqlite_rc, _ = run_sqlite(dml_sql)
            self.assertNotEqual(sqlite_rc, 0)
            duck_rc, _ = run_duckdb(dml_sql)
            self.assertNotEqual(duck_rc, 0)

    def test_null_three_valued_logic_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_null_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.t (id INT PRIMARY KEY, a INT, b INT); "
                f"INSERT INTO {schema}.t VALUES "
                f"(1, 1, 1), (2, 1, NULL), (3, NULL, 1), (4, NULL, NULL), (5, 0, 1);"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_null_3vl.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                payload = run_cli("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli(
                    "INSERT INTO t VALUES (1, 1, 1), (2, 1, NULL), (3, NULL, 1), (4, NULL, NULL), (5, 0, 1)"
                )
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))

                queries = [
                    "SELECT id FROM t WHERE a = 1 OR b = NULL ORDER BY id",
                    "SELECT id FROM t WHERE a = 1 AND b = NULL ORDER BY id",
                    "SELECT id FROM t WHERE NOT (a = NULL) ORDER BY id",
                    "SELECT id FROM t WHERE a IN (1, NULL) ORDER BY id",
                    "SELECT id FROM t WHERE a = NULL ORDER BY id",
                ]
                for query in queries:
                    pg_rows = run_psql(query.replace("FROM t", f"FROM {schema}.t"))
                    payload = run_cli(query)
                    self.assertTrue(payload.get("ok"), msg=f"{query}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=query)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_scalar_functions_and_concat_match_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_func_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.t (id INT PRIMARY KEY, val INT, name TEXT); "
                f"INSERT INTO {schema}.t VALUES (1, NULL, '  AbC  '), (2, 20, 'xy');"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_scalar_funcs.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                payload = run_cli("CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli("INSERT INTO t VALUES (1, NULL, '  AbC  '), (2, 20, 'xy')")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))

                queries = [
                    "SELECT COALESCE(val, 99) FROM t ORDER BY id",
                    "SELECT NULLIF(val, 20) FROM t ORDER BY id",
                    "SELECT LENGTH(name), LOWER(name), UPPER(name), TRIM(name), TRIM(name) || '_x' FROM t WHERE id = 1",
                ]
                for query in queries:
                    pg_rows = run_psql(query.replace("FROM t", f"FROM {schema}.t"))
                    payload = run_cli(query)
                    self.assertTrue(payload.get("ok"), msg=f"{query}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=query)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_case_cast_between_exists_and_like_escape_match_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_case_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.t (id INT PRIMARY KEY, name TEXT); "
                f"CREATE TABLE {schema}.t2 (id INT); "
                f"INSERT INTO {schema}.t VALUES (1, 'a_%'); "
                f"INSERT INTO {schema}.t VALUES (2, 'abc'); "
                f"INSERT INTO {schema}.t2 VALUES (7);"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_case_cast_exists.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                payload = run_cli("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli("CREATE TABLE t2 (id INT)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli("INSERT INTO t VALUES (1, 'a_%')")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli("INSERT INTO t VALUES (2, 'abc')")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                payload = run_cli("INSERT INTO t2 VALUES (7)")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))

                queries = [
                    "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END, CAST(id AS TEXT) FROM t ORDER BY id",
                    "SELECT id FROM t WHERE id BETWEEN 1 AND 1 ORDER BY id",
                    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t2) ORDER BY id",
                    "SELECT id FROM t WHERE name LIKE 'a#_%' ESCAPE '#' ORDER BY id",
                ]
                for query in queries:
                    pg_rows = run_psql(query.replace("FROM t", f"FROM {schema}.t").replace("FROM t2", f"FROM {schema}.t2"))
                    payload = run_cli(query)
                    self.assertTrue(payload.get("ok"), msg=f"{query}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=query)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_on_conflict_do_nothing_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_upsert_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL); "
                f"INSERT INTO {schema}.users VALUES (1, 'a@x', 'alice'); "
                f"INSERT INTO {schema}.users VALUES (1, 'b@x', 'dup-id') ON CONFLICT DO NOTHING; "
                f"INSERT INTO {schema}.users VALUES (2, 'a@x', 'dup-email') ON CONFLICT (email) DO NOTHING;"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_on_conflict.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                setup_sql = [
                    "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL)",
                    "INSERT INTO users VALUES (1, 'a@x', 'alice')",
                    "INSERT INTO users VALUES (1, 'b@x', 'dup-id') ON CONFLICT DO NOTHING",
                    "INSERT INTO users VALUES (2, 'a@x', 'dup-email') ON CONFLICT (email) DO NOTHING",
                ]
                for stmt in setup_sql:
                    payload = run_cli(stmt)
                    self.assertTrue(payload.get("ok"), msg=f"{stmt}: {payload.get('error')}")

                query = "SELECT id, email, name FROM users ORDER BY id"
                pg_rows = run_psql(query.replace("FROM users", f"FROM {schema}.users"))
                payload = run_cli(query)
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                self.assertEqual(payload.get("rows", []), pg_rows)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_on_conflict_do_update_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_upsert_upd_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL); "
                f"INSERT INTO {schema}.users VALUES (1, 'a@x', 1); "
                f"INSERT INTO {schema}.users VALUES (1, 'b@x', 5) "
                f"ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, visits = {schema}.users.visits + EXCLUDED.visits; "
                f"INSERT INTO {schema}.users VALUES (1, 'c@x', 9) "
                f"ON CONFLICT (id) DO UPDATE SET visits = EXCLUDED.visits WHERE {schema}.users.email = 'nope';"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_on_conflict_update.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                setup_sql = [
                    "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL)",
                    "INSERT INTO users VALUES (1, 'a@x', 1)",
                    "INSERT INTO users VALUES (1, 'b@x', 5) "
                    "ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, visits = users.visits + EXCLUDED.visits",
                    "INSERT INTO users VALUES (1, 'c@x', 9) "
                    "ON CONFLICT (id) DO UPDATE SET visits = EXCLUDED.visits WHERE users.email = 'nope'",
                ]
                for stmt in setup_sql:
                    payload = run_cli(stmt)
                    self.assertTrue(payload.get("ok"), msg=f"{stmt}: {payload.get('error')}")

                query = "SELECT id, email, visits FROM users ORDER BY id"
                pg_rows = run_psql(query.replace("FROM users", f"FROM {schema}.users"))
                payload = run_cli(query)
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                self.assertEqual(payload.get("rows", []), pg_rows)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_insert_returning_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_returning_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL);"
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_insert_returning.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                setup = run_cli("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL)")
                self.assertTrue(setup.get("ok"), msg=setup.get("error"))

                query_pairs = [
                    (
                        "INSERT INTO users VALUES (1, 'a@x', 1) RETURNING id, email",
                        f"INSERT INTO {schema}.users VALUES (1, 'a@x', 1) RETURNING id, email",
                    ),
                    (
                        "INSERT INTO users VALUES (1, 'a@x', 4) ON CONFLICT (id) DO UPDATE "
                        "SET visits = users.visits + EXCLUDED.visits RETURNING visits",
                        f"INSERT INTO {schema}.users VALUES (1, 'a@x', 4) ON CONFLICT (id) DO UPDATE "
                        f"SET visits = {schema}.users.visits + EXCLUDED.visits RETURNING visits",
                    ),
                    (
                        "INSERT INTO users VALUES (1, 'dup@x', 9) ON CONFLICT DO NOTHING RETURNING id",
                        f"INSERT INTO {schema}.users VALUES (1, 'dup@x', 9) ON CONFLICT DO NOTHING RETURNING id",
                    ),
                ]
                for cli_sql, pg_sql in query_pairs:
                    pg_rows = run_psql(pg_sql)
                    payload = run_cli(cli_sql)
                    self.assertTrue(payload.get("ok"), msg=f"{cli_sql}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=cli_sql)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_non_recursive_cte_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_cte_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.users (id INT PRIMARY KEY, name TEXT); "
                f"CREATE TABLE {schema}.t (id INT PRIMARY KEY); "
                f"INSERT INTO {schema}.users VALUES (1, 'a'), (2, 'b'), (3, 'c'); "
                f"INSERT INTO {schema}.t VALUES (99);"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_cte.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                for stmt in [
                    "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
                    "CREATE TABLE t (id INT PRIMARY KEY)",
                    "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                    "INSERT INTO t VALUES (99)",
                ]:
                    payload = run_cli(stmt)
                    self.assertTrue(payload.get("ok"), msg=f"{stmt}: {payload.get('error')}")

                query_pairs = [
                    (
                        "WITH base AS (SELECT id, name FROM users WHERE id <= 2) "
                        "SELECT id, name FROM base ORDER BY id",
                        f"WITH base AS (SELECT id, name FROM {schema}.users WHERE id <= 2) "
                        "SELECT id, name FROM base ORDER BY id",
                    ),
                    (
                        "WITH a AS (SELECT id FROM users), b(x) AS (SELECT id FROM a WHERE id > 1) "
                        "SELECT x FROM b ORDER BY x",
                        f"WITH a AS (SELECT id FROM {schema}.users), b(x) AS (SELECT id FROM a WHERE id > 1) "
                        "SELECT x FROM b ORDER BY x",
                    ),
                    (
                        "WITH t AS (SELECT id FROM users WHERE id = 1) SELECT id FROM t",
                        f"WITH t AS (SELECT id FROM {schema}.users WHERE id = 1) SELECT id FROM t",
                    ),
                ]

                for cli_query, pg_query in query_pairs:
                    pg_rows = run_psql(pg_query)
                    payload = run_cli(cli_query)
                    self.assertTrue(payload.get("ok"), msg=f"{cli_query}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=cli_query)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_union_all_matches_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_union_all_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.a (id INT); "
                f"CREATE TABLE {schema}.b (id INT); "
                f"INSERT INTO {schema}.a VALUES (1), (2); "
                f"INSERT INTO {schema}.b VALUES (2), (3);"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_union_all.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                for stmt in [
                    "CREATE TABLE a (id INT)",
                    "CREATE TABLE b (id INT)",
                    "INSERT INTO a VALUES (1), (2)",
                    "INSERT INTO b VALUES (2), (3)",
                ]:
                    payload = run_cli(stmt)
                    self.assertTrue(payload.get("ok"), msg=f"{stmt}: {payload.get('error')}")

                queries = [
                    (
                        "SELECT id FROM a UNION ALL SELECT id FROM b",
                        f"SELECT id FROM {schema}.a UNION ALL SELECT id FROM {schema}.b",
                    ),
                    (
                        "SELECT id FROM a UNION SELECT id FROM b",
                        f"SELECT id FROM {schema}.a UNION SELECT id FROM {schema}.b",
                    ),
                    (
                        "SELECT id FROM a INTERSECT SELECT id FROM b",
                        f"SELECT id FROM {schema}.a INTERSECT SELECT id FROM {schema}.b",
                    ),
                    (
                        "SELECT id FROM a EXCEPT SELECT id FROM b",
                        f"SELECT id FROM {schema}.a EXCEPT SELECT id FROM {schema}.b",
                    ),
                ]
                for cli_query, pg_query in queries:
                    pg_rows = run_psql(pg_query)
                    payload = run_cli(cli_query)
                    self.assertTrue(payload.get("ok"), msg=f"{cli_query}: {payload.get('error')}")
                    self.assertEqual(payload.get("rows", []), pg_rows, msg=cli_query)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass

    def test_check_constraints_match_postgres(self) -> None:
        psql = shutil.which("psql")
        cli = os.environ.get("DECENTDB")
        if cli is None:
            repo_root = Path(__file__).resolve().parents[2]
            candidate = repo_root / "decentdb"
            if candidate.exists():
                cli = str(candidate)
        if not psql or not cli:
            self.skipTest("psql or decentdb not available")
        if "PGDATABASE" not in os.environ:
            self.skipTest("PGDATABASE not set for PostgreSQL differential test")

        schema = f"decentdb_check_{random.randint(1000, 9999)}"

        def run_psql(sql: str) -> list[str]:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())
            return [line for line in proc.stdout.strip().splitlines() if line]

        def run_psql_expect_error(sql: str) -> bool:
            proc = subprocess.run(
                [psql, "-X", "-q", "-t", "-A", "-c", sql],
                capture_output=True,
                text=True,
                check=False,
            )
            return proc.returncode != 0

        try:
            run_psql(
                f"CREATE SCHEMA {schema}; "
                f"CREATE TABLE {schema}.accounts ("
                "id INT PRIMARY KEY, "
                "amount INT, "
                "note TEXT, "
                "CONSTRAINT amount_nonneg CHECK (amount >= 0), "
                "CHECK (note IS NULL OR LENGTH(note) > 0)"
                ");"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "diff_check_constraints.ddb"

                def run_cli(sql: str) -> dict:
                    proc = subprocess.run(
                        [cli, "exec", "--db", str(db_path), "--sql", sql],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    return json.loads(proc.stdout.strip() or "{}")

                setup = run_cli(
                    "CREATE TABLE accounts ("
                    "id INT PRIMARY KEY, "
                    "amount INT, "
                    "note TEXT, "
                    "CONSTRAINT amount_nonneg CHECK (amount >= 0), "
                    "CHECK (note IS NULL OR LENGTH(note) > 0))"
                )
                self.assertTrue(setup.get("ok"), msg=setup.get("error"))

                for cli_sql, pg_sql in [
                    (
                        "INSERT INTO accounts VALUES (1, 10, 'ok')",
                        f"INSERT INTO {schema}.accounts VALUES (1, 10, 'ok')",
                    ),
                    (
                        "INSERT INTO accounts VALUES (2, NULL, NULL)",
                        f"INSERT INTO {schema}.accounts VALUES (2, NULL, NULL)",
                    ),
                ]:
                    payload = run_cli(cli_sql)
                    self.assertTrue(payload.get("ok"), msg=f"{cli_sql}: {payload.get('error')}")
                    run_psql(pg_sql)

                self.assertTrue(
                    run_psql_expect_error(f"INSERT INTO {schema}.accounts VALUES (3, -1, 'bad')")
                )
                bad_insert = run_cli("INSERT INTO accounts VALUES (3, -1, 'bad')")
                self.assertFalse(bad_insert.get("ok", False))

                self.assertTrue(
                    run_psql_expect_error(f"UPDATE {schema}.accounts SET amount = -5 WHERE id = 1")
                )
                bad_update = run_cli("UPDATE accounts SET amount = -5 WHERE id = 1")
                self.assertFalse(bad_update.get("ok", False))

                pg_rows = run_psql(f"SELECT id, amount, note FROM {schema}.accounts ORDER BY id")
                payload = run_cli("SELECT id, amount, note FROM accounts ORDER BY id")
                self.assertTrue(payload.get("ok"), msg=payload.get("error"))
                self.assertEqual(payload.get("rows", []), pg_rows)
        except RuntimeError as exc:
            self.skipTest(f"PostgreSQL setup failed: {exc}")
        finally:
            try:
                run_psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
