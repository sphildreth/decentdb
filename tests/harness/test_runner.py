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


if __name__ == "__main__":
    unittest.main()
