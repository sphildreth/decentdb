version       = "0.0.1"
author        = "DecentDb contributors"
description   = "DecentDb engine (pre-alpha)"
license       = "Apache-2.0"
srcDir        = "src"
bin           = @["decentdb_cli"]

task test, "Run Nim + Python unit tests":
  exec "nim c -r tests/nim/test_faulty_vfs.nim"
  exec "nim c -r tests/nim/test_db_header.nim"
  exec "nim c -r tests/nim/test_pager.nim"
  exec "python -m unittest tests/harness/test_runner.py"

task test_nim, "Run Nim unit tests":
  exec "nim c -r tests/nim/test_faulty_vfs.nim"
  exec "nim c -r tests/nim/test_db_header.nim"
  exec "nim c -r tests/nim/test_pager.nim"

task test_py, "Run Python harness tests":
  exec "python -m unittest tests/harness/test_runner.py"

task lint, "Static checks for Nim + Python":
  exec "nim check src/decentdb_cli.nim"
  exec "nim check src/engine.nim"
  exec "nim check src/vfs/vfs.nim"
  exec "nim check src/vfs/os_vfs.nim"
  exec "nim check src/vfs/faulty_vfs.nim"
  exec "nim check src/pager/db_header.nim"
  exec "nim check src/pager/pager.nim"
  exec "nim check tests/nim/test_faulty_vfs.nim"
  exec "nim check tests/nim/test_db_header.nim"
  exec "nim check tests/nim/test_pager.nim"
  exec "python -m compileall tests/harness"
