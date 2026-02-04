import src/pager/pager
import src/pager/db_header
import src/vfs/os_vfs
import src/vfs/types
import src/errors
import std/monotimes
import std/times
import std/os

proc bench() =
  let path = "/tmp/bench_pager_test.db"
  if fileExists(path): removeFile(path)
  
  let vfs = newOsVfs()
  
  let openRes = vfs.open(path, fmReadWrite, true)
  if not openRes.ok: quit("Failed to create file: " & openRes.err.message)
  var f = openRes.value
  
  # Initialize header
  let header = DbHeader(
    formatVersion: FormatVersion,
    pageSize: 4096,
    schemaCookie: 0,
    rootCatalog: 0,
    rootFreelist: 0,
    freelistHead: 0,
    freelistCount: 0,
    lastCheckpointLsn: 0
  )
  let headerBufArray = encodeHeader(header)
  var headerBuf = newString(headerBufArray.len)
  copyMem(addr headerBuf[0], unsafeAddr headerBufArray[0], headerBufArray.len)
  # But we need 4096 bytes page.
  var pageBuf = newString(4096)
  if headerBuf.len > 0:
    copyMem(addr pageBuf[0], unsafeAddr headerBuf[0], headerBuf.len)
  
  let writeRes = vfs.writeStr(f, 0, pageBuf)
  if not writeRes.ok: quit("Write failed")
  
  # Create page 1 (root) -- write same buffer to page 1
  let writeRes2 = vfs.writeStr(f, 4096, pageBuf)
  if not writeRes2.ok: quit("Write root failed")
  
  let closeRes = vfs.close(f)
  if not closeRes.ok: quit("Close failed")
  
  # Re-open
  let openRes2 = vfs.open(path, fmReadWrite, false) # create=false
  if not openRes2.ok: quit("Failed to open file")
  f = openRes2.value
    
  let pagerRes = newPager(vfs, f)
  if not pagerRes.ok:
    quit("Failed to open pager: " & pagerRes.err.message)
  let pager = pagerRes.value
  
  let Iterations = 1_000_000
  echo "Benchmarking withPageRo (" & $Iterations & " iterations)..."
  
  let tStart = getMonoTime()
  var dummy = 0
  for i in 0 ..< Iterations:
    let res = pager.withPageRo(1'u32, proc(p: string): Result[Void] =
      dummy += p.len
      okVoid()
    )
    if not res.ok: quit("Read failed")
    
  let tEnd = getMonoTime()
  let duration = tEnd - tStart
  let ns = duration.inNanoseconds
  let usPerOp = float(ns) / float(Iterations) / 1000.0
  
  echo "Total Time: " & $(float(ns) / 1_000_000_000.0) & " s"
  echo "Time per Op: " & $usPerOp & " us"
  echo "Ops/Sec: " & $(1_000_000_000.0 / (float(ns) / float(Iterations)))

  discard closePager(pager)
  removeFile(path)

bench()
