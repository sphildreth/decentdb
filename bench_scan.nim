import src/record/record
import src/pager/db_header
import std/monotimes
import std/times
import std/random

proc bench() =
  # Mock a page with 17 keys
  var page = newString(4096)
  var offset = 8
  
  # Fill with 17 keys and children
  # Key: varint 1 byte
  # Child: varint 1 byte
  for i in 0 ..< 17:
    page[offset] = char(0x01) # key=1
    offset.inc
    page[offset] = char(0x01) # child=1
    offset.inc
  
  let count = 17
  let searchKey = 10'u64 # Search for key > 10 (none, so scans all)
  
  let iterations = 1_000_000
  let tStart = cpuTime()
  
  var totalScanned = 0
  
  for _ in 1 .. iterations:
    var off = 8
    var found = false
    # next loop simulates findLeaf scan
    for i in 0 ..< count:
      var k: uint64
      # decode key
      if not decodeVarintFast(page, off, k): quit("err k")
      
      var c: uint64
      # decode child
      if not decodeVarintFast(page, off, c): quit("err c")
      
      if k > searchKey:
        found = true
        break
    totalScanned += count
    
  let duration = cpuTime() - tStart
  echo "Iterations: ", iterations
  echo "Total Time: ", duration, " s"
  echo "Time per Op: ", (duration / float(iterations) * 1_000_000), " us"

bench()
