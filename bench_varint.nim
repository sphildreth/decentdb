import std/monotimes
import std/times

# Self-contained types to mimic DecentDB structures without imports
type ErrorCode* = int
const ERR_CORRUPTION* = 100

type Error* = object
  code*: ErrorCode
  message*: string
  context*: string

type Result*[T] = object
  ok*: bool
  value*: T
  err*: Error

proc ok*[T](val: T): Result[T] = Result[T](ok: true, value: val)
proc err*[T](code: ErrorCode, msg: string, ctx: string = ""): Result[T] = 
  Result[T](ok: false, err: Error(code: code, message: msg, context: ctx))

# Original slow implementation style
proc decodeVarintSlow*(data: string, offset: var int): Result[uint64] =
  var shift = 0
  var value: uint64 = 0
  while offset < data.len:
    let b = byte(data[offset])
    offset.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      return ok(value)
    shift += 7
    if shift > 63:
      return err[uint64](ERR_CORRUPTION, "Varint overflow")
  err[uint64](ERR_CORRUPTION, "Unexpected end of varint")

# Fast implementation using primitive return
proc decodeVarintFast*(data: string, offset: var int, valOut: var uint64): bool =
  var shift = 0
  var value: uint64 = 0
  let L = data.len
  while offset < L:
    let b = byte(data[offset])
    offset.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      valOut = value
      return true
    shift += 7
    if shift > 63:
      return false
  return false

proc bench() =
  var data = ""
  # Encode some varints
  for i in 0..1000:
    for val in [1'u64, 127'u64, 128'u64, 16384'u64, 1234567890'u64, 0xFFFFFFFFFFFFFFFF'u64]:
      var v = val
      while true:
        var b = byte(v and 0x7F)
        v = v shr 7
        if v != 0: b = b or 0x80
        data.add(char(b))
        if v == 0: break
  
  let Iterations = 10_000
  
  echo "Benchmarking decodeVarint (" & $Iterations & " iters)..."
  
  # Warmup
  var offset = 0
  var dummy: uint64
  discard decodeVarintFast(data, offset, dummy)

  # Bench Slow
  var tStart = getMonoTime()
  var sumSlow = 0'u64
  for i in 0 ..< Iterations:
    offset = 0
    while offset < data.len:
      let res = decodeVarintSlow(data, offset)
      if res.ok:
        sumSlow += res.value
      else:
        break
  let tSlow = getMonoTime() - tStart
  
  # Bench Fast
  tStart = getMonoTime()
  var sumFast = 0'u64
  for i in 0 ..< Iterations:
    offset = 0
    var val: uint64
    while offset < data.len:
      if decodeVarintFast(data, offset, val):
        sumFast += val
      else:
        break
  let tFast = getMonoTime() - tStart
  
  echo "Slow Time: " & $(float(tSlow.inNanoseconds) / 1_000_000.0) & " ms"
  echo "Fast Time: " & $(float(tFast.inNanoseconds) / 1_000_000.0) & " ms"
  echo "Improvement: " & $(float(tSlow.inNanoseconds) / float(tFast.inNanoseconds)) & "x"
  echo "Sum Check: " & $sumSlow & " vs " & $sumFast

  # Bench Mock Closure Overhead
  echo "\nBenchmarking Closure Overhead (" & $Iterations & " iters)..."
  
  proc withPage(body: proc(p: string): bool): bool =
    body(data)

  tStart = getMonoTime()
  var cSum = 0
  for i in 0 ..< Iterations:
    discard withPage(proc(p: string): bool =
      cSum += p.len
      return true
    )
  let tClosure = getMonoTime() - tStart
  
  echo "Closure Time: " & $(float(tClosure.inNanoseconds) / 1_000_000.0) & " ms"
  echo "Ops/Sec: " & $(float(Iterations) / (float(tClosure.inNanoseconds) / 1_000_000_000.0))

bench()
