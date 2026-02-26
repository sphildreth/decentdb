## datetime.nim — helpers for the native TIMESTAMP (vkDateTime) type.
##
## Storage unit: int64 microseconds since Unix epoch UTC.
## This matches PostgreSQL's internal timestamp representation.

import std/times
import std/strutils
import ../errors

proc datetimeToMicros*(dt: DateTime): int64 =
  ## Convert a DateTime (assumed UTC) to microseconds since Unix epoch.
  let t = dt.toTime()
  t.toUnix() * 1_000_000 + int64(t.nanosecond) div 1_000

proc microsToDatetime*(micros: int64): DateTime =
  ## Convert microseconds since Unix epoch to a UTC DateTime.
  let secs = micros div 1_000_000
  let ns = int((micros mod 1_000_000) * 1_000)
  initTime(secs, ns).utc()

proc formatDatetimeMicros*(micros: int64): string =
  ## Format as "YYYY-MM-DD HH:MM:SS[.ffffff]" (no timezone suffix).
  let dt = microsToDatetime(micros)
  let base = dt.format("yyyy-MM-dd HH:mm:ss")
  let us = int(micros mod 1_000_000)
  let usAbs = if us < 0: us + 1_000_000 else: us
  if usAbs == 0:
    base
  else:
    base & "." & align($usAbs, 6, '0')

proc parseDatetimeMicros*(input: string): Result[int64] =
  ## Parse an ISO-8601 datetime string into microseconds since Unix epoch UTC.
  ## Accepts: "YYYY-MM-DD HH:MM:SS[.ffffff]", "YYYY-MM-DD",
  ##          "YYYY-MM-DDTHH:MM:SS[.ffffff]", "HH:MM:SS".
  var s = input.strip()
  var micros = 0i64
  let dotPos = s.rfind('.')
  if dotPos >= 0:
    let fracStr = s[dotPos + 1 .. ^1]
    s = s[0 ..< dotPos]
    var fracPadded = fracStr
    while fracPadded.len < 6:
      fracPadded.add('0')
    try:
      micros = int64(parseInt(fracPadded[0..5]))
    except ValueError:
      return err[int64](ERR_SQL, "Invalid datetime fractional seconds", input)

  let formats = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd",
    "HH:mm:ss",
  ]
  for fmt in formats:
    try:
      let dt = parse(s, fmt, utc())
      return ok(datetimeToMicros(dt) + micros)
    except ValueError:
      discard
  err[int64](ERR_SQL, "Cannot parse datetime", input)
