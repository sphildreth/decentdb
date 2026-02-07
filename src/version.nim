import std/os
import std/strutils

func parseNimbleVersion(text: string): string =
  ## Parses `version = "x.y.z"` from a .nimble file.
  for line in text.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed.startsWith("#"):
      continue
    if not trimmed.startsWith("version"):
      continue
    let eqPos = trimmed.find('=')
    if eqPos < 0:
      continue
    let rhs = trimmed[eqPos + 1 .. ^1].strip()
    if rhs.len < 2:
      continue
    let quote = rhs[0]
    if quote != '"' and quote != '\'':
      continue
    let endPos = rhs.find(quote, 1)
    if endPos <= 1:
      continue
    return rhs[1 ..< endPos]
  ""

const NimblePath = joinPath(parentDir(currentSourcePath()), "..", "decentdb.nimble")
const DecentDbVersion* = parseNimbleVersion(staticRead(NimblePath))

static:
  doAssert DecentDbVersion.len > 0, "Failed to parse version from nimble file: " & NimblePath
