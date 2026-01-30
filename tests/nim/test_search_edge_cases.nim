import unittest
import search/search
import errors

suite "Search Edge Cases":
  test "trigrams with exactly 3 characters":
    let result = trigrams("ABC")
    check result.len == 1

  test "trigrams with 2 characters":
    let result = trigrams("AB")
    check result.len == 0  # Less than 3 chars should return empty

  test "trigrams with 1 character":
    let result = trigrams("A")
    check result.len == 0  # Less than 3 chars should return empty

  test "trigrams with empty string":
    let result = trigrams("")
    check result.len == 0

  test "encodePostingsSorted with empty input":
    let result = encodePostingsSorted(@[])
    check result.len == 0

  test "encodePostingsSorted with single element":
    let result = encodePostingsSorted(@[42'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[42'u64]

  test "encodePostingsSorted with multiple elements":
    let result = encodePostingsSorted(@[10'u64, 20'u64, 30'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[10'u64, 20'u64, 30'u64]

  test "encodePostingsSorted with duplicate elements":
    let result = encodePostingsSorted(@[5'u64, 5'u64, 10'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[5'u64, 5'u64, 10'u64]

  test "postingsCount with empty data":
    let count = postingsCount(@[])
    check count == 0

  test "postingsCount with invalid data":
    let count = postingsCount(@[byte(0xFF), byte(0xFF), byte(0xFF)])
    check count == 0  # Should return 0 when decoding fails

  test "addRowid with invalid data":
    let result = addRowid(@[byte(0xFF), byte(0xFF)], 42'u64)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "removeRowid with invalid data":
    let result = removeRowid(@[byte(0xFF), byte(0xFF)], 42'u64)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "intersectPostings with empty list":
    let result = intersectPostings(@[])
    check result.len == 0

  test "intersectPostings with one empty list":
    let emptyList: seq[uint64] = @[]
    let result = intersectPostings(@[emptyList])
    check result.len == 0

  test "intersectPostings with no overlap":
    let result = intersectPostings(@[@[1'u64], @[2'u64]])  # Lists with no overlap
    check result.len == 0