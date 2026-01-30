import unittest
import search/search
import errors
import sequtils
import algorithm

suite "Search Extended":
  test "canonicalize handles empty string":
    check canonicalize("") == ""

  test "canonicalize handles already uppercase":
    check canonicalize("HELLO") == "HELLO"

  test "canonicalize handles mixed case":
    check canonicalize("HeLLo WoRLd") == "HELLO WORLD"

  test "canonicalize handles special characters":
    check canonicalize("test@123#") == "TEST@123#"

  test "trigrams with exact 3 characters":
    let result = trigrams("abc")
    check result.len == 1

  test "trigrams with 4 characters":
    let result = trigrams("abcd")
    check result.len == 2

  test "trigrams with long string":
    let result = trigrams("abcdefghij")
    check result.len == 8

  test "trigrams returns empty for empty string":
    check trigrams("").len == 0

  test "trigrams are case insensitive via canonicalize":
    let t1 = trigrams("ABC")
    let t2 = trigrams("abc")
    check t1 == t2

  test "encodePostings with empty list":
    let encoded = encodePostings(@[])
    check encoded.len == 0
    let decoded = decodePostings(encoded)
    check decoded.ok
    check decoded.value.len == 0

  test "encodePostings with single element":
    let encoded = encodePostings(@[42'u64])
    let decoded = decodePostings(encoded)
    check decoded.ok
    check decoded.value == @[42'u64]

  test "encodePostings with duplicate rowids":
    let encoded = encodePostingsSorted(@[1'u64, 1'u64, 2'u64])
    let decoded = decodePostings(encoded)
    check decoded.ok
    # Delta encoding: 1, 0, 1 -> reconstructs as 1, 1, 2
    check decoded.value.len == 3

  test "encodePostings sorts unsorted input":
    let encoded = encodePostings(@[10'u64, 5'u64, 8'u64])
    let decoded = decodePostings(encoded)
    check decoded.ok
    check decoded.value == @[5'u64, 8'u64, 10'u64]

  test "addRowid to empty postings":
    let added = addRowid(@[], 5'u64)
    check added.ok
    let decoded = decodePostings(added.value)
    check decoded.ok
    check decoded.value == @[5'u64]

  test "addRowid with new rowid":
    let postings = encodePostings(@[1'u64, 2'u64])
    let added = addRowid(postings, 3'u64)
    check added.ok
    let decoded = decodePostings(added.value)
    check decoded.ok
    check 3'u64 in decoded.value
    check decoded.value.len == 3

  test "addRowid with existing rowid returns same":
    let postings = encodePostings(@[1'u64, 2'u64])
    let added = addRowid(postings, 2'u64)
    check added.ok
    check added.value == postings

  test "removeRowid from empty postings":
    let removed = removeRowid(@[], 5'u64)
    check removed.ok
    let decoded = decodePostings(removed.value)
    check decoded.ok
    check decoded.value.len == 0

  test "removeRowid removes existing":
    let postings = encodePostings(@[1'u64, 2'u64, 3'u64])
    let removed = removeRowid(postings, 2'u64)
    check removed.ok
    let decoded = decodePostings(removed.value)
    check decoded.ok
    check 2'u64 notin decoded.value
    check decoded.value == @[1'u64, 3'u64]

  test "removeRowid with non-existent rowid":
    let postings = encodePostings(@[1'u64, 2'u64])
    let removed = removeRowid(postings, 5'u64)
    check removed.ok
    let decoded = decodePostings(removed.value)
    check decoded.ok
    check decoded.value == @[1'u64, 2'u64]

  test "postingsCount with valid data":
    let postings = encodePostings(@[1'u64, 2'u64, 3'u64])
    check postingsCount(postings) == 3

  test "intersectPostings with single list":
    let result = intersectPostings(@[@[1'u64, 2'u64, 3'u64]])
    check result == @[1'u64, 2'u64, 3'u64]

  test "intersectPostings with no overlap":
    let result = intersectPostings(@[@[1'u64, 2'u64], @[3'u64, 4'u64]])
    check result.len == 0

  test "intersectPostings with complete overlap":
    let result = intersectPostings(@[@[1'u64, 2'u64], @[1'u64, 2'u64]])
    check result == @[1'u64, 2'u64]

  test "intersectPostings with partial overlap":
    let result = intersectPostings(@[@[1'u64, 2'u64, 3'u64], @[2'u64, 3'u64, 4'u64]])
    check result == @[2'u64, 3'u64]

  test "intersectPostings optimizes by sorting lists":
    # Small list first should be more efficient
    let result = intersectPostings(@[@[1'u64, 2'u64, 3'u64, 4'u64, 5'u64], @[2'u64], @[2'u64, 3'u64]])
    check result == @[2'u64]

  test "intersectPostings with three lists":
    let result = intersectPostings(@[
      @[1'u64, 2'u64, 3'u64, 4'u64],
      @[2'u64, 3'u64, 4'u64, 5'u64],
      @[3'u64, 4'u64, 5'u64, 6'u64]
    ])
    check result == @[3'u64, 4'u64]

  test "decodePostings with invalid varint":
    # 0x80 means "more bytes coming" but we don't provide them
    let invalid = @[byte(0x80)]
    let result = decodePostings(invalid)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "decodePostings with incomplete data":
    # Start of multi-byte varint but truncated
    let invalid = @[byte(0x80), byte(0x80)]
    let result = decodePostings(invalid)
    check not result.ok

  test "postings with large rowids":
    let largeIds = @[1'u64, 1000'u64, 1000000'u64, uint64.high]
    let encoded = encodePostings(largeIds)
    let decoded = decodePostings(encoded)
    check decoded.ok
    check decoded.value == largeIds

  test "DefaultPostingsThreshold constant":
    check DefaultPostingsThreshold == 100000
