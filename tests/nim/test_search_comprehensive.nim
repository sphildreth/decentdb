import unittest
import search/search
import errors

suite "Search Comprehensive":
  test "trigrams with exactly 3 characters":
    let result = trigrams("ABC")
    check result.len == 1
    # The trigram ABC should equal packTrigram('A', 'B', 'C') which is (65 << 16) | (66 << 8) | 67 = 4259840 + 16896 + 67 = 4276803
    check result[0] == 4276803'u32

  test "trigrams with different characters":
    let result = trigrams("XYZ")
    check result.len == 1
    # The trigram XYZ should equal packTrigram('X', 'Y', 'Z') which is (88 << 16) | (89 << 8) | 90 = 5790042
    check result[0] == 5790042'u32

  test "trigrams with numbers":
    let result = trigrams("123")
    check result.len == 1
    # The trigram 123 should equal packTrigram('1', '2', '3') which is (49 << 16) | (50 << 8) | 51 = 3224115
    check result[0] == 3224115'u32

  test "trigrams with 2 characters returns empty":
    let result = trigrams("AB")
    check result.len == 0

  test "trigrams with 1 character returns empty":
    let result = trigrams("A")
    check result.len == 0

  test "trigrams with empty string returns empty":
    let result = trigrams("")
    check result.len == 0

  test "trigrams with 4 characters returns 2 trigrams":
    let result = trigrams("ABCD")
    check result.len == 2
    check result[0] == 4276803'u32  # ABC
    check result[1] == 4342596'u32  # BCD (66 << 16) | (67 << 8) | 68

  test "trigrams with 5 characters returns 3 trigrams":
    let result = trigrams("ABCDE")
    check result.len == 3
    check result[0] == 4276803'u32  # ABC
    check result[1] == 4342596'u32  # BCD
    check result[2] == 4408389'u32  # CDE (67 << 16) | (68 << 8) | 69

  test "trigrams with lowercase gets canonicalized":
    let result = trigrams("abc")
    check result.len == 1
    check result[0] == 4276803'u32  # Should be converted to uppercase (ABC)

  test "trigrams with mixed case gets canonicalized":
    let result = trigrams("AbC")
    check result.len == 1
    check result[0] == 4276803'u32  # Should be converted to uppercase (ABC)

  test "canonicalize handles empty string":
    check canonicalize("") == ""

  test "canonicalize handles already uppercase":
    check canonicalize("HELLO") == "HELLO"

  test "canonicalize handles mixed case":
    check canonicalize("HeLLo WoRLd") == "HELLO WORLD"

  test "canonicalize handles special characters":
    check canonicalize("test@123#") == "TEST@123#"

  test "encodePostingsSorted with single value":
    let result = encodePostingsSorted(@[42'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[42'u64]

  test "encodePostingsSorted with multiple values":
    let result = encodePostingsSorted(@[10'u64, 20'u64, 30'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[10'u64, 20'u64, 30'u64]

  test "encodePostingsSorted with duplicate values":
    let result = encodePostingsSorted(@[5'u64, 5'u64, 10'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[5'u64, 5'u64, 10'u64]

  test "encodePostingsSorted with large gaps":
    let result = encodePostingsSorted(@[1'u64, 1000000'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[1'u64, 1000000'u64]

  test "encodePostings with empty input":
    let emptySeq: seq[uint64] = @[]
    let result = encodePostings(emptySeq)
    let decoded = decodePostings(result)
    check decoded.ok
    let expected: seq[uint64] = @[]
    check decoded.value == expected

  test "encodePostings with single value":
    let result = encodePostings(@[42'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[42'u64]

  test "encodePostings with unsorted input gets sorted":
    let result = encodePostings(@[30'u64, 10'u64, 20'u64])
    let decoded = decodePostings(result)
    check decoded.ok
    check decoded.value == @[10'u64, 20'u64, 30'u64]

  test "decodePostings with empty input":
    let emptyData: seq[byte] = @[]
    let result = decodePostings(emptyData)
    check result.ok
    let expected: seq[uint64] = @[]
    check result.value == expected

  test "decodePostings with single value":
    let encoded = encodePostings(@[123'u64])
    let result = decodePostings(encoded)
    check result.ok
    check result.value == @[123'u64]

  test "decodePostings with multiple values":
    let original = @[10'u64, 20'u64, 30'u64]
    let encoded = encodePostings(original)
    let result = decodePostings(encoded)
    check result.ok
    check result.value == original

  test "postingsCount with empty data":
    let emptyData: seq[byte] = @[]
    let count = postingsCount(emptyData)
    check count == 0'i32

  test "postingsCount with single value":
    let encoded = encodePostings(@[42'u64])
    let count = postingsCount(encoded)
    check count == 1

  test "postingsCount with multiple values":
    let encoded = encodePostings(@[10'u64, 20'u64, 30'u64])
    let count = postingsCount(encoded)
    check count == 3

  test "postingsCount with decode error returns 0":
    # Invalid varint that will cause decode to fail
    let invalid = @[byte(0xFF), byte(0xFF), byte(0xFF), byte(0xFF), byte(0xFF)]
    let count = postingsCount(invalid)
    check count == 0

  test "addRowid to empty postings":
    let emptyData: seq[byte] = @[]
    let result = addRowid(emptyData, 42'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    let expected = @[42'u64]
    check decoded.value == expected

  test "addRowid to existing postings":
    let original = encodePostings(@[10'u64, 20'u64])
    let result = addRowid(original, 30'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    check 30'u64 in decoded.value
    check decoded.value.len == 3

  test "addRowid with existing value returns same data":
    let original = encodePostings(@[10'u64, 20'u64, 30'u64])
    let result = addRowid(original, 20'u64)
    check result.ok
    check result.value == original  # Should return identical data

  test "addRowid with decode error returns error":
    let invalid = @[byte(0xFF), byte(0xFF)]
    let result = addRowid(invalid, 42'u64)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "removeRowid from empty postings":
    let emptyData: seq[byte] = @[]
    let result = removeRowid(emptyData, 42'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    let expected: seq[uint64] = @[]
    check decoded.value == expected

  test "removeRowid removes existing value":
    let original = encodePostings(@[10'u64, 20'u64, 30'u64])
    let result = removeRowid(original, 20'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    check 20'u64 notin decoded.value
    check decoded.value.len == 2

  test "removeRowid with non-existing value returns unchanged":
    let original = encodePostings(@[10'u64, 20'u64, 30'u64])
    let result = removeRowid(original, 40'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    let expected = @[10'u64, 20'u64, 30'u64]
    check decoded.value == expected

  test "removeRowid with decode error returns error":
    let invalid = @[byte(0xFF), byte(0xFF)]
    let result = removeRowid(invalid, 42'u64)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "intersectPostings with empty input":
    let emptyLists: seq[seq[uint64]] = @[]
    let result = intersectPostings(emptyLists)
    let expected: seq[uint64] = @[]
    check result == expected

  test "intersectPostings with single empty list":
    let emptyList: seq[uint64] = @[]
    let result = intersectPostings(@[emptyList])
    let expected: seq[uint64] = @[]
    check result == expected

  test "intersectPostings with single non-empty list":
    let inputList = @[1'u64, 2'u64, 3'u64]
    let result = intersectPostings(@[inputList])
    check result == @[1'u64, 2'u64, 3'u64]

  test "intersectPostings with two identical lists":
    let list1 = @[1'u64, 2'u64, 3'u64]
    let list2 = @[1'u64, 2'u64, 3'u64]
    let result = intersectPostings(@[list1, list2])
    check result == @[1'u64, 2'u64, 3'u64]

  test "intersectPostings with two different lists with overlap":
    let list1 = @[1'u64, 2'u64, 3'u64]
    let list2 = @[2'u64, 3'u64, 4'u64]
    let result = intersectPostings(@[list1, list2])
    check result == @[2'u64, 3'u64]

  test "intersectPostings with two lists with no overlap":
    let list1 = @[1'u64, 2'u64]
    let list2 = @[3'u64, 4'u64]
    let result = intersectPostings(@[list1, list2])
    let expected: seq[uint64] = @[]
    check result == expected

  test "intersectPostings with multiple lists":
    let list1 = @[1'u64, 2'u64, 3'u64, 4'u64]
    let list2 = @[2'u64, 3'u64, 4'u64, 5'u64]
    let list3 = @[3'u64, 4'u64, 5'u64, 6'u64]
    let result = intersectPostings(@[list1, list2, list3])
    check result == @[3'u64, 4'u64]

  test "intersectPostings with first list empty":
    let emptyList: seq[uint64] = @[]
    let list2 = @[1'u64, 2'u64]
    let result = intersectPostings(@[emptyList, list2])
    let expected: seq[uint64] = @[]
    check result == expected

  test "intersectPostings with middle list empty":
    let list1 = @[1'u64, 2'u64]
    let emptyList: seq[uint64] = @[]
    let list3 = @[2'u64, 3'u64]
    let result = intersectPostings(@[list1, emptyList, list3])
    let expected: seq[uint64] = @[]
    check result == expected

  test "intersectPostings with last list empty":
    let list1 = @[1'u64, 2'u64]
    let list2 = @[2'u64, 3'u64]
    let emptyList: seq[uint64] = @[]
    let result = intersectPostings(@[list1, list2, emptyList])
    let expected: seq[uint64] = @[]
    check result == expected

  test "DefaultPostingsThreshold constant value":
    check DefaultPostingsThreshold == 100000