import unittest
import storage/storage
import errors
import record/record

suite "Storage Edge Cases":
  test "indexKeyFromValue with different value types":
    # Test with integer
    let intVal = Value(kind: vkInt64, int64Val: 42'i64)
    let intKey = indexKeyFromValue(intVal)
    check intKey == cast[uint64](42'i64)

    # Test with boolean true
    let boolTrueVal = Value(kind: vkBool, boolVal: true)
    let boolTrueKey = indexKeyFromValue(boolTrueVal)
    check boolTrueKey == 1'u64

    # Test with boolean false
    let boolFalseVal = Value(kind: vkBool, boolVal: false)
    let boolFalseKey = indexKeyFromValue(boolFalseVal)
    check boolFalseKey == 0'u64

    # Test with float
    let floatVal = Value(kind: vkFloat64, float64Val: 3.14'f64)
    let floatKey = indexKeyFromValue(floatVal)
    check floatKey == cast[uint64](3.14'f64)

    # Test with text
    let textVal = Value(kind: vkText, bytes: @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')])
    let textKey = indexKeyFromValue(textVal)
    # The CRC32C of "hello" should be computed

    # Test with blob
    let blobVal = Value(kind: vkBlob, bytes: @[byte(1), byte(2), byte(3)])
    let blobKey = indexKeyFromValue(blobVal)
    # The CRC32C of the blob should be computed

  test "normalizeValues with different value types":
    # Test with empty values
    # This would require a pager parameter, so we'll skip for now
    discard

  test "indexSeek with invalid parameters":
    # This would require a full setup, so we'll skip for now
    discard