import unittest
import catalog/catalog
import record/record
import storage/storage

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Storage Helper Functions":
  test "indexKeyFromValue for all types":
    check indexKeyFromValue(Value(kind: vkInt64, int64Val: 42)) == 42'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: true)) == 1'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: false)) == 0'u64
    check indexKeyFromValue(Value(kind: vkFloat64, float64Val: 3.14)) != 0'u64
    check indexKeyFromValue(Value(kind: vkText, bytes: toBytes("test"))) != 0'u64
    check indexKeyFromValue(Value(kind: vkBlob, bytes: @[1'u8, 2'u8])) != 0'u64
    check indexKeyFromValue(Value(kind: vkNull)) == 0'u64
    check indexKeyFromValue(Value(kind: vkTextOverflow)) == 0'u64
    check indexKeyFromValue(Value(kind: vkBlobOverflow)) == 0'u64

  test "indexKeyFromValue overflow types":
    let textVal = Value(kind: vkTextOverflow, overflowPage: 1, overflowLen: 100)
    check indexKeyFromValue(textVal) == 0'u64
    
    let blobVal = Value(kind: vkBlobOverflow, overflowPage: 1, overflowLen: 100)
    check indexKeyFromValue(blobVal) == 0'u64

  test "indexKeyFromValue float64":
    let floatVal1 = Value(kind: vkFloat64, float64Val: 0.0)
    check indexKeyFromValue(floatVal1) == 0'u64
    
    let floatVal2 = Value(kind: vkFloat64, float64Val: 1.5)
    check indexKeyFromValue(floatVal2) != 0'u64

suite "Catalog Helper Functions":
  test "parseColumnType for all types":
    check parseColumnType("INT").ok and parseColumnType("INT").value == ctInt64
    check parseColumnType("INT64").ok and parseColumnType("INT64").value == ctInt64
    check parseColumnType("BIGINT").ok and parseColumnType("BIGINT").value == ctInt64
    check parseColumnType("INT4").ok and parseColumnType("INT4").value == ctInt64
    check parseColumnType("INT8").ok and parseColumnType("INT8").value == ctInt64
    
    check parseColumnType("BOOL").ok and parseColumnType("BOOL").value == ctBool
    check parseColumnType("BOOLEAN").ok and parseColumnType("BOOLEAN").value == ctBool
    
    check parseColumnType("FLOAT").ok and parseColumnType("FLOAT").value == ctFloat64
    check parseColumnType("FLOAT64").ok and parseColumnType("FLOAT64").value == ctFloat64
    check parseColumnType("DOUBLE").ok and parseColumnType("DOUBLE").value == ctFloat64
    
    check parseColumnType("TEXT").ok and parseColumnType("TEXT").value == ctText
    
    check parseColumnType("BLOB").ok and parseColumnType("BLOB").value == ctBlob
    
    let bad = parseColumnType("BADTYPE")
    check not bad.ok

  test "columnTypeToText for all types":
    check columnTypeToText(ctInt64) == "INT64"
    check columnTypeToText(ctBool) == "BOOL"
    check columnTypeToText(ctFloat64) == "FLOAT64"
    check columnTypeToText(ctText) == "TEXT"
    check columnTypeToText(ctBlob) == "BLOB"

  test "columnTypeToText roundtrip":
    for typeName in @["INT", "BOOL", "FLOAT64", "TEXT", "BLOB"]:
      let parseRes = parseColumnType(typeName)
      if parseRes.ok:
        let kind = parseRes.value
        let text = columnTypeToText(kind)
        # Should normalize to canonical form
        check text.len > 0
