import unittest
import json

import sql/sql

proc makeNode(): JsonNode =
  newJObject()

suite "SQL Helpers":
  test "node helpers handle nested strings":
    var root = makeNode()
    root["name"] = %"example"
    root["nested"] = %*{"String": %*{"str": "inner"}}

    check nodeHas(root, "name")
    check not nodeHas(root, "missing")
    check nodeGet(root, "missing").kind == JNull
    check nodeString(root["name"]) == "example"
    check nodeString(root["nested"]) == "inner"
    check nodeStringOr(root, "missing", "fallback") == "fallback"

  test "parseAConst covers integer, string, float, and null paths":
    let nilNode = %*{"isnull": true}
    let nilRes = parseAConst(nilNode)
    check nilRes.ok
    check nilRes.value.value.kind == svNull

    let intNode = %*{"ival": 42}
    let intRes = parseAConst(intNode)
    check intRes.ok
    check intRes.value.value.kind == svInt
    check intRes.value.value.intVal == 42

    let strNode = %*{"sval": %*{"sval": "payload"}}
    let strRes = parseAConst(strNode)
    check strRes.ok
    check strRes.value.value.kind == svString
    check strRes.value.value.strVal == "payload"

    let floatNode = %*{"fval": %*{"fval": "3.14"}}
    let floatRes = parseAConst(floatNode)
    check floatRes.ok
    check floatRes.value.value.kind == svFloat
    check abs(floatRes.value.value.floatVal - 3.14) < 0.001

    let valNode = %*{"val": %*{"String": %*{"str": "json"}}}
    let valRes = parseAConst(valNode)
    check valRes.ok
    check valRes.value.value.kind == svString
    check valRes.value.value.strVal == "json"
