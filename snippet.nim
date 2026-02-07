
       of ctInt64:
          case valRes.value.kind
          of vkInt64: return ok(valRes.value)
          of vkFloat64: return ok(Value(kind: vkInt64, int64Val: int64(valRes.value.float64Val)))
          of vkText:
             try:
               let i = parseBiggestInt(valueToString(valRes.value).strip())
               return ok(Value(kind: vkInt64, int64Val: i))
             except ValueError:
               return err[Value](ERR_SQL, "Invalid integer format")
          of vkDecimal:
             let res = scaleDecimal(valRes.value.int64Val, valRes.value.decimalScale, 0)
             if not res.ok: return err[Value](res.err.code, res.err.message)
             return ok(Value(kind: vkInt64, int64Val: res.value))
          of vkBool:
             return ok(Value(kind: vkInt64, int64Val: if valRes.value.boolVal: 1 else: 0))
          else:
             return err[Value](ERR_SQL, "Cannot cast to INT", $valRes.value.kind)

       of ctFloat64:
          case valRes.value.kind
          of vkFloat64: return ok(valRes.value)
          of vkInt64: return ok(Value(kind: vkFloat64, float64Val: float64(valRes.value.int64Val)))
          of vkDecimal:
             var f = float64(valRes.value.int64Val)
             var divS = 1.0
             for _ in 1 .. valRes.value.decimalScale: divS *= 10.0
             return ok(Value(kind: vkFloat64, float64Val: f / divS))
          of vkText:
             try:
               let f = parseFloat(valueToString(valRes.value).strip())
               return ok(Value(kind: vkFloat64, float64Val: f))
             except ValueError:
               return err[Value](ERR_SQL, "Invalid float format")
          of vkBool:
             return ok(Value(kind: vkFloat64, float64Val: if valRes.value.boolVal: 1.0 else: 0.0))
          else:
             return err[Value](ERR_SQL, "Cannot cast to FLOAT", $valRes.value.kind)
