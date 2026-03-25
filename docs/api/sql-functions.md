# SQL Function Reference

This page documents SQL functions and aggregate/window additions recently implemented in DecentDB.

For broader syntax coverage, see the SQL reference and feature matrix.

## Subquery operators

Supported:

- `EXISTS (subquery)` / `NOT EXISTS (subquery)`
- `expr op ANY (subquery)` and `expr op SOME (subquery)` (`SOME` is a synonym)
- `expr op ALL (subquery)`

Behavior notes:

- Subquery comparison operators support `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`.
- `ANY` returns `TRUE` if at least one comparison is true; `ALL` returns `TRUE` only if all comparisons are true.
- Empty subquery semantics follow SQL quantifier rules: `ANY` yields `FALSE`, `ALL` yields `TRUE`.
- `NULL` comparison propagation follows SQL three-valued logic.

Examples:

```sql
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

SELECT * FROM employees
WHERE salary > ANY (SELECT salary FROM peers);

SELECT * FROM employees
WHERE salary >= ALL (SELECT salary FROM peers);
```

## Regex comparison operators

Supported:

- `left ~ pattern` (case-sensitive match)
- `left ~* pattern` (case-insensitive match)
- `left !~ pattern` (case-sensitive non-match)
- `left !~* pattern` (case-insensitive non-match)

Behavior notes:

- Both operands must be `TEXT`; otherwise an SQL type error is raised.
- `NULL` operands yield `NULL`.
- Invalid regex patterns return an SQL error.

Examples:

```sql
SELECT name FROM users WHERE name ~ '^A';
SELECT name FROM users WHERE name ~* '^admin';
SELECT name FROM users WHERE name !~ 'bot$';
SELECT name FROM users WHERE name !~* '^test_';
```

## Math functions

### Trigonometric

Supported:

- `SIN(x)`
- `COS(x)`
- `TAN(x)`
- `ASIN(x)`
- `ACOS(x)`
- `ATAN(x)`
- `ATAN2(y, x)`
- `PI()`
- `DEGREES(x)`
- `RADIANS(x)`
- `COT(x)`

Behavior notes:

- Numeric inputs are accepted (`INT64`, `FLOAT64`, `DECIMAL`); outputs are `FLOAT64`.
- `ASIN` and `ACOS` return `NULL` for out-of-domain values outside `[-1, 1]`.
- `TAN` returns `NULL` near undefined points (odd multiples of `π/2`).
- `COT` returns `NULL` when `tan(x)` is approximately zero.

Examples:

```sql
SELECT SIN(PI() / 2), COS(0), TAN(PI() / 4);
SELECT ASIN(1), ACOS(0), ATAN2(1, 1);
SELECT DEGREES(PI()), RADIANS(180), COT(PI() / 4);
```

## Conditional functions

Supported:

- `GREATEST(value1, value2, ...)`
- `LEAST(value1, value2, ...)`
- `IIF(condition, then_value, else_value)`

Behavior notes:

- `GREATEST`/`LEAST` return `NULL` if any argument is `NULL`.
- `IIF` follows `CASE`-like behavior and uses DecentDB truthiness semantics for the condition.

Examples:

```sql
SELECT GREATEST(10, 20, 15), LEAST(10, 20, 15);
SELECT IIF(score >= 60, 'pass', 'fail') FROM exams;
```

## Date/time functions

Supported:

- `DATE_TRUNC(precision, timestamp)`
- `DATE_PART(field, timestamp)`
- `DATE_DIFF(part, start, end)`
- `LAST_DAY(timestamp)`
- `NEXT_DAY(timestamp, weekday)`
- `MAKE_DATE(year, month, day)`
- `MAKE_TIMESTAMP(year, month, day, hour, minute, second)`
- `TO_TIMESTAMP(epoch_or_text [, format])`
- `AGE(timestamp [, timestamp])`
- `INTERVAL '...'` (for timestamp arithmetic)

Behavior notes:

- `DATE_TRUNC` supports: microsecond, millisecond, second, minute, hour, day, week, month, quarter, year, decade, century, millennium.
- `TO_TIMESTAMP(text, format)` currently supports formats: `YYYY-MM-DD HH24:MI:SS`, `YYYY-MM-DD`, and `DD/MM/YYYY`.
- `AGE` returns a textual interval (for example, `"1 days 00:00:00"`).
- `INTERVAL` literal parsing supports integer `year/month/week/day/hour/minute/second` units in amount-unit pairs.
- Timestamp interval arithmetic supports `timestamp +/- INTERVAL '...'` and date/timestamp text on the left side.

Examples:

```sql
SELECT DATE_TRUNC('month', '2024-03-15 14:30:45');
SELECT DATE_PART('doy', '2024-03-15');
SELECT DATE_DIFF('day', '2024-03-10', '2024-03-15');
SELECT LAST_DAY('2024-02-11'), NEXT_DAY('2024-03-15', 'Monday');
SELECT MAKE_DATE(2024, 3, 15), MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 0);
SELECT TO_TIMESTAMP(1710505800), TO_TIMESTAMP('15/03/2024', 'DD/MM/YYYY');
SELECT AGE('2024-03-15', '2024-03-14');
SELECT '2024-03-15 14:30:00'::timestamp + INTERVAL '1 day';
```

## String functions

Supported:

- `CONCAT(expr, ...)`
- `CONCAT_WS(separator, expr, ...)`
- `POSITION(substring IN string)`
- `INITCAP(string)`
- `ASCII(string)`
- `REGEXP_REPLACE(string, pattern, replacement [, flags])`
- `SPLIT_PART(string, delimiter, index)`
- `STRING_TO_ARRAY(string, delimiter)`
- `QUOTE_IDENT(string)`
- `QUOTE_LITERAL(string)`
- `MD5(string)`
- `SHA256(string)`

Behavior notes:

- `CONCAT` treats `NULL` arguments as empty strings.
- `CONCAT_WS` skips `NULL` value arguments; `NULL` separator returns `NULL`.
- `POSITION` returns 1-based positions, and `0` if no match exists.
- `REGEXP_REPLACE` supports `g` (global) and `i` (case-insensitive) flags.
- `STRING_TO_ARRAY` returns a JSON text array.

Examples:

```sql
SELECT CONCAT('hello', ' ', 'world');
SELECT CONCAT_WS(', ', 'Alice', NULL, 'Bob');
SELECT POSITION('world' IN 'hello world');
SELECT INITCAP('hello world from decentdb');
SELECT ASCII('A');
SELECT REGEXP_REPLACE('abc123def', '\d', '', 'g');
SELECT SPLIT_PART('a,b,c', ',', 2);
SELECT STRING_TO_ARRAY('a,b,c', ',');
SELECT QUOTE_IDENT('table name'), QUOTE_LITERAL('O''Brien');
SELECT MD5('hello'), SHA256('hello');
```

## Aggregate functions

### Statistical aggregates

Supported:

- `STDDEV(expr)` (alias of `STDDEV_SAMP`)
- `STDDEV_SAMP(expr)`
- `STDDEV_POP(expr)`
- `VARIANCE(expr)` (alias of `VAR_SAMP`)
- `VAR_SAMP(expr)`
- `VAR_POP(expr)`

Behavior notes:

- Implemented using a numerically stable online (Welford-style) accumulation strategy.
- `*_SAMP` forms return `NULL` when fewer than 2 non-`NULL` values exist.
- Population forms return `NULL` for empty input sets.
- `DISTINCT` is supported.

### Boolean aggregates

Supported:

- `BOOL_AND(expr)`
- `BOOL_OR(expr)`

Behavior notes:

- `NULL` inputs are ignored.
- If all values are `NULL`, result is `NULL`.
- Non-boolean non-`NULL` inputs are rejected.

### Collection and ordered-set aggregates

Supported:

- `ARRAY_AGG(expr [ORDER BY ...])`
- `MEDIAN(expr)`
- `PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)`
- `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)`

Behavior notes:

- `ARRAY_AGG` returns JSON text arrays (for example, `"[1,null,2]"`).
- `ARRAY_AGG(DISTINCT ...)` is supported.
- `MEDIAN` returns `FLOAT64` and ignores `NULL` inputs.
- Percentile fraction must be between `0` and `1` inclusive.
- `PERCENTILE_CONT` interpolates and returns `FLOAT64`.
- `PERCENTILE_DISC` returns a value from the ordered input domain.

Examples:

```sql
SELECT STDDEV(amount), VARIANCE(amount), BOOL_AND(amount > 0), BOOL_OR(amount > 100) FROM orders;

SELECT ARRAY_AGG(amount ORDER BY created_at) FROM orders;
SELECT MEDIAN(amount) FROM orders;

SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM orders;
SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY amount) FROM orders;
```

## Window functions

Additional supported window features include:

- `NTILE(n)`
- `PERCENT_RANK()`
- `CUME_DIST()`
- Aggregate window functions such as `SUM(...) OVER (...)`, `COUNT(...) OVER (...)`, `MIN/MAX/AVG/... OVER (...)`
- `ROWS` frame clauses
- `RANGE` frames for `UNBOUNDED`/`CURRENT ROW` style bounds (offset-based `RANGE` bounds are not yet supported)

Examples:

```sql
SELECT id, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM employees;

SELECT id,
       PERCENT_RANK() OVER (ORDER BY score) AS pct_rank,
       CUME_DIST() OVER (ORDER BY score) AS cume_dist
FROM results;

SELECT created_at, amount,
       SUM(amount) OVER (
         ORDER BY created_at
         ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS rolling_sum
FROM orders;
```
