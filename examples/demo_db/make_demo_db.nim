import std/[os, strutils, parseopt, random, times]
import ../../src/engine

var sqlStatements = 0

proc log(msg: string) =
  echo "[demo_db] ", msg
  flushFile(stdout)

proc usage(): string =
  result = """
make_demo_db.nim

Creates a demo DecentDB database file with a schema and seed data covering:
- Types: INTEGER/INT64, REAL/FLOAT64, BOOL, DECIMAL, TEXT, BLOB, UUID, DATE/TIMESTAMP (stored as TEXT)
- Constraints: PRIMARY KEY, UNIQUE, NOT NULL, CHECK, DEFAULT, FOREIGN KEY (CASCADE/SET NULL/ON UPDATE CASCADE)
- Indexes: BTREE (composite), expression index, partial index, trigram index, UNIQUE INDEX
- Generated columns (STORED), views (12 showcase views), triggers (AFTER + INSTEAD OF)
- DML: INSERT RETURNING, ON CONFLICT DO NOTHING/DO UPDATE, auto-increment
- Query features in views: GROUP BY, HAVING, ORDER BY, LIMIT, window functions,
    CTEs (non-recursive), aggregates, CASE/COALESCE/NULLIF, set operations,
    subqueries, LIKE/ILIKE, BETWEEN, JSON operators (->/->>), scalar functions
    (string/math/date/UUID), CROSS JOIN, PRINTF
- Transactions: BEGIN/COMMIT, SAVEPOINT/ROLLBACK TO/RELEASE
- Statistics: ANALYZE

Usage:
  # From the repo root:
  nim c -r examples/demo_db/make_demo_db.nim --out=./demo.ddb [--seed=1] [--tiny|--small|--medium|--large|--xlarge] [--users=2000] [--postsPerUser=5]

  # If you're already in examples/demo_db:
  nim c -r make_demo_db.nim --out=./demo.ddb [--seed=1] [--tiny|--small|--medium|--large|--xlarge] [--users=2000] [--postsPerUser=5]

Presets:
  --tiny    (defaults: users=200,    postsPerUser=3)
  --small   (defaults: users=2000,   postsPerUser=5)
  --medium  (defaults: users=10000,  postsPerUser=5)
  --large   (defaults: users=50000,  postsPerUser=5)
  --xlarge  (defaults: users=200000, postsPerUser=5)
  --jumbo   (alias for --xlarge)
"""

proc sqlEscape(s: string): string =
  result = s.replace("'", "''")

proc uuidBytesToHexLiteral(bytes: array[16, byte]): string =
  const hex = "0123456789abcdef"
  var buf = newStringOfCap(3 + 32 + 1)
  buf.add("X'")
  for b in bytes:
    buf.add(hex[int(b shr 4)])
    buf.add(hex[int(b and 0x0F)])
  buf.add("'")
  buf

proc makeDeterministicUuidBytes(r: var Rand; salt: uint64): array[16, byte] =
  # Not RFC4122; just stable test bytes.
  var u: array[16, byte]
  var x = salt
  for i in 0 ..< 16:
    x = x xor (x shl 13)
    x = x xor (x shr 7)
    x = x xor (x shl 17)
    u[i] = byte((x and 0xff'u64))
  u

proc exec(db: Db; sql: string) =
  inc sqlStatements
  let r = execSql(db, sql)
  if not r.ok:
    const maxSqlPreview = 4096
    let sqlPreview = if sql.len <= maxSqlPreview: sql
      else: sql[0 ..< maxSqlPreview] & "\n...[truncated, sqlLen=" & $sql.len & "]"
    quit "SQL failed: " & r.err.message & "\n---\n" & sqlPreview

proc flushInsert(db: Db; tableName: string; columnsSql: string; values: var seq[string]): int =
  if values.len == 0:
    return 0
  let rows = values.len
  var sql = newStringOfCap(64 + columnsSql.len + values.len * 64)
  sql.add("INSERT INTO ")
  sql.add(tableName)
  sql.add(" ")
  sql.add(columnsSql)
  sql.add(" VALUES ")
  sql.add(values.join(","))
  exec(db, sql)
  values.setLen(0)
  rows

when isMainModule:
  let startTime = epochTime()

  var outPath = "demo.ddb"
  var seed: int64 = 1
  var users = 2000
  var postsPerUser = 5
  var usersSet = false
  var postsPerUserSet = false
  var preset: string = ""

  var p = initOptParser(commandLineParams())
  for kind, key, val in p.getopt():
    case kind
    of cmdLongOption, cmdShortOption:
      case key
      of "out": outPath = val
      of "seed": seed = parseBiggestInt(val)
      of "users":
        users = parseInt(val)
        usersSet = true
      of "postsPerUser":
        postsPerUser = parseInt(val)
        postsPerUserSet = true
      of "tiny": preset = "tiny"
      of "small": preset = "small"
      of "medium": preset = "medium"
      of "large": preset = "large"
      of "xlarge": preset = "xlarge"
      of "jumbo": preset = "xlarge"
      of "h", "help":
        echo usage()
        quit 0
      else:
        echo "Unknown option: --", key
        echo usage()
        quit 2
    else:
      discard

  # Apply preset defaults only if user didn't explicitly set the knobs.
  if preset == "tiny":
    if not usersSet: users = 200
    if not postsPerUserSet: postsPerUser = 3
  elif preset == "small":
    if not usersSet: users = 2_000
    if not postsPerUserSet: postsPerUser = 5
  elif preset == "medium":
    if not usersSet: users = 10_000
    if not postsPerUserSet: postsPerUser = 5
  elif preset == "large":
    if not usersSet: users = 50_000
    if not postsPerUserSet: postsPerUser = 5
  elif preset == "xlarge":
    if not usersSet: users = 200_000
    if not postsPerUserSet: postsPerUser = 5

  if users < 1 or postsPerUser < 0:
    quit "Invalid arguments: users must be >= 1 and postsPerUser must be >= 0"

  if fileExists(outPath):
    removeFile(outPath)
  if fileExists(outPath & "-wal"):
    removeFile(outPath & "-wal")

  log "Creating demo database: " & outPath
  log "Config: seed=" & $seed & " users=" & $users & " postsPerUser=" & $postsPerUser

  let openRes = openDb(outPath)
  if not openRes.ok:
    quit "Failed to open db: " & openRes.err.message
  let db = openRes.value

  var rng = initRand(seed)

  # Build everything in one transaction for durability + speed.
  log "BEGIN transaction"
  exec(db, "BEGIN")

  log "Creating schema (tables / views / trigger)"
  exec(db, """
    CREATE TABLE IF NOT EXISTS demo_types (
      id INTEGER PRIMARY KEY,
      int64_val INTEGER,
      float64_val REAL,
      bool_val BOOL,
      dec_val DECIMAL(18,6),
      text_val TEXT,
      blob_val BLOB,
      uuid_val UUID,
      date_val DATE,
      ts_val TIMESTAMP,
      json_val TEXT,
      text_len INTEGER GENERATED ALWAYS AS (LENGTH(text_val)) STORED,
      CHECK (int64_val IS NULL OR int64_val >= 0)
    )
  """)

  # Hierarchical table — self-referencing FK with ON DELETE SET NULL.
  exec(db, """
    CREATE TABLE IF NOT EXISTS categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      parent_id INTEGER REFERENCES categories(id) ON DELETE SET NULL ON UPDATE CASCADE,
      UNIQUE(name)
    )
  """)

  # Settings table — showcases DEFAULT values and UNIQUE INDEX.
  exec(db, """
    CREATE TABLE IF NOT EXISTS settings (
      id INTEGER PRIMARY KEY,
      key TEXT NOT NULL,
      value TEXT DEFAULT '',
      enabled BOOL DEFAULT TRUE,
      ref_id UUID DEFAULT GEN_RANDOM_UUID(),
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(key)
    )
  """)

  exec(db, """
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      username TEXT NOT NULL,
      email TEXT,
      bio TEXT,
      created_at TIMESTAMP NOT NULL,
      external_id UUID NOT NULL,
      UNIQUE(username)
    )
  """)

  exec(db, """
    CREATE TABLE IF NOT EXISTS posts (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL,
      published BOOL NOT NULL,
      rating DECIMAL(10,2),
      payload BLOB,
      CHECK (user_id > 0)
    )
  """)

  exec(db, """
    CREATE TABLE IF NOT EXISTS tags (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      UNIQUE(name)
    )
  """)

  exec(db, """
    CREATE TABLE IF NOT EXISTS post_tags (
      post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
      tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
      PRIMARY KEY (post_id, tag_id),
      CHECK (post_id > 0),
      CHECK (tag_id > 0)
    )
  """)

  exec(db, """
    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY,
      msg TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL
    )
  """)

  # Clear existing rows so regeneration is idempotent.
  log "Clearing existing rows (idempotent regeneration)"
  exec(db, "DELETE FROM post_tags")
  exec(db, "DELETE FROM posts")
  exec(db, "DELETE FROM users")
  exec(db, "DELETE FROM tags")
  exec(db, "DELETE FROM audit_log")
  exec(db, "DELETE FROM demo_types")
  exec(db, "DELETE FROM categories")
  exec(db, "DELETE FROM settings")

  # demo_types: a few rows hitting NULLs and edge cases.
  exec(db, """
    INSERT INTO demo_types (id, int64_val, float64_val, bool_val, dec_val, text_val, blob_val, uuid_val, date_val, ts_val, json_val)
    VALUES
      (1, 0, 0.0, FALSE, 0.000001, 'hello', X'00010203', UUID_PARSE('00112233-4455-6677-8899-aabbccddeeff'), CURRENT_DATE, CURRENT_TIMESTAMP,
       '{"kind":"demo","tags":["a","b"],"n":1}'),
      (2, 9223372036854775807, 3.14159, TRUE, 12345.670000, 'unicode π', X'deadbeef', GEN_RANDOM_UUID(), DATE('now'), NOW(),
       '{"kind":"demo","tags":[],"n":2}'),
      (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
  """)

  # Seed categories — hierarchical data for recursive CTE demos.
  log "Seeding categories"
  exec(db, """
    INSERT INTO categories (id, name, parent_id) VALUES
      (1, 'Electronics', NULL),
      (2, 'Computers', 1),
      (3, 'Laptops', 2),
      (4, 'Desktops', 2),
      (5, 'Phones', 1),
      (6, 'Smartphones', 5),
      (7, 'Clothing', NULL),
      (8, 'Mens', 7),
      (9, 'Womens', 7),
      (10, 'Books', NULL)
  """)

  # Seed settings — exercise DEFAULT values and auto-increment (omit PK).
  log "Seeding settings (DEFAULT + auto-increment)"
  exec(db, "INSERT INTO settings (key, value) VALUES ('theme', 'dark')")
  exec(db, "INSERT INTO settings (key) VALUES ('locale')")

  # Exercise INSERT … RETURNING (auto-increment returns the new id).
  exec(db, "INSERT INTO settings (key, value) VALUES ('timezone', 'UTC') RETURNING id")

  # Exercise INSERT … ON CONFLICT DO NOTHING.
  exec(db, "INSERT INTO settings (key, value) VALUES ('theme', 'light') ON CONFLICT (key) DO NOTHING")

  # Exercise INSERT … ON CONFLICT DO UPDATE (upsert).
  exec(db, """
    INSERT INTO settings (key, value, enabled) VALUES ('locale', 'en-US', TRUE)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, enabled = EXCLUDED.enabled
  """)

  # Seed tags.
  log "Seeding tags"
  let tagNames = @["music", "books", "games", "science", "sports", "news", "tech", "travel"]
  block:
    var values = newSeq[string](tagNames.len)
    for i, t in tagNames:
      values[i] = "(" & $(i + 1) & ", '" & sqlEscape(t) & "')"
    exec(db, "INSERT INTO tags (id, name) VALUES " & values.join(","))

  # Seed users first (FK parents must exist before children).
  log "Seeding users"
  let progressEvery = max(1, users div 100) # ~1% updates
  let userBatchSize = if users >= 50_000: 1_000 else: 500
  let postBatchSize = if users >= 50_000: 2_000 else: 1_000
  let tagBatchSize = if users >= 50_000: 10_000 else: 5_000

  var insertedUsers = 0
  var insertedPosts = 0
  var insertedPostTags = 0

  var userValues = newSeqOfCap[string](userBatchSize)

  for u in 1 .. users:
    let username = "user_" & $u
    let email = if (u mod 7) == 0: "" else: username & "@example.com"
    let bio = "I like music, books, and travel. user=" & $u & " seed=" & $seed

    let uuidBytes = makeDeterministicUuidBytes(rng, uint64(u))
    let uuidLit = uuidBytesToHexLiteral(uuidBytes)

    let createdAt = "2026-01-" & align($((u mod 28) + 1), 2, '0') & " 12:" & align($((u mod 60)), 2, '0') & ":00"

    var userRow = newStringOfCap(256)
    userRow.add("(")
    userRow.add($u)
    userRow.add(", '")
    userRow.add(sqlEscape(username))
    userRow.add("', ")
    if email.len == 0:
      userRow.add("NULL")
    else:
      userRow.add("'")
      userRow.add(sqlEscape(email))
      userRow.add("'")
    userRow.add(", '")
    userRow.add(sqlEscape(bio))
    userRow.add("', '")
    userRow.add(createdAt)
    userRow.add("', ")
    userRow.add(uuidLit)
    userRow.add(")")

    userValues.add(userRow)
    if userValues.len >= userBatchSize:
      insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)

    if (u mod progressEvery) == 0 or u == users:
      let pct = (u * 100) div users
      log "Progress: users=" & $u & "/" & $users & " (" & $pct & "%)"

  insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)

  # Seed posts and tags (all FK parent users already exist).
  log "Seeding posts and tags"
  var postValues = newSeqOfCap[string](postBatchSize)
  var postTagValues = newSeqOfCap[string](tagBatchSize)

  var nextPostId = 1
  for u in 1 .. users:
    let username = "user_" & $u
    let createdAt = "2026-01-" & align($((u mod 28) + 1), 2, '0') & " 12:" & align($((u mod 60)), 2, '0') & ":00"

    for pidx in 0 ..< postsPerUser:
      let postId = nextPostId
      inc nextPostId
      let published = ((postId mod 3) != 0)
      let title = "Post " & $postId & " by " & username
      let body = "This is a demo post body containing searchable phrases like decentdb and dbeaver. id=" & $postId
      let rating = (postId mod 100).float / 10.0
      let ratingStr = formatFloat(rating, ffDecimal, 2)
      let payload = "X'" & repeat("ab", (postId mod 16) + 1) & "'"

      var postRow = newStringOfCap(512)
      postRow.add("(")
      postRow.add($postId)
      postRow.add(", ")
      postRow.add($u)
      postRow.add(", '")
      postRow.add(sqlEscape(title))
      postRow.add("', '")
      postRow.add(sqlEscape(body))
      postRow.add("', '")
      postRow.add(createdAt)
      postRow.add("', ")
      postRow.add(if published: "TRUE" else: "FALSE")
      postRow.add(", ")
      postRow.add(ratingStr)
      postRow.add(", ")
      postRow.add(payload)
      postRow.add(")")
      postValues.add(postRow)
      if postValues.len >= postBatchSize:
        insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)

      # Assign a couple tags (avoid duplicates so we don't need ON CONFLICT).
      let t1 = 1 + (postId mod tagNames.len)
      let t2 = 1 + ((postId * 3) mod tagNames.len)
      postTagValues.add("(" & $postId & ", " & $t1 & ")")
      if t2 != t1:
        postTagValues.add("(" & $postId & ", " & $t2 & ")")
      if postTagValues.len >= tagBatchSize:
        insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)
        insertedPostTags += flushInsert(db, "post_tags", "(post_id, tag_id)", postTagValues)

    if (u mod progressEvery) == 0 or u == users:
      let totalPosts = nextPostId - 1
      let pct = (u * 100) div users
      log "Progress: posts=" & $totalPosts & " (" & $pct & "%)"

  insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)
  insertedPostTags += flushInsert(db, "post_tags", "(post_id, tag_id)", postTagValues)

  log "Creating indexes (btree / expression / partial / trigram / unique)"
  exec(db, "CREATE INDEX IF NOT EXISTS idx_posts_user_created ON posts(user_id, created_at)")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_lower_username ON users ((LOWER(username)))")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_email_not_null ON users(email) WHERE email IS NOT NULL")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_bio_trgm ON users USING trigram(bio)")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_posts_body_trgm ON posts USING trigram(body)")
  exec(db, "CREATE UNIQUE INDEX IF NOT EXISTS idx_settings_key ON settings(key)")

  # ── Views ──────────────────────────────────────────────────────────
  # Views with GROUP BY/HAVING/ORDER BY/LIMIT are supported via subquery
  # wrapping.  All views below are queryable from tools like DBeaver.
  log "Creating views"

  # View — INNER JOIN + WHERE + scalar functions.
  exec(db, """
    CREATE OR REPLACE VIEW v_recent_posts AS
      SELECT p.id, p.title, p.created_at, p.published, p.rating, u.username
      FROM posts p
      JOIN users u ON u.id = p.user_id
      WHERE p.published = TRUE
  """)

  # View — LEFT JOIN.
  exec(db, """
    CREATE OR REPLACE VIEW v_user_posts AS
      SELECT u.id AS user_id, u.username, u.email, p.id AS post_id, p.title, p.rating
      FROM users u
      LEFT JOIN posts p ON p.user_id = u.id
  """)

  # View — window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD.
  exec(db, """
    CREATE OR REPLACE VIEW v_post_rankings AS
      SELECT
        p.id,
        p.user_id,
        p.title,
        p.rating,
        ROW_NUMBER() OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS rn,
        RANK()       OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS rnk,
        DENSE_RANK() OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS dense_rnk,
        LAG(p.rating, 1)  OVER (PARTITION BY p.user_id ORDER BY p.id) AS prev_rating,
        LEAD(p.rating, 1) OVER (PARTITION BY p.user_id ORDER BY p.id) AS next_rating
      FROM posts p
  """)

  # View — FIRST_VALUE / LAST_VALUE / NTH_VALUE window functions.
  exec(db, """
    CREATE OR REPLACE VIEW v_user_post_window AS
      SELECT
        p.user_id,
        p.id AS post_id,
        p.rating,
        FIRST_VALUE(p.title) OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS top_title,
        LAST_VALUE(p.title)  OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS bottom_title,
        NTH_VALUE(p.title, 2) OVER (PARTITION BY p.user_id ORDER BY p.rating DESC) AS second_title
      FROM posts p
  """)

  # View — CTE (non-recursive) + CASE + COALESCE + NULLIF.
  exec(db, """
    CREATE OR REPLACE VIEW v_user_status AS
      WITH enriched AS (
        SELECT u.id, u.username, u.email, u.bio, u.created_at
        FROM users u
        WHERE u.email IS NOT NULL
      )
      SELECT id, username,
             COALESCE(email, 'no-email') AS email,
             NULLIF(bio, '') AS bio_or_null,
             CASE WHEN id <= 50 THEN 'early'
                  WHEN id <= 150 THEN 'mid'
                  ELSE 'late' END AS cohort
      FROM enriched
  """)

  # View — scalar functions: string, math, date, UUID, JSON, concat.
  exec(db, """
    CREATE OR REPLACE VIEW v_function_showcase AS
      SELECT
        d.id,
        UPPER(d.text_val) AS upper_text,
        LOWER(d.text_val) AS lower_text,
        TRIM(d.text_val) AS trimmed,
        LENGTH(d.text_val) AS text_len,
        LEFT(d.text_val, 3) AS left3,
        RIGHT(d.text_val, 3) AS right3,
        LPAD(CAST(d.id AS TEXT), 5, '0') AS padded_id,
        RPAD(COALESCE(d.text_val, ''), 10, '.') AS rpadded,
        REPEAT('*', d.id) AS stars,
        REVERSE(COALESCE(d.text_val, '')) AS reversed,
        REPLACE(COALESCE(d.text_val, ''), 'hello', 'world') AS replaced,
        SUBSTRING(COALESCE(d.text_val, ''), 1, 3) AS substr3,
        INSTR(COALESCE(d.text_val, ''), 'l') AS instr_l,
        CHR(65 + d.id) AS chr_val,
        HEX(d.id) AS hex_id,
        d.text_val || ' [id=' || CAST(d.id AS TEXT) || ']' AS concatenated,
        ABS(COALESCE(d.int64_val, -1)) AS abs_val,
        ROUND(COALESCE(d.float64_val, 0.0), 2) AS rounded,
        CEIL(COALESCE(d.float64_val, 0.0)) AS ceiled,
        FLOOR(COALESCE(d.float64_val, 0.0)) AS floored,
        SIGN(COALESCE(d.int64_val, 0)) AS sign_val,
        SQRT(ABS(COALESCE(d.float64_val, 1.0))) AS sqrt_val,
        POWER(2, d.id) AS power_val,
        MOD(COALESCE(d.int64_val, 0), 7) AS mod_val,
        UUID_TO_STRING(d.uuid_val) AS uuid_str,
        EXTRACT(YEAR FROM d.ts_val) AS ts_year,
        STRFTIME('%Y-%m-%d', COALESCE(d.ts_val, '2026-01-01 00:00:00')) AS ts_formatted,
        JSON_EXTRACT(d.json_val, '$.kind') AS json_kind,
        JSON_ARRAY_LENGTH(JSON_EXTRACT(d.json_val, '$.tags')) AS json_tags_count,
        JSON_TYPE(d.json_val) AS json_type_val,
        JSON_VALID(COALESCE(d.json_val, '')) AS json_is_valid
      FROM demo_types d
      WHERE d.id <= 2
  """)

  # View — JSON operators (-> / ->>), json_object, json_array, PRINTF.
  exec(db, """
    CREATE OR REPLACE VIEW v_json_ops AS
      SELECT
        d.id,
        d.json_val ->> 'kind' AS kind_text,
        d.json_val -> 'tags' AS tags_json,
        JSON_OBJECT('id', d.id, 'text', d.text_val) AS constructed_obj,
        JSON_ARRAY(d.id, d.text_val, d.bool_val) AS constructed_arr,
        PRINTF('row %d: %s', d.id, COALESCE(d.text_val, 'NULL')) AS formatted
      FROM demo_types d
      WHERE d.json_val IS NOT NULL
  """)

  # View — set operations: UNION ALL.
  exec(db, """
    CREATE OR REPLACE VIEW v_set_operations AS
      SELECT id, username AS name, 'user' AS source FROM users WHERE id <= 5
      UNION ALL
      SELECT id, name, 'tag' AS source FROM tags
  """)

  # View — subquery, EXISTS, IN, BETWEEN, LIKE, ILIKE.
  exec(db, """
    CREATE OR REPLACE VIEW v_query_predicates AS
      SELECT u.id, u.username, u.email
      FROM users u
      WHERE EXISTS (SELECT 1 FROM posts p WHERE p.user_id = u.id AND p.published = TRUE)
        AND u.id IN (SELECT user_id FROM posts WHERE rating BETWEEN 1.0 AND 5.0)
        AND u.username LIKE 'user_%'
        AND u.bio ILIKE '%music%'
  """)

  # View — CROSS JOIN.
  exec(db, """
    CREATE OR REPLACE VIEW v_tag_pairs AS
      SELECT t1.name AS tag1, t2.name AS tag2
      FROM tags t1
      CROSS JOIN tags t2
      WHERE t1.id < t2.id AND t1.id <= 3
  """)

  # Views with GROUP BY / HAVING / aggregates (now supported via subquery wrapping).
  exec(db, """
    CREATE OR REPLACE VIEW v_user_post_counts AS
      SELECT u.id, u.username,
             COUNT(p.id) AS post_count,
             COUNT(DISTINCT p.published) AS distinct_states,
             SUM(p.rating) AS total_rating,
             AVG(p.rating) AS avg_rating,
             MIN(p.rating) AS min_rating,
             MAX(p.rating) AS max_rating,
             TOTAL(p.rating) AS total_fn,
             GROUP_CONCAT(CAST(p.id AS TEXT), ',') AS post_ids
      FROM users u
      JOIN posts p ON p.user_id = u.id
      GROUP BY u.id, u.username
      HAVING COUNT(p.id) >= 2
  """)

  # View with ORDER BY + LIMIT.
  exec(db, """
    CREATE OR REPLACE VIEW v_top_users AS
      SELECT u.id, u.username, COUNT(p.id) AS post_count
      FROM users u
      JOIN posts p ON p.user_id = u.id
      GROUP BY u.id, u.username
      ORDER BY post_count DESC
      LIMIT 10
  """)

  # ── Direct queries that exercise features beyond views ───────────
  # DISTINCT ON, recursive CTEs, EXCEPT/INTERSECT, OFFSET/FETCH,
  # FULL OUTER JOIN.
  log "Exercising advanced queries (DISTINCT ON, recursive CTEs, set ops)"

  # DISTINCT ON.
  exec(db, """
    SELECT DISTINCT ON (p.user_id) p.user_id, p.id, p.title, p.created_at
    FROM posts p
    ORDER BY p.user_id, p.created_at DESC
  """)

  # Recursive CTE: category tree traversal.
  exec(db, """
    WITH RECURSIVE tree(id, name, parent_id, lvl, path) AS (
      SELECT id, name, parent_id, 0, name
      FROM categories
      WHERE parent_id IS NULL
      UNION ALL
      SELECT c.id, c.name, c.parent_id, t.lvl + 1, t.path || ' > ' || c.name
      FROM categories c
      JOIN tree t ON c.parent_id = t.id
    )
    SELECT id, name, parent_id, lvl, path FROM tree
  """)

  # Recursive CTE: generate series 1..5.
  exec(db, """
    WITH RECURSIVE cnt(x) AS (
      SELECT 1
      UNION ALL
      SELECT x + 1 FROM cnt WHERE x < 5
    )
    SELECT x FROM cnt
  """)

  # EXCEPT and INTERSECT set operations.
  exec(db, "SELECT id FROM tags EXCEPT SELECT id FROM tags WHERE id <= 2")
  exec(db, "SELECT id FROM tags WHERE id <= 5 INTERSECT SELECT id FROM tags WHERE id >= 3")

  # OFFSET/FETCH (SQL:2008 pagination syntax).
  exec(db, "SELECT id, username FROM users ORDER BY id OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")

  # FULL OUTER JOIN.
  exec(db, """
    SELECT t.name, pt.post_id
    FROM tags t
    FULL OUTER JOIN post_tags pt ON pt.tag_id = t.id
    WHERE t.id <= 3
  """)

  # ── Triggers ──────────────────────────────────────────────────────
  log "Creating triggers"
  exec(db, """
    CREATE TRIGGER users_ins_audit AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg, created_at) VALUES (''user added'', NOW())');
  """)

  # INSTEAD OF trigger — makes v_user_posts insertable (for demo purposes).
  exec(db, """
    CREATE TRIGGER v_user_posts_insert INSTEAD OF INSERT ON v_user_posts
    FOR EACH ROW
    EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg, created_at) VALUES (''view insert intercepted'', NOW())');
  """)

  # Seed a small audit trail so tooling has something to display without waiting
  # for interactive inserts.
  exec(db, "INSERT INTO audit_log (msg, created_at) VALUES ('seed audit row', NOW())")

  # Demonstrate savepoints are supported (no-op, but exercises the parser/executor).
  exec(db, "SAVEPOINT demo_sp")
  exec(db, "ROLLBACK TO SAVEPOINT demo_sp")
  exec(db, "RELEASE SAVEPOINT demo_sp")

  log "COMMIT transaction"
  exec(db, "COMMIT")

  # ANALYZE runs outside the transaction to populate query planner statistics.
  log "Running ANALYZE"
  exec(db, "ANALYZE")

  discard closeDb(db)
  log "Created demo DB: " & outPath
  log "Users: " & $users & "  Posts: " & $(users * postsPerUser)

  let elapsed = epochTime() - startTime
  let rowOps = float(insertedUsers + insertedPosts + insertedPostTags)
  let stmtOps = float(sqlStatements)
  log "Stats: elapsed=" & formatFloat(elapsed, ffDecimal, 3) & "s" &
      " rowOps=" & $(int(rowOps)) &
      " (" & formatFloat(rowOps / max(elapsed, 0.000001), ffDecimal, 1) & "/s)" &
      " stmts=" & $sqlStatements &
      " (" & formatFloat(stmtOps / max(elapsed, 0.000001), ffDecimal, 1) & "/s)"
