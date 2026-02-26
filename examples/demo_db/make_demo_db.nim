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
- Constraints: PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY (with CASCADE/SET NULL)
- Indexes: BTREE, expression index, partial index, trigram index
- Views, generated columns, triggers

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

  # Seed tags.
  log "Seeding tags"
  let tagNames = @["music", "books", "games", "science", "sports", "news", "tech", "travel"]
  block:
    var values = newSeq[string](tagNames.len)
    for i, t in tagNames:
      values[i] = "(" & $(i + 1) & ", '" & sqlEscape(t) & "')"
    exec(db, "INSERT INTO tags (id, name) VALUES " & values.join(","))

  # Seed users and posts.
  log "Seeding users and posts"
  let progressEvery = max(1, users div 100) # ~1% updates
  let userBatchSize = if users >= 50_000: 1_000 else: 500
  let postBatchSize = if users >= 50_000: 2_000 else: 1_000
  let tagBatchSize = if users >= 50_000: 10_000 else: 5_000

  var insertedUsers = 0
  var insertedPosts = 0
  var insertedPostTags = 0

  var userValues: seq[string] = @[]
  userValues.setLen(0)
  userValues = newSeqOfCap[string](userBatchSize)

  var postValues: seq[string] = @[]
  postValues.setLen(0)
  postValues = newSeqOfCap[string](postBatchSize)

  var postTagValues: seq[string] = @[]
  postTagValues.setLen(0)
  postTagValues = newSeqOfCap[string](tagBatchSize)

  var nextPostId = 1
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
    # UUID is stored as 16-byte BLOB, so use a 0x... literal.
    userRow.add(uuidLit)
    userRow.add(")")

    userValues.add(userRow)
    if userValues.len >= userBatchSize:
      insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)

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
        # FK safety: posts reference users, so ensure referenced users are inserted first.
        insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)
        insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)

      # Assign a couple tags (avoid duplicates so we don't need ON CONFLICT).
      let t1 = 1 + (postId mod tagNames.len)
      let t2 = 1 + ((postId * 3) mod tagNames.len)
      postTagValues.add("(" & $postId & ", " & $t1 & ")")
      if t2 != t1:
        postTagValues.add("(" & $postId & ", " & $t2 & ")")
      if postTagValues.len >= tagBatchSize:
        # FK safety: post_tags reference posts (and tags), so ensure posts are inserted first.
        insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)
        insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)
        insertedPostTags += flushInsert(db, "post_tags", "(post_id, tag_id)", postTagValues)

    if (u mod progressEvery) == 0 or u == users:
      let insertedPosts = nextPostId - 1
      let pct = (u * 100) div users
      log "Progress: users=" & $u & "/" & $users & " (" & $pct & "%) posts=" & $insertedPosts

  insertedUsers += flushInsert(db, "users", "(id, username, email, bio, created_at, external_id)", userValues)
  insertedPosts += flushInsert(db, "posts", "(id, user_id, title, body, created_at, published, rating, payload)", postValues)
  insertedPostTags += flushInsert(db, "post_tags", "(post_id, tag_id)", postTagValues)

  log "Creating indexes (btree / expression / partial / trigram)"
  exec(db, "CREATE INDEX IF NOT EXISTS idx_posts_user_created ON posts(user_id, created_at)")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_lower_username ON users ((LOWER(username)))")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_email_not_null ON users(email) WHERE email IS NOT NULL")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_users_bio_trgm ON users USING trigram(bio)")
  exec(db, "CREATE INDEX IF NOT EXISTS idx_posts_body_trgm ON posts USING trigram(body)")

  log "Creating views"
  exec(db, """
    CREATE OR REPLACE VIEW v_user_post_counts AS
      SELECT u.id AS user_id, u.username, COUNT(p.id) AS post_count
      FROM users u
      LEFT JOIN posts p ON p.user_id = u.id
      GROUP BY u.id, u.username
  """)

  exec(db, """
    CREATE OR REPLACE VIEW v_recent_posts AS
      SELECT p.*, u.username
      FROM posts p
      JOIN users u ON u.id = p.user_id
      WHERE p.created_at >= DATETIME('now', '-7 day')
  """)

  log "Creating trigger"
  exec(db, """
    CREATE TRIGGER users_ins_audit AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg, created_at) VALUES (''user added'', NOW())');
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
