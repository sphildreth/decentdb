import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:decentdb/decentdb.dart';

const _categoryCount = 100;
const _customerCount = 1000;
const _productCount = 5000;
const _orderCount = 10000;
const _reviewCount = 10000;
const _pointReads = 1000;
const _fetchBatchSize = 4096;

String findNativeLib() {
  final env = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (env != null && env.isNotEmpty) return env;
  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final name in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
      'target/release/libdecentdb.so',
      'target/release/libdecentdb.dylib',
      'target/release/decentdb.dll',
    ]) {
      final file = File('${dir.path}/$name');
      if (file.existsSync()) return file.path;
    }
    dir = dir.parent;
  }
  throw StateError('Cannot find native library. Set DECENTDB_NATIVE_LIB.');
}

double percentile(List<double> sorted, int pct) {
  if (sorted.isEmpty) return 0;
  final idx = ((pct / 100.0) * (sorted.length - 1)).round();
  return sorted[idx.clamp(0, sorted.length - 1)];
}

String formatNum(num value) {
  final s = value.toStringAsFixed(0);
  final buf = StringBuffer();
  var count = 0;
  for (var i = s.length - 1; i >= 0; i--) {
    if (count == 3) {
      buf.write(',');
      count = 0;
    }
    buf.write(s[i]);
    count++;
  }
  return buf.toString().split('').reversed.join();
}

class _Metrics {
  final _data = <String, num>{};
  final _meta = <String, dynamic>{};

  void put(String key, num value) => _data[key] = value;
  void meta(String key, dynamic value) => _meta[key] = value;

  void writeJson(String path) {
    final out = <String, dynamic>{
      'timestamp': DateTime.now().toUtc().toIso8601String(),
      ..._meta,
      'metrics': _data,
    };
    File(path).writeAsStringSync(json.encode(out));
  }
}

void main(List<String> args) {
  var scaleFactor = 1;
  var keepDb = false;
  var dbPath = 'dart_complex_demo.ddb';
  var jsonPath = 'bench_result.json';

  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--count':
        scaleFactor = int.parse(args[++i]);
        if (scaleFactor < 1) throw ArgumentError('--count must be >= 1');
      case '--keep-db':
        keepDb = true;
      case '--db-path':
        dbPath = args[++i];
      case '--json':
        jsonPath = args[++i];
      case '-h':
      case '--help':
        print(
          'Usage: dart run ../examples/console_complex/main.dart [options]',
        );
        print('');
        print('Options:');
        print(
          '  --count <n>    Multiply base data volumes by n (default: 1 = 56,100 rows)',
        );
        print('  --keep-db      Persist the database file');
        print(
          '  --db-path <p>  Custom file path (default: dart_complex_demo.ddb)',
        );
        print(
          '  --json <path>  JSON output path (default: bench_result.json)',
        );
        print('  -h, --help     Show help');
        return;
    }
  }

  final catCount = _categoryCount * scaleFactor;
  final custCount = _customerCount * scaleFactor;
  final prodCount = _productCount * scaleFactor;
  final ordCount = _orderCount * scaleFactor;
  final revCount = _reviewCount * scaleFactor;
  final totalRows = catCount + custCount + prodCount + ordCount + revCount;

  final libPath = findNativeLib();
  final usePath = keepDb ? dbPath : ':memory:';
  final db = Database.open(usePath, libraryPath: libPath);
  final m = _Metrics();

  m.meta('engine_version', db.engineVersion);
  m.meta('database', usePath);
  m.meta('scale_factor', scaleFactor);
  m.meta('total_target_rows', totalRows);

  print('=== DecentDB Dart Complex Demo ===');
  print('Engine version: ${db.engineVersion}');
  print('Database: $usePath');
  print('Scale: ${scaleFactor}x ($totalRows target rows)');
  print('');

  // ── Section 1: DDL Schema Creation ──────────────────────────────
  print('--- Schema Creation ---');
  final ddlWatch = Stopwatch()..start();

  db.execute('''
    CREATE TABLE categories (
      id INT64 PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      description TEXT
    )''');
  db.execute('''
    CREATE TABLE customers (
      id INT64 PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      city TEXT,
      created_at TEXT
    )''');
  db.execute('''
    CREATE TABLE products (
      id INT64 PRIMARY KEY,
      name TEXT NOT NULL,
      price FLOAT64 NOT NULL,
      category_id INT64 NOT NULL REFERENCES categories(id),
      stock INT64 NOT NULL DEFAULT 0
    )''');
  db.execute('''
    CREATE TABLE orders (
      id INT64 PRIMARY KEY,
      customer_id INT64 NOT NULL REFERENCES customers(id),
      order_date TEXT NOT NULL,
      total FLOAT64 NOT NULL DEFAULT 0.0
    )''');
  db.execute('''
    CREATE TABLE order_items (
      id INT64 PRIMARY KEY,
      order_id INT64 NOT NULL REFERENCES orders(id),
      product_id INT64 NOT NULL REFERENCES products(id),
      quantity INT64 NOT NULL,
      unit_price FLOAT64 NOT NULL
    )''');
  db.execute('''
    CREATE TABLE reviews (
      id INT64 PRIMARY KEY,
      product_id INT64 NOT NULL REFERENCES products(id),
      customer_id INT64 NOT NULL REFERENCES customers(id),
      rating INT64 NOT NULL,
      comment TEXT
    )''');

  db.execute('CREATE INDEX idx_customers_email ON customers (email)');
  db.execute('CREATE INDEX idx_customers_city ON customers (city)');
  db.execute('CREATE INDEX idx_products_category ON products (category_id)');
  db.execute('CREATE INDEX idx_products_name ON products (name)');
  db.execute('CREATE INDEX idx_orders_customer ON orders (customer_id)');
  db.execute('CREATE INDEX idx_orders_date ON orders (order_date)');
  db.execute('CREATE INDEX idx_order_items_order ON order_items (order_id)');
  db.execute(
    'CREATE INDEX idx_order_items_product ON order_items (product_id)',
  );
  db.execute('CREATE INDEX idx_reviews_product ON reviews (product_id)');

  ddlWatch.stop();
  final ddlMs = ddlWatch.elapsedMicroseconds / 1000.0;
  m.put('ddl_ms', ddlMs);
  print('  6 tables, 9 indexes created in ${ddlMs}ms');
  print('');

  // ── Section 2: Bulk Insert ──────────────────────────────────────
  print('--- Bulk Insert ---');
  final rng = Random(42);

  final insertTimings = <String, double>{};
  final insertRows = <String, int>{};

  // 2a. Categories
  var watch = Stopwatch()..start();
  final catStmt = db.prepare(r'INSERT INTO categories VALUES ($1, $2, $3)');
  try {
    db.begin();
    try {
      for (var i = 0; i < catCount; i++) {
        catStmt.reset();
        catStmt.clearBindings();
        catStmt.bindAll(<Object?>[
          i,
          'category_$i',
          'Description for category $i with some longer text content to simulate realistic data',
        ]);
        catStmt.execute();
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    }
  } finally {
    catStmt.dispose();
  }
  watch.stop();
  insertTimings['categories'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['categories'] = catCount;

  // 2b. Customers
  watch = Stopwatch()..start();
  final custStmt = db.prepare(
    r'INSERT INTO customers VALUES ($1, $2, $3, $4, $5)',
  );
  final cities = [
    'New York',
    'Los Angeles',
    'Chicago',
    'Houston',
    'Phoenix',
    'Philadelphia',
    'San Antonio',
    'San Diego',
    'Dallas',
    'Austin',
    'Seattle',
    'Denver',
    'Boston',
    'Miami',
    'Portland',
  ];
  try {
    db.begin();
    try {
      for (var i = 0; i < custCount; i++) {
        custStmt.reset();
        custStmt.clearBindings();
        custStmt.bindAll(<Object?>[
          i,
          'Customer_${i.toString().padLeft(6, '0')}',
          'user$i@example.com',
          cities[i % cities.length],
          '2024-${((i % 12) + 1).toString().padLeft(2, '0')}-${((i % 28) + 1).toString().padLeft(2, '0')}',
        ]);
        custStmt.execute();
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    }
  } finally {
    custStmt.dispose();
  }
  watch.stop();
  insertTimings['customers'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['customers'] = custCount;

  // 2c. Products
  watch = Stopwatch()..start();
  final prodStmt = db.prepare(
    r'INSERT INTO products VALUES ($1, $2, $3, $4, $5)',
  );
  try {
    db.begin();
    try {
      for (var i = 0; i < prodCount; i++) {
        prodStmt.reset();
        prodStmt.clearBindings();
        prodStmt.bindAll(<Object?>[
          i,
          'Product_${i.toString().padLeft(6, '0')}_Widget',
          (rng.nextDouble() * 990.0 + 10.0),
          rng.nextInt(catCount),
          rng.nextInt(500),
        ]);
        prodStmt.execute();
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    }
  } finally {
    prodStmt.dispose();
  }
  watch.stop();
  insertTimings['products'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['products'] = prodCount;

  // 2d. Orders + Order Items
  watch = Stopwatch()..start();
  final orderStmt = db.prepare(r'INSERT INTO orders VALUES ($1, $2, $3, $4)');
  final itemStmt = db.prepare(
    r'INSERT INTO order_items VALUES ($1, $2, $3, $4, $5)',
  );
  var itemId = 0;
  try {
    db.begin();
    try {
      for (var i = 0; i < ordCount; i++) {
        final custId = rng.nextInt(custCount);
        final month = ((i % 12) + 1).toString().padLeft(2, '0');
        final day = ((i % 28) + 1).toString().padLeft(2, '0');
        final lineCount = 1 + rng.nextInt(5);

        final items = <List<Object?>>[];
        var orderTotal = 0.0;
        for (var j = 0; j < lineCount; j++) {
          final prodId = rng.nextInt(prodCount);
          final qty = 1 + rng.nextInt(10);
          final price = (rng.nextDouble() * 990.0 + 10.0);
          orderTotal += qty * price;
          items.add(<Object?>[itemId + j, i, prodId, qty, price]);
        }

        orderStmt.reset();
        orderStmt.clearBindings();
        orderStmt.bindAll(<Object?>[i, custId, '2024-$month-$day', orderTotal]);
        orderStmt.execute();

        for (final item in items) {
          itemStmt.reset();
          itemStmt.clearBindings();
          itemStmt.bindAll(item);
          itemStmt.execute();
        }
        itemId += lineCount;
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    }
  } finally {
    orderStmt.dispose();
    itemStmt.dispose();
  }
  watch.stop();
  insertTimings['orders'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['orders'] = ordCount;
  insertTimings['order_items'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['order_items'] = itemId;

  // 2e. Reviews
  watch = Stopwatch()..start();
  final revStmt = db.prepare(
    r'INSERT INTO reviews VALUES ($1, $2, $3, $4, $5)',
  );
  final comments = [
    'Excellent product, highly recommend!',
    'Good value for money.',
    'Average quality, meets expectations.',
    'Could be better, some issues found.',
    'Outstanding! Will buy again.',
    'Not what I expected but functional.',
    'Perfect for my needs.',
    'Decent product with minor flaws.',
    'Five stars, no complaints.',
    'Below average, disappointed.',
  ];
  try {
    db.begin();
    try {
      for (var i = 0; i < revCount; i++) {
        revStmt.reset();
        revStmt.clearBindings();
        revStmt.bindAll(<Object?>[
          i,
          rng.nextInt(prodCount),
          rng.nextInt(custCount),
          1 + rng.nextInt(5),
          comments[i % comments.length],
        ]);
        revStmt.execute();
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    }
  } finally {
    revStmt.dispose();
  }
  watch.stop();
  insertTimings['reviews'] = watch.elapsedMicroseconds / 1000000.0;
  insertRows['reviews'] = revCount;

  var totalInsertSeconds = 0.0;
  var totalInsertRows = 0;
  for (final entry in insertTimings.entries) {
    final secs = entry.value;
    final rows = insertRows[entry.key]!;
    totalInsertSeconds += secs;
    totalInsertRows += rows;
    final rps = rows / secs;
    m.put('insert_${entry.key}_rows', rows);
    m.put('insert_${entry.key}_s', secs);
    m.put('insert_${entry.key}_rps', rps);
    print(
      '  ${entry.key.padRight(14)} ${formatNum(rows).padLeft(8)} rows in '
      '${secs.toStringAsFixed(2).padLeft(8)}s (${formatNum(rps.round())} rows/sec)',
    );
  }
  final totalRps = totalInsertRows / totalInsertSeconds;
  m.put('insert_total_rows', totalInsertRows);
  m.put('insert_total_s', totalInsertSeconds);
  m.put('insert_total_rps', totalRps);
  print(
    '  ${"TOTAL".padRight(14)} ${formatNum(totalInsertRows).padLeft(8)} rows in '
    '${totalInsertSeconds.toStringAsFixed(2).padLeft(8)}s (${formatNum(totalRps.round())} rows/sec)',
  );
  print('');

  // ── Section 3: Fetchall & Streaming ─────────────────────────────
  print('--- Fetchall & Streaming ---');
  db.query('SELECT COUNT(*) FROM products');

  watch = Stopwatch()..start();
  final allProducts = db.query(
    'SELECT id, name, price, category_id, stock FROM products',
  );
  watch.stop();
  final fetchallMs = watch.elapsedMicroseconds / 1000.0;
  m.put('fetchall_ms', fetchallMs);
  m.put('fetchall_rows', allProducts.length);
  print('  Fetchall (${allProducts.length} products): ${fetchallMs}ms');

  final streamStmt = db.prepare(
    'SELECT id, name, price, category_id, stock FROM products',
  );
  var streamCount = 0;
  try {
    watch = Stopwatch()..start();
    while (true) {
      final page = streamStmt.nextPage(_fetchBatchSize);
      streamCount += page.rows.length;
      if (page.isLast) break;
    }
    watch.stop();
  } finally {
    streamStmt.dispose();
  }
  final streamingMs = watch.elapsedMicroseconds / 1000.0;
  m.put('streaming_ms', streamingMs);
  m.put('streaming_rows', streamCount);
  print(
    '  Streaming (batch=$_fetchBatchSize, $streamCount rows): ${streamingMs}ms',
  );
  print('');

  // ── Section 4: Point Reads by ID ────────────────────────────────
  print('--- Point Reads by ID (${_pointReads}x on products) ---');
  final pointStmt = db.prepare(
    r'SELECT id, name, price FROM products WHERE id = $1',
  );
  final pointIds = List<int>.generate(prodCount, (i) => i);
  for (var i = 0; i < _pointReads; i++) {
    final j = i + rng.nextInt(prodCount - i);
    final tmp = pointIds[i];
    pointIds[i] = pointIds[j];
    pointIds[j] = tmp;
  }

  final latencies = List<double>.filled(_pointReads, 0.0);
  try {
    pointStmt.reset();
    pointStmt.clearBindings();
    pointStmt.bindAll(<Object?>[pointIds[0]]);
    pointStmt.query();

    for (var i = 0; i < _pointReads; i++) {
      final sw = Stopwatch()..start();
      pointStmt.reset();
      pointStmt.clearBindings();
      pointStmt.bindAll(<Object?>[pointIds[i]]);
      final rows = pointStmt.query();
      sw.stop();
      if (rows.isEmpty) throw StateError('Point read missed id=${pointIds[i]}');
      latencies[i] = sw.elapsedMicroseconds / 1000.0;
    }
  } finally {
    pointStmt.dispose();
  }
  latencies.sort();
  final p50 = percentile(latencies, 50);
  final p95 = percentile(latencies, 95);
  final p99 = percentile(latencies, 99);
  m.put('point_read_p50_ms', p50);
  m.put('point_read_p95_ms', p95);
  m.put('point_read_p99_ms', p99);
  print(
    '  p50=${p50}ms  p95=${p95}ms  p99=${p99}ms',
  );
  print('');

  // ── Section 5: Indexed Lookup (email) ───────────────────────────
  print('--- Indexed Lookup (email) ---');
  final emailStmt = db.prepare(
    r'SELECT id, name, city FROM customers WHERE email = $1',
  );
  final emailLatencies = List<double>.filled(_pointReads, 0.0);
  try {
    for (var i = 0; i < _pointReads; i++) {
      final id = rng.nextInt(custCount);
      final sw = Stopwatch()..start();
      emailStmt.reset();
      emailStmt.clearBindings();
      emailStmt.bindAll(<Object?>['user$id@example.com']);
      final rows = emailStmt.query();
      sw.stop();
      if (rows.isEmpty) {
        throw StateError('Email lookup missed user$id@example.com');
      }
      emailLatencies[i] = sw.elapsedMicroseconds / 1000.0;
    }
  } finally {
    emailStmt.dispose();
  }
  emailLatencies.sort();
  final emailP50 = percentile(emailLatencies, 50);
  final emailP95 = percentile(emailLatencies, 95);
  m.put('email_lookup_p50_ms', emailP50);
  m.put('email_lookup_p95_ms', emailP95);
  print('  p50=${emailP50}ms  p95=${emailP95}ms');
  print('');

  // ── Section 6: 4-Table Join ─────────────────────────────────────
  print('--- 4-Table Join (orders + customers + order_items + products) ---');
  watch = Stopwatch()..start();
  final joinRows = db.query('''
    SELECT o.id AS order_id, c.name AS customer, p.name AS product,
           oi.quantity, oi.unit_price, o.order_date
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    JOIN order_items oi ON oi.order_id = o.id
    JOIN products p ON p.id = oi.product_id
    ORDER BY o.id
    LIMIT 10000
  ''');
  watch.stop();
  final joinMs = watch.elapsedMicroseconds / 1000.0;
  m.put('join_4table_ms', joinMs);
  m.put('join_4table_rows', joinRows.length);
  print('  Result: ${joinRows.length} rows in ${joinMs}ms');
  print('');

  // ── Section 7: Inner Join + Aggregation (reviewed products) ─────
  print('--- Inner Join + Aggregation (reviewed products) ---');
  watch = Stopwatch()..start();
  final ratingRows = db.query('''
    SELECT p.name, p.price, c.name AS category,
           AVG(r.rating) AS avg_rating,
           COUNT(r.id) AS review_count
    FROM reviews r
    JOIN products p ON p.id = r.product_id
    JOIN categories c ON c.id = p.category_id
    GROUP BY p.id, p.name, p.price, c.name
    ORDER BY avg_rating DESC, review_count DESC
    LIMIT 20
  ''');
  watch.stop();
  final ratingMs = watch.elapsedMicroseconds / 1000.0;
  m.put('join_agg_rating_ms', ratingMs);
  m.put('join_agg_rating_rows', ratingRows.length);
  print('  Result: ${ratingRows.length} rows in ${ratingMs}ms');
  for (var i = 0; i < min(5, ratingRows.length); i++) {
    final r = ratingRows[i];
    print(
      '    ${r['name']} (${r['category']}): avg=${(r['avg_rating'] as num).toStringAsFixed(2)}, reviews=${r['review_count']}',
    );
  }
  print('');

  // ── Section 8: Aggregations ─────────────────────────────────────
  print('--- Aggregations ---');

  watch = Stopwatch()..start();
  final revByCust = db.query('''
    SELECT c.name, COUNT(o.id) AS order_count, SUM(o.total) AS total_spent
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    GROUP BY c.id, c.name
    ORDER BY total_spent DESC
    LIMIT 10
  ''');
  watch.stop();
  final custSpendMs = watch.elapsedMicroseconds / 1000.0;
  m.put('agg_customer_spend_ms', custSpendMs);
  print('  Top 10 customers by spend: ${custSpendMs}ms');
  for (var i = 0; i < min(5, revByCust.length); i++) {
    final r = revByCust[i];
    print(
      '    ${r['name']}: ${r['order_count']} orders, \$${(r['total_spent'] as num).toStringAsFixed(2)}',
    );
  }

  watch = Stopwatch()..start();
  final catStats = db.query('''
    SELECT c.name, COUNT(p.id) AS product_count, AVG(p.price) AS avg_price, SUM(p.stock) AS total_stock
    FROM categories c
    JOIN products p ON p.category_id = c.id
    GROUP BY c.id, c.name
    ORDER BY product_count DESC
    LIMIT 10
  ''');
  watch.stop();
  final catStatsMs = watch.elapsedMicroseconds / 1000.0;
  m.put('agg_category_stats_ms', catStatsMs);
  print('  Top 10 categories by product count: ${catStatsMs}ms');
  for (var i = 0; i < min(5, catStats.length); i++) {
    final r = catStats[i];
    print(
      '    ${r['name']}: ${r['product_count']} products, avg \$${(r['avg_price'] as num).toStringAsFixed(2)}, stock=${r['total_stock']}',
    );
  }

  watch = Stopwatch()..start();
  final topProducts = db.query('''
    SELECT p.name, COUNT(r.id) AS review_count, AVG(r.rating) AS avg_rating
    FROM products p
    JOIN reviews r ON r.product_id = p.id
    GROUP BY p.id, p.name
    ORDER BY review_count DESC
    LIMIT 10
  ''');
  watch.stop();
  final topProdMs = watch.elapsedMicroseconds / 1000.0;
  m.put('agg_top_products_ms', topProdMs);
  print('  Top 10 products by review count: ${topProdMs}ms');
  for (var i = 0; i < min(5, topProducts.length); i++) {
    final r = topProducts[i];
    print(
      '    ${r['name']}: ${r['review_count']} reviews, avg rating ${(r['avg_rating'] as num).toStringAsFixed(2)}',
    );
  }
  print('');

  // ── Section 9: Text Search ──────────────────────────────────────
  print('--- Text Search (LIKE) ---');
  watch = Stopwatch()..start();
  final searchRows = db.query(
    r'''
    SELECT id, name, price FROM products
    WHERE name LIKE '%' || $1 || '%'
    ORDER BY price DESC
  ''',
    <Object?>['Widget'],
  );
  watch.stop();
  final searchMs = watch.elapsedMicroseconds / 1000.0;
  m.put('text_search_contains_ms', searchMs);
  m.put('text_search_contains_matches', searchRows.length);
  print('  LIKE "%Widget%": ${searchRows.length} matches in ${searchMs}ms');

  watch = Stopwatch()..start();
  final custSearch = db.query(
    r'''
    SELECT id, name, city FROM customers
    WHERE name LIKE $1
    ORDER BY name
    LIMIT 100
  ''',
    <Object?>['Customer_000%'],
  );
  watch.stop();
  final prefixMs = watch.elapsedMicroseconds / 1000.0;
  m.put('text_search_prefix_ms', prefixMs);
  m.put('text_search_prefix_matches', custSearch.length);
  print(
      '  LIKE "Customer_000%": ${custSearch.length} matches in ${prefixMs}ms');
  print('');

  // ── Section 10: CTE / Subqueries ────────────────────────────────
  print('--- CTE / Subqueries ---');

  watch = Stopwatch()..start();
  final aboveAvg = db.query('''
    WITH cust_totals AS (
      SELECT customer_id, SUM(total) AS spent
      FROM orders
      GROUP BY customer_id
    )
    SELECT c.name, ct.spent
    FROM cust_totals ct
    JOIN customers c ON c.id = ct.customer_id
    WHERE ct.spent > (SELECT AVG(spent) FROM cust_totals)
    ORDER BY ct.spent DESC
    LIMIT 10
  ''');
  watch.stop();
  final aboveAvgMs = watch.elapsedMicroseconds / 1000.0;
  m.put('cte_above_avg_spend_ms', aboveAvgMs);
  print(
      '  Customers above avg spend: ${aboveAvg.length} rows in ${aboveAvgMs}ms');

  watch = Stopwatch()..start();
  final topOrders = db.query('''
    WITH order_details AS (
      SELECT o.id AS order_id, c.name AS customer, o.total,
             COUNT(oi.id) AS item_count
      FROM orders o
      JOIN customers c ON c.id = o.customer_id
      JOIN order_items oi ON oi.order_id = o.id
      GROUP BY o.id, c.name, o.total
    )
    SELECT od.order_id, od.customer, od.total, od.item_count
    FROM order_details od
    WHERE od.total > (SELECT AVG(total) FROM orders)
    ORDER BY od.total DESC
    LIMIT 10
  ''');
  watch.stop();
  final topOrdersMs = watch.elapsedMicroseconds / 1000.0;
  m.put('cte_top_orders_ms', topOrdersMs);
  print(
      '  Orders above avg total: ${topOrders.length} rows in ${topOrdersMs}ms');
  for (var i = 0; i < min(5, topOrders.length); i++) {
    final r = topOrders[i];
    print(
      '    Order ${r['order_id']}: ${r['customer']}, \$${(r['total'] as num).toStringAsFixed(2)}, ${r['item_count']} items',
    );
  }

  watch = Stopwatch()..start();
  final monthlyTotals = db.query('''
    SELECT SUBSTR(order_date, 1, 7) AS month,
           COUNT(*) AS order_count,
           SUM(total) AS revenue
    FROM orders
    GROUP BY SUBSTR(order_date, 1, 7)
    ORDER BY SUBSTR(order_date, 1, 7)
  ''');
  watch.stop();
  final monthlyMs = watch.elapsedMicroseconds / 1000.0;
  m.put('cte_monthly_totals_ms', monthlyMs);
  print(
      '  Monthly order totals: ${monthlyTotals.length} months in ${monthlyMs}ms');
  print('');

  // ── Section 11: Transaction (new order + rollback demo) ─────────
  print('--- Transaction Demo ---');

  watch = Stopwatch()..start();
  final beforeStock = db.query(
    r'SELECT stock FROM products WHERE id = $1',
    <Object?>[0],
  );
  final stockBefore = beforeStock.first['stock'] as int;

  db.begin();
  db.executeWithParams(
    r'INSERT INTO orders VALUES ($1, $2, $3, $4)',
    <Object?>[ordCount + 1000, 0, '2025-01-01', 299.97],
  );
  db.executeWithParams(
    r'INSERT INTO order_items VALUES ($1, $2, $3, $4, $5)',
    <Object?>[itemId + 1000, ordCount + 1000, 0, 3, 99.99],
  );
  db.executeWithParams(
    r'UPDATE products SET stock = stock - 3 WHERE id = $1',
    <Object?>[0],
  );
  db.commit();
  watch.stop();
  final commitMs = watch.elapsedMicroseconds / 1000.0;
  m.put('txn_commit_ms', commitMs);

  final afterStock = db.query(
    r'SELECT stock FROM products WHERE id = $1',
    <Object?>[0],
  );
  final stockAfter = afterStock.first['stock'] as int;
  print('  Committed: stock $stockBefore -> $stockAfter in ${commitMs}ms');

  watch = Stopwatch()..start();
  final preRollbackStock = db.query(
    r'SELECT stock FROM products WHERE id = $1',
    <Object?>[1],
  );
  final preStock = preRollbackStock.first['stock'] as int;

  db.begin();
  db.executeWithParams(
    r'UPDATE products SET stock = stock - 999 WHERE id = $1',
    <Object?>[1],
  );
  db.rollback();
  watch.stop();
  final rollbackMs = watch.elapsedMicroseconds / 1000.0;
  m.put('txn_rollback_ms', rollbackMs);

  final postRollbackStock = db.query(
    r'SELECT stock FROM products WHERE id = $1',
    <Object?>[1],
  );
  final postStock = postRollbackStock.first['stock'] as int;
  print(
      '  Rollback: stock $preStock -> UPDATE -999 -> ROLLBACK -> $postStock in ${rollbackMs}ms');
  print('');

  // ── Section 12: Views + Schema Introspection ────────────────────
  print('--- Views ---');
  watch = Stopwatch()..start();
  db.execute('''
    CREATE VIEW order_summary AS
    SELECT o.id AS order_id, c.name AS customer, c.city,
           o.order_date, o.total, COUNT(oi.id) AS item_count
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    JOIN order_items oi ON oi.order_id = o.id
    GROUP BY o.id, c.name, c.city, o.order_date, o.total
  ''');
  watch.stop();
  final viewOrderMs = watch.elapsedMicroseconds / 1000.0;
  m.put('view_order_summary_ms', viewOrderMs);
  print('  Created order_summary view in ${viewOrderMs}ms');

  watch = Stopwatch()..start();
  db.execute('''
    CREATE VIEW product_ratings AS
    SELECT r.product_id, p.name AS product_name, p.price,
           c.name AS category, r.rating, r.comment
    FROM reviews r
    JOIN products p ON p.id = r.product_id
    JOIN categories c ON c.id = p.category_id
  ''');
  watch.stop();
  final viewProdMs = watch.elapsedMicroseconds / 1000.0;
  m.put('view_product_ratings_ms', viewProdMs);
  print('  Created product_ratings view in ${viewProdMs}ms');

  watch = Stopwatch()..start();
  final summaryRows = db.query('''
    SELECT * FROM order_summary ORDER BY total DESC LIMIT 5
  ''');
  watch.stop();
  final queryViewMs = watch.elapsedMicroseconds / 1000.0;
  m.put('query_order_summary_ms', queryViewMs);
  print('  Query order_summary (top 5): ${queryViewMs}ms');
  for (var i = 0; i < summaryRows.length; i++) {
    final r = summaryRows[i];
    print(
      '    Order ${r['order_id']}: ${r['customer']} (${r['city']}), \$${(r['total'] as num).toStringAsFixed(2)}, ${r['item_count']} items',
    );
  }
  print('');

  print('--- Schema Introspection ---');
  watch = Stopwatch()..start();
  final tables = db.schema.listTables();
  final indexes = db.schema.listIndexes();
  final views = db.schema.listViews();
  final triggers = db.schema.listTriggers();
  watch.stop();
  final introMs = watch.elapsedMicroseconds / 1000.0;
  m.put('introspection_ms', introMs);
  print('  Tables: ${tables.join(', ')}');
  print('  Indexes: ${indexes.length} total');
  for (final idx in indexes) {
    print(
      '    ${idx.name} on ${idx.tableName}(${idx.columns.join(', ')}) [${idx.kind}]${idx.unique ? ' UNIQUE' : ''}',
    );
  }
  print('  Views: ${views.join(', ')}');
  print('  Triggers: ${triggers.length}');
  final ordersInfo = db.schema.describeTable('orders');
  print('  orders columns:');
  for (final col in ordersInfo.columns) {
    print(
      '    ${col.name}: ${col.type}${col.nullable ? '' : ' NOT NULL'}${col.primaryKey ? ' PK' : ''}',
    );
  }
  for (final fk in ordersInfo.foreignKeys) {
    print(
      '    FK: ${fk.columns.join(', ')} -> ${fk.referencedTable}(${fk.referencedColumns.join(', ')}) ON DELETE ${fk.onDelete}',
    );
  }
  print('  Introspection: ${introMs}ms');
  print('');

  // ── Performance Summary ─────────────────────────────────────────
  print('=== Performance Summary ===');
  print(
      '  Insert throughput:  ${formatNum(totalRps.round())} rows/sec ($totalInsertRows rows)');
  print('  Point read p50:     ${p50}ms');
  print('  Point read p95:     ${p95}ms');
  print('  Point read p99:     ${p99}ms');
  print('  Email lookup p50:   ${emailP50}ms');
  print('  Fetchall ($prodCount rows): ${fetchallMs}ms');
  print('');

  // ── Write JSON ──────────────────────────────────────────────────
  m.writeJson(jsonPath);
  print('Results written to: $jsonPath');

  if (keepDb) {
    print('Database saved to: $dbPath');
  }

  db.close();
  print('Done.');
}
