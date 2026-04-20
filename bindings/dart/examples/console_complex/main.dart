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

String formatInt(num value) {
  final s = value.toStringAsFixed(0);
  final buf = StringBuffer();
  var count = 0;
  for (var i = s.length - 1; i >= 0; i--) {
    if (count == 3 && s[i] != '-') {
      buf.write(',');
      count = 0;
    }
    buf.write(s[i]);
    count++;
  }
  return buf.toString().split('').reversed.join();
}

String formatMs(double ms) {
  if (ms >= 1000) return '${(ms / 1000).toStringAsFixed(3)}s';
  if (ms >= 100) return '${ms.toStringAsFixed(1)}ms';
  if (ms >= 10) return '${ms.toStringAsFixed(2)}ms';
  return '${ms.toStringAsFixed(3)}ms';
}

String formatSeconds(double seconds) => '${seconds.toStringAsFixed(2)}s';

class DemoConfig {
  DemoConfig({
    required this.scaleFactor,
    required this.keepDb,
    required this.dbPath,
    required this.jsonPath,
  });

  final int scaleFactor;
  final bool keepDb;
  final String dbPath;
  final String jsonPath;

  static DemoConfig parse(List<String> args) {
    var scaleFactor = 1;
    var keepDb = false;
    var dbPath = 'dart_complex_demo.ddb';
    var jsonPath = 'bench_result.json';

    var i = 0;
    while (i < args.length) {
      final arg = args[i];
      if (arg == '--count') {
        if (i + 1 >= args.length) {
          throw ArgumentError('--count requires a value');
        }
        scaleFactor = int.parse(args[++i]);
        if (scaleFactor < 1) {
          throw ArgumentError('--count must be >= 1');
        }
      } else if (arg == '--keep-db') {
        keepDb = true;
      } else if (arg == '--db-path') {
        if (i + 1 >= args.length) {
          throw ArgumentError('--db-path requires a value');
        }
        dbPath = args[++i];
      } else if (arg == '--json') {
        if (i + 1 >= args.length) {
          throw ArgumentError('--json requires a value');
        }
        jsonPath = args[++i];
      } else if (arg == '-h' || arg == '--help') {
        _printUsage();
        exit(0);
      } else {
        throw ArgumentError('Unknown argument: $arg');
      }
      i++;
    }

    return DemoConfig(
      scaleFactor: scaleFactor,
      keepDb: keepDb,
      dbPath: dbPath,
      jsonPath: jsonPath,
    );
  }

  static void _printUsage() {
    print('Usage: dart run main.dart [options]');
    print('');
    print('Options:');
    print(
      '  --count <n>    Multiply base data volumes by n (default: 1 = 26,100 base rows)',
    );
    print('  --keep-db      Persist the database file');
    print('  --db-path <p>  Custom file path (default: dart_complex_demo.ddb)');
    print('  --json <path>  JSON output path (default: bench_result.json)');
    print('  -h, --help     Show help');
  }
}

class _ScaledCounts {
  const _ScaledCounts({
    required this.categories,
    required this.customers,
    required this.products,
    required this.orders,
    required this.reviews,
  });

  factory _ScaledCounts.forScale(int scaleFactor) => _ScaledCounts(
        categories: _categoryCount * scaleFactor,
        customers: _customerCount * scaleFactor,
        products: _productCount * scaleFactor,
        orders: _orderCount * scaleFactor,
        reviews: _reviewCount * scaleFactor,
      );

  final int categories;
  final int customers;
  final int products;
  final int orders;
  final int reviews;

  int get baseRows => categories + customers + products + orders + reviews;
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

class _InsertStage {
  const _InsertStage({
    required this.key,
    required this.label,
    required this.rows,
    required this.seconds,
  });

  final String key;
  final String label;
  final int rows;
  final double seconds;
}

class _PointReadSummary {
  const _PointReadSummary(this.p50, this.p95, this.p99);

  final double p50;
  final double p95;
  final double p99;
}

class ComplexDemoRunner {
  ComplexDemoRunner(this.config)
      : counts = _ScaledCounts.forScale(config.scaleFactor),
        rng = Random(42),
        db = Database.open(
          config.keepDb ? config.dbPath : ':memory:',
          libraryPath: findNativeLib(),
        );

  final DemoConfig config;
  final _ScaledCounts counts;
  final Random rng;
  final Database db;
  final metrics = _Metrics();

  late final String databasePath = config.keepDb ? config.dbPath : ':memory:';
  final cities = const [
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
  final comments = const [
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

  int _orderItemCount = 0;
  int _actualInsertedRows = 0;
  double _totalInsertSeconds = 0;
  double _totalInsertRowsPerSecond = 0;
  late _PointReadSummary _pointReadSummary;
  double _emailLookupP50 = 0;
  double _emailLookupP95 = 0;
  double _fetchallMs = 0;

  void run() {
    _recordMetadata();
    _printHeader();
    try {
      _createSchema();
      _runBulkInsert();
      _runFetchAndStreaming();
      _runPointReads();
      _runIndexedLookup();
      _runJoinShowcase();
      _runAggregations();
      _runTextSearch();
      _runCteAndSubqueryShowcase();
      _runTransactionDemo();
      _runViewsAndIntrospection();
      _printSummary();
      metrics.writeJson(config.jsonPath);
      print('Results written to: ${config.jsonPath}');
      if (config.keepDb) {
        print('Database saved to: ${config.dbPath}');
      }
      print('Done.');
    } finally {
      db.close();
    }
  }

  void _recordMetadata() {
    metrics.meta('engine_version', db.engineVersion);
    metrics.meta('database', databasePath);
    metrics.meta('scale_factor', config.scaleFactor);
    metrics.meta('target_base_rows', counts.baseRows);
    metrics.meta('note',
        'Wide-surface showcase timings. Good for regressions and demos; not a fair cross-engine benchmark.');
  }

  void _printHeader() {
    print('=== DecentDB Dart Complex Demo ===');
    print('Engine version: ${db.engineVersion}');
    print('Database: $databasePath');
    print(
        'Scale: ${config.scaleFactor}x (${formatInt(counts.baseRows)} base rows + derived order items)');
    print('This is a scope-heavy showcase, not an apples-to-apples benchmark.');
    print('');
  }

  void _createSchema() {
    print('--- Schema Creation ---');
    final watch = Stopwatch()..start();

    // Group all DDL into a single transaction so the WAL fsync cost is paid
    // once instead of once per statement. With WalSyncMode::Full each commit
    // calls fdatasync (~30 ms on typical NVMe); collapsing 10 commits into 1
    // turns ~325 ms of DDL into a few tens of ms.
    db.transaction(() {
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

      // Keep explicit indexes focused on the demo queries rather than duplicating
      // PK/UNIQUE/FK-backed indexes the engine already creates.
      db.execute('CREATE INDEX idx_customers_city ON customers (city)');
      db.execute('CREATE INDEX idx_products_name ON products (name)');
      db.execute('CREATE INDEX idx_orders_date ON orders (order_date)');
      db.execute('CREATE INDEX idx_orders_total ON orders (total)');
    });

    watch.stop();
    final ddlMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('ddl_ms', ddlMs);
    metrics.meta('explicit_index_count', 4);
    print('  6 tables, 4 explicit indexes created in ${formatMs(ddlMs)}');
    print('');
  }

  void _runBulkInsert() {
    print('--- Bulk Insert ---');
    final stages = <_InsertStage>[
      _insertCategories(),
      _insertCustomers(),
      _insertProducts(),
      _insertOrdersAndItems(),
      _insertReviews(),
    ];

    _totalInsertSeconds = stages.fold(0.0, (sum, stage) => sum + stage.seconds);
    _actualInsertedRows = stages.fold(0, (sum, stage) => sum + stage.rows);
    _totalInsertRowsPerSecond = _actualInsertedRows / _totalInsertSeconds;

    for (final stage in stages) {
      final rps = stage.rows / stage.seconds;
      metrics.put('insert_${stage.key}_rows', stage.rows);
      metrics.put('insert_${stage.key}_s', stage.seconds);
      metrics.put('insert_${stage.key}_rps', rps);
      print(
        '  ${stage.label.padRight(18)} ${formatInt(stage.rows).padLeft(8)} rows in '
        '${formatSeconds(stage.seconds).padLeft(8)} (${formatInt(rps.round())} rows/sec)',
      );
    }

    metrics.put('insert_total_rows', _actualInsertedRows);
    metrics.put('insert_total_s', _totalInsertSeconds);
    metrics.put('insert_total_rps', _totalInsertRowsPerSecond);
    metrics.meta('actual_inserted_rows', _actualInsertedRows);
    metrics.meta('actual_order_item_rows', _orderItemCount);
    print(
      '  ${'TOTAL'.padRight(18)} ${formatInt(_actualInsertedRows).padLeft(8)} rows in '
      '${formatSeconds(_totalInsertSeconds).padLeft(8)} (${formatInt(_totalInsertRowsPerSecond.round())} rows/sec)',
    );
    print('');
  }

  _InsertStage _insertCategories() {
    final rows = <List<Object?>>[];
    for (var i = 0; i < counts.categories; i++) {
      rows.add(<Object?>[
        i,
        'category_$i',
        'Description for category $i with realistic text payload',
      ]);
    }
    final stmt = db.prepare(r'INSERT INTO categories VALUES ($1, $2, $3)');
    final watch = Stopwatch()..start();
    try {
      db.transaction(() {
        stmt.executeBatchTyped('itt', rows);
      });
    } finally {
      stmt.dispose();
    }
    watch.stop();
    return _InsertStage(
      key: 'categories',
      label: 'categories',
      rows: counts.categories,
      seconds: watch.elapsedMicroseconds / 1000000.0,
    );
  }

  _InsertStage _insertCustomers() {
    final rows = <List<Object?>>[];
    for (var i = 0; i < counts.customers; i++) {
      rows.add(<Object?>[
        i,
        'Customer_${i.toString().padLeft(6, '0')}',
        'user$i@example.com',
        cities[i % cities.length],
        '2024-${((i % 12) + 1).toString().padLeft(2, '0')}-${((i % 28) + 1).toString().padLeft(2, '0')}',
      ]);
    }
    final stmt = db.prepare(
      r'INSERT INTO customers VALUES ($1, $2, $3, $4, $5)',
    );
    final watch = Stopwatch()..start();
    try {
      db.transaction(() {
        stmt.executeBatchTyped('itttt', rows);
      });
    } finally {
      stmt.dispose();
    }
    watch.stop();
    return _InsertStage(
      key: 'customers',
      label: 'customers',
      rows: counts.customers,
      seconds: watch.elapsedMicroseconds / 1000000.0,
    );
  }

  _InsertStage _insertProducts() {
    final rows = <List<Object?>>[];
    for (var i = 0; i < counts.products; i++) {
      rows.add(<Object?>[
        i,
        'Product_${i.toString().padLeft(6, '0')}_Widget',
        rng.nextDouble() * 990.0 + 10.0,
        rng.nextInt(counts.categories),
        rng.nextInt(500),
      ]);
    }
    final stmt = db.prepare(
      r'INSERT INTO products VALUES ($1, $2, $3, $4, $5)',
    );
    final watch = Stopwatch()..start();
    try {
      db.transaction(() {
        stmt.executeBatchTyped('itfii', rows);
      });
    } finally {
      stmt.dispose();
    }
    watch.stop();
    return _InsertStage(
      key: 'products',
      label: 'products',
      rows: counts.products,
      seconds: watch.elapsedMicroseconds / 1000000.0,
    );
  }

  _InsertStage _insertOrdersAndItems() {
    var itemId = 0;
    final orderRows = <List<Object?>>[];
    final itemRows = <List<Object?>>[];

    for (var i = 0; i < counts.orders; i++) {
      final customerId = rng.nextInt(counts.customers);
      final month = ((i % 12) + 1).toString().padLeft(2, '0');
      final day = ((i % 28) + 1).toString().padLeft(2, '0');
      final lineCount = 1 + rng.nextInt(5);
      var orderTotal = 0.0;
      for (var j = 0; j < lineCount; j++) {
        final productId = rng.nextInt(counts.products);
        final quantity = 1 + rng.nextInt(10);
        final unitPrice = rng.nextDouble() * 990.0 + 10.0;
        orderTotal += quantity * unitPrice;
        itemRows.add(<Object?>[
          itemId + j,
          i,
          productId,
          quantity,
          unitPrice,
        ]);
      }
      orderRows.add(<Object?>[i, customerId, '2024-$month-$day', orderTotal]);
      itemId += lineCount;
    }

    final orderStmt = db.prepare(r'INSERT INTO orders VALUES ($1, $2, $3, $4)');
    final itemStmt = db.prepare(
      r'INSERT INTO order_items VALUES ($1, $2, $3, $4, $5)',
    );
    final watch = Stopwatch()..start();
    try {
      db.transaction(() {
        orderStmt.executeBatchTyped('iitf', orderRows);
        itemStmt.executeBatchTyped('iiiif', itemRows);
      });
    } finally {
      orderStmt.dispose();
      itemStmt.dispose();
    }
    watch.stop();
    _orderItemCount = itemId;
    return _InsertStage(
      key: 'orders_with_items',
      label: 'orders+items',
      rows: counts.orders + _orderItemCount,
      seconds: watch.elapsedMicroseconds / 1000000.0,
    );
  }

  _InsertStage _insertReviews() {
    final rows = <List<Object?>>[];
    for (var i = 0; i < counts.reviews; i++) {
      rows.add(<Object?>[
        i,
        rng.nextInt(counts.products),
        rng.nextInt(counts.customers),
        1 + rng.nextInt(5),
        comments[i % comments.length],
      ]);
    }
    final stmt = db.prepare(r'INSERT INTO reviews VALUES ($1, $2, $3, $4, $5)');
    final watch = Stopwatch()..start();
    try {
      db.transaction(() {
        stmt.executeBatchTyped('iiiit', rows);
      });
    } finally {
      stmt.dispose();
    }
    watch.stop();
    return _InsertStage(
      key: 'reviews',
      label: 'reviews',
      rows: counts.reviews,
      seconds: watch.elapsedMicroseconds / 1000000.0,
    );
  }

  void _runFetchAndStreaming() {
    print('--- Fetchall & Streaming ---');
    db.query('SELECT COUNT(*) FROM products');

    var watch = Stopwatch()..start();
    final allProducts = db.query(
      'SELECT id, name, price, category_id, stock FROM products',
    );
    watch.stop();
    _fetchallMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('fetchall_ms', _fetchallMs);
    metrics.put('fetchall_rows', allProducts.length);
    print(
        '  Fetchall (${allProducts.length} products): ${formatMs(_fetchallMs)}');

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
    metrics.put('streaming_ms', streamingMs);
    metrics.put('streaming_rows', streamCount);
    print(
      '  Streaming (batch=$_fetchBatchSize, $streamCount rows): ${formatMs(streamingMs)}',
    );
    print('');
  }

  void _runPointReads() {
    print('--- Point Reads by ID ($_pointReads x on products) ---');
    final stmt =
        db.prepare(r'SELECT id, name, price FROM products WHERE id = $1');
    final pointIds = List<int>.generate(counts.products, (i) => i);
    for (var i = 0; i < _pointReads; i++) {
      final j = i + rng.nextInt(counts.products - i);
      final tmp = pointIds[i];
      pointIds[i] = pointIds[j];
      pointIds[j] = tmp;
    }

    final latencies = List<double>.filled(_pointReads, 0.0);
    try {
      stmt.reset();
      stmt.clearBindings();
      stmt.bindAll(<Object?>[pointIds[0]]);
      stmt.query();

      for (var i = 0; i < _pointReads; i++) {
        final sw = Stopwatch()..start();
        stmt.reset();
        stmt.clearBindings();
        stmt.bindAll(<Object?>[pointIds[i]]);
        final rows = stmt.query();
        sw.stop();
        if (rows.isEmpty) {
          throw StateError('Point read missed id=${pointIds[i]}');
        }
        latencies[i] = sw.elapsedMicroseconds / 1000.0;
      }
    } finally {
      stmt.dispose();
    }

    latencies.sort();
    _pointReadSummary = _PointReadSummary(
      percentile(latencies, 50),
      percentile(latencies, 95),
      percentile(latencies, 99),
    );
    metrics.put('point_read_p50_ms', _pointReadSummary.p50);
    metrics.put('point_read_p95_ms', _pointReadSummary.p95);
    metrics.put('point_read_p99_ms', _pointReadSummary.p99);
    print(
      '  p50=${formatMs(_pointReadSummary.p50)}  '
      'p95=${formatMs(_pointReadSummary.p95)}  '
      'p99=${formatMs(_pointReadSummary.p99)}',
    );
    print('');
  }

  void _runIndexedLookup() {
    print('--- Indexed Lookup (email) ---');
    final stmt = db.prepare(
      r'SELECT id, name, city FROM customers WHERE email = $1',
    );
    final latencies = List<double>.filled(_pointReads, 0.0);
    try {
      for (var i = 0; i < _pointReads; i++) {
        final id = rng.nextInt(counts.customers);
        final sw = Stopwatch()..start();
        stmt.reset();
        stmt.clearBindings();
        stmt.bindAll(<Object?>['user$id@example.com']);
        final rows = stmt.query();
        sw.stop();
        if (rows.isEmpty) {
          throw StateError('Email lookup missed user$id@example.com');
        }
        latencies[i] = sw.elapsedMicroseconds / 1000.0;
      }
    } finally {
      stmt.dispose();
    }
    latencies.sort();
    _emailLookupP50 = percentile(latencies, 50);
    _emailLookupP95 = percentile(latencies, 95);
    metrics.put('email_lookup_p50_ms', _emailLookupP50);
    metrics.put('email_lookup_p95_ms', _emailLookupP95);
    print(
        '  p50=${formatMs(_emailLookupP50)}  p95=${formatMs(_emailLookupP95)}');
    print('');
  }

  void _runJoinShowcase() {
    print('--- 4-Table Join (orders + customers + order_items + products) ---');
    final watch = Stopwatch()..start();
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
    metrics.put('join_4table_ms', joinMs);
    metrics.put('join_4table_rows', joinRows.length);
    print('  Result: ${joinRows.length} rows in ${formatMs(joinMs)}');
    print('');

    print('--- Inner Join + Aggregation (reviewed products) ---');
    final ratingWatch = Stopwatch()..start();
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
    ratingWatch.stop();
    final ratingMs = ratingWatch.elapsedMicroseconds / 1000.0;
    metrics.put('join_agg_rating_ms', ratingMs);
    metrics.put('join_agg_rating_rows', ratingRows.length);
    print('  Result: ${ratingRows.length} rows in ${formatMs(ratingMs)}');
    for (var i = 0; i < min(5, ratingRows.length); i++) {
      final row = ratingRows[i];
      print(
        '    ${row['name']} (${row['category']}): '
        'avg=${(row['avg_rating'] as num).toStringAsFixed(2)}, '
        'reviews=${row['review_count']}',
      );
    }
    print('');
  }

  void _runAggregations() {
    print('--- Aggregations ---');

    var watch = Stopwatch()..start();
    final byCustomer = db.query('''
      SELECT c.name, COUNT(o.id) AS order_count, SUM(o.total) AS total_spent
      FROM customers c
      JOIN orders o ON o.customer_id = c.id
      GROUP BY c.id, c.name
      ORDER BY total_spent DESC
      LIMIT 10
    ''');
    watch.stop();
    final customerSpendMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('agg_customer_spend_ms', customerSpendMs);
    print('  Top 10 customers by spend: ${formatMs(customerSpendMs)}');
    for (var i = 0; i < min(5, byCustomer.length); i++) {
      final row = byCustomer[i];
      print(
        '    ${row['name']}: ${row['order_count']} orders, '
        '\$${(row['total_spent'] as num).toStringAsFixed(2)}',
      );
    }

    watch = Stopwatch()..start();
    final categoryStats = db.query('''
      SELECT c.name, COUNT(p.id) AS product_count, AVG(p.price) AS avg_price,
             SUM(p.stock) AS total_stock
      FROM categories c
      JOIN products p ON p.category_id = c.id
      GROUP BY c.id, c.name
      ORDER BY product_count DESC
      LIMIT 10
    ''');
    watch.stop();
    final categoryStatsMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('agg_category_stats_ms', categoryStatsMs);
    print('  Top 10 categories by product count: ${formatMs(categoryStatsMs)}');
    for (var i = 0; i < min(5, categoryStats.length); i++) {
      final row = categoryStats[i];
      print(
        '    ${row['name']}: ${row['product_count']} products, '
        'avg \$${(row['avg_price'] as num).toStringAsFixed(2)}, '
        'stock=${row['total_stock']}',
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
    final topProductsMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('agg_top_products_ms', topProductsMs);
    print('  Top 10 products by review count: ${formatMs(topProductsMs)}');
    for (var i = 0; i < min(5, topProducts.length); i++) {
      final row = topProducts[i];
      print(
        '    ${row['name']}: ${row['review_count']} reviews, '
        'avg rating ${(row['avg_rating'] as num).toStringAsFixed(2)}',
      );
    }
    print('');
  }

  void _runTextSearch() {
    print('--- Text Search (LIKE) ---');
    var watch = Stopwatch()..start();
    final searchRows = db.query(
      r'''
      SELECT id, name, price FROM products
      WHERE name LIKE '%' || $1 || '%'
      ORDER BY price DESC
    ''',
      <Object?>['Widget'],
    );
    watch.stop();
    final containsMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('text_search_contains_ms', containsMs);
    metrics.put('text_search_contains_matches', searchRows.length);
    print(
        '  LIKE "%Widget%": ${searchRows.length} matches in ${formatMs(containsMs)}');

    watch = Stopwatch()..start();
    final prefixRows = db.query(
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
    metrics.put('text_search_prefix_ms', prefixMs);
    metrics.put('text_search_prefix_matches', prefixRows.length);
    print(
        '  LIKE "Customer_000%": ${prefixRows.length} matches in ${formatMs(prefixMs)}');
    print('');
  }

  void _runCteAndSubqueryShowcase() {
    print('--- CTE / Subqueries ---');

    var watch = Stopwatch()..start();
    final aboveAverage = db.query('''
      WITH customer_totals AS (
        SELECT customer_id, SUM(total) AS spent
        FROM orders
        GROUP BY customer_id
      )
      SELECT c.name, ct.spent
      FROM customer_totals ct
      JOIN customers c ON c.id = ct.customer_id
      WHERE ct.spent > (SELECT AVG(spent) FROM customer_totals)
      ORDER BY ct.spent DESC
      LIMIT 10
    ''');
    watch.stop();
    final aboveAverageMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('cte_above_avg_spend_ms', aboveAverageMs);
    print(
      '  Customers above avg spend: ${aboveAverage.length} rows in ${formatMs(aboveAverageMs)}',
    );

    // Pick the top qualifying orders first, then count items for just those
    // rows. This keeps the showcase broad without turning it into a full-table
    // aggregate over order_items on every run.
    watch = Stopwatch()..start();
    final topOrders = db.query('''
      WITH avg_total AS (
        SELECT AVG(total) AS avg_order_total FROM orders
      ),
      top_orders AS (
        SELECT o.id AS order_id, o.customer_id, o.total
        FROM orders o
        CROSS JOIN avg_total a
        WHERE o.total > a.avg_order_total
        ORDER BY o.total DESC
        LIMIT 10
      )
      SELECT t.order_id, c.name AS customer, t.total,
             (
               SELECT COUNT(*)
               FROM order_items oi
               WHERE oi.order_id = t.order_id
             ) AS item_count
      FROM top_orders t
      JOIN customers c ON c.id = t.customer_id
      ORDER BY t.total DESC
    ''');
    watch.stop();
    final topOrdersMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('cte_top_orders_ms', topOrdersMs);
    print(
      '  Orders above avg total: ${topOrders.length} rows in ${formatMs(topOrdersMs)}',
    );
    for (var i = 0; i < min(5, topOrders.length); i++) {
      final row = topOrders[i];
      print(
        '    Order ${row['order_id']}: ${row['customer']}, '
        '\$${(row['total'] as num).toStringAsFixed(2)}, '
        '${row['item_count']} items',
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
    final monthlyTotalsMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('cte_monthly_totals_ms', monthlyTotalsMs);
    print(
      '  Monthly order totals: ${monthlyTotals.length} months in ${formatMs(monthlyTotalsMs)}',
    );
    print('');
  }

  void _runTransactionDemo() {
    print('--- Transaction Demo ---');

    var watch = Stopwatch()..start();
    final stockBefore = db.query(
      r'SELECT stock FROM products WHERE id = $1',
      <Object?>[0],
    ).first['stock'] as int;

    db.transaction(() {
      db.executeWithParams(
        r'INSERT INTO orders VALUES ($1, $2, $3, $4)',
        <Object?>[counts.orders + 1000, 0, '2025-01-01', 299.97],
      );
      db.executeWithParams(
        r'INSERT INTO order_items VALUES ($1, $2, $3, $4, $5)',
        <Object?>[_orderItemCount + 1000, counts.orders + 1000, 0, 3, 99.99],
      );
      db.executeWithParams(
        r'UPDATE products SET stock = stock - 3 WHERE id = $1',
        <Object?>[0],
      );
    });
    watch.stop();
    final commitMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('txn_commit_ms', commitMs);

    final stockAfter = db.query(
      r'SELECT stock FROM products WHERE id = $1',
      <Object?>[0],
    ).first['stock'] as int;
    print(
        '  Committed: stock $stockBefore -> $stockAfter in ${formatMs(commitMs)}');

    watch = Stopwatch()..start();
    final rollbackBefore = db.query(
      r'SELECT stock FROM products WHERE id = $1',
      <Object?>[1],
    ).first['stock'] as int;

    db.begin();
    try {
      db.executeWithParams(
        r'UPDATE products SET stock = stock - 999 WHERE id = $1',
        <Object?>[1],
      );
      db.rollback();
    } catch (_) {
      db.rollback();
      rethrow;
    }
    watch.stop();
    final rollbackMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('txn_rollback_ms', rollbackMs);

    final rollbackAfter = db.query(
      r'SELECT stock FROM products WHERE id = $1',
      <Object?>[1],
    ).first['stock'] as int;
    if (rollbackAfter != rollbackBefore) {
      throw StateError(
          'Rollback changed stock unexpectedly: $rollbackBefore -> $rollbackAfter');
    }
    print(
      '  Rollback: stock $rollbackBefore -> UPDATE -999 -> ROLLBACK -> $rollbackAfter '
      'in ${formatMs(rollbackMs)}',
    );
    print('');
  }

  void _runViewsAndIntrospection() {
    print('--- Views ---');
    var watch = Stopwatch()..start();
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
    final orderSummaryViewMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('view_order_summary_ms', orderSummaryViewMs);
    print('  Created order_summary view in ${formatMs(orderSummaryViewMs)}');

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
    final productRatingsViewMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('view_product_ratings_ms', productRatingsViewMs);
    print(
        '  Created product_ratings view in ${formatMs(productRatingsViewMs)}');

    watch = Stopwatch()..start();
    final summaryRows = db.query(
      'SELECT * FROM order_summary ORDER BY total DESC LIMIT 5',
    );
    watch.stop();
    final queryViewMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('query_order_summary_ms', queryViewMs);
    print('  Query order_summary (top 5): ${formatMs(queryViewMs)}');
    for (var i = 0; i < summaryRows.length; i++) {
      final row = summaryRows[i];
      print(
        '    Order ${row['order_id']}: ${row['customer']} (${row['city']}), '
        '\$${(row['total'] as num).toStringAsFixed(2)}, ${row['item_count']} items',
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
    final introspectionMs = watch.elapsedMicroseconds / 1000.0;
    metrics.put('introspection_ms', introspectionMs);
    print('  Tables: ${tables.join(', ')}');
    print('  Indexes: ${indexes.length} total');
    for (final index in indexes) {
      print(
        '    ${index.name} on ${index.tableName}(${index.columns.join(', ')}) '
        '[${index.kind}]${index.unique ? ' UNIQUE' : ''}',
      );
    }
    print('  Views: ${views.join(', ')}');
    print('  Triggers: ${triggers.length}');
    final ordersInfo = db.schema.describeTable('orders');
    print('  orders columns:');
    for (final column in ordersInfo.columns) {
      print(
        '    ${column.name}: ${column.type}'
        '${column.nullable ? '' : ' NOT NULL'}'
        '${column.primaryKey ? ' PK' : ''}',
      );
    }
    for (final foreignKey in ordersInfo.foreignKeys) {
      print(
        '    FK: ${foreignKey.columns.join(', ')} -> '
        '${foreignKey.referencedTable}(${foreignKey.referencedColumns.join(', ')}) '
        'ON DELETE ${foreignKey.onDelete}',
      );
    }
    final snapshot = db.schema.getSchemaSnapshot();
    final tempTableCount =
        snapshot.tables.where((table) => table.temporary).length;
    print(
      '  Snapshot: ${snapshot.tables.length} tables, ${snapshot.views.length} views, '
      '${snapshot.indexes.length} indexes, ${snapshot.triggers.length} triggers '
      '($tempTableCount temp tables)',
    );
    print('  Introspection: ${formatMs(introspectionMs)}');
    print('');
  }

  void _printSummary() {
    print('=== Performance Summary ===');
    print(
        '  Insert throughput:  ${formatInt(_totalInsertRowsPerSecond.round())} rows/sec '
        '(${formatInt(_actualInsertedRows)} actual rows)');
    print('  Point read p50:     ${formatMs(_pointReadSummary.p50)}');
    print('  Point read p95:     ${formatMs(_pointReadSummary.p95)}');
    print('  Point read p99:     ${formatMs(_pointReadSummary.p99)}');
    print('  Email lookup p50:   ${formatMs(_emailLookupP50)}');
    print('  Email lookup p95:   ${formatMs(_emailLookupP95)}');
    print(
        '  Fetchall (${formatInt(counts.products)} rows): ${formatMs(_fetchallMs)}');
    print('');
  }
}

void main(List<String> args) {
  final config = DemoConfig.parse(args);
  ComplexDemoRunner(config).run();
}
