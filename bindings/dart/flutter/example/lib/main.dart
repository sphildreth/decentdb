import 'dart:typed_data';

import 'package:decentdb_flutter/decentdb_flutter.dart';
import 'package:flutter/material.dart' hide Row;
import 'package:path_provider/path_provider.dart';

void main() {
  runApp(const DecentDbExampleApp());
}

final class DemoKeyProvider implements DecentDbKeyProvider {
  @override
  Future<Uint8List> loadDatabaseKey() async {
    return Uint8List.fromList(List<int>.generate(32, (index) => index + 1));
  }
}

class DecentDbExampleApp extends StatelessWidget {
  const DecentDbExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'DecentDB',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.teal),
        useMaterial3: true,
      ),
      home: const DecentDbExampleHome(),
    );
  }
}

class DecentDbExampleHome extends StatefulWidget {
  const DecentDbExampleHome({super.key});

  @override
  State<DecentDbExampleHome> createState() => _DecentDbExampleHomeState();
}

class _DecentDbExampleHomeState extends State<DecentDbExampleHome>
    with WidgetsBindingObserver {
  Database? _db;
  var _rows = <Row>[];
  var _status = 'Closed';

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.paused ||
        state == AppLifecycleState.detached) {
      _checkpointAndClose();
    }
  }

  Future<void> _open() async {
    final db = await DecentDbMobile.openAppDatabase(
      'example.ddb',
      keyProvider: DemoKeyProvider(),
      processCoordination: ProcessCoordinationMode.required,
      processCoordinationTimeoutMs: 30000,
    );
    db.execute(
      'CREATE TABLE IF NOT EXISTS notes (id INT64 PRIMARY KEY, body TEXT)',
    );
    setState(() {
      _db = db;
      _status = 'Open';
    });
    _refresh();
  }

  Future<void> _insert() async {
    final db = _db;
    if (db == null) return;
    final nextId =
        ((db.query('SELECT COUNT(*) AS cnt FROM notes').single['cnt'] as int) +
            1);
    final insert = db.prepare(r'INSERT INTO notes VALUES ($1, $2)');
    try {
      insert.bindAll([nextId, 'note $nextId']);
      insert.execute();
    } finally {
      insert.dispose();
    }
    _refresh();
  }

  Future<void> _export() async {
    final db = _db;
    if (db == null) return;
    final directory = await getApplicationSupportDirectory();
    final exportPath = '${directory.path}/example-copy.ddb';
    db.saveAs(exportPath);
    setState(() {
      _status = 'Exported copy';
    });
  }

  Future<void> _syncDemo() async {
    final db = _db;
    if (db == null) return;
    db.sync.initReplica('mobile-example');
    final status = db.sync.status();
    setState(() {
      _status = 'Sync ${status['replica_id']}';
    });
  }

  void _refresh() {
    final db = _db;
    if (db == null) return;
    setState(() {
      _rows = db.query('SELECT id, body FROM notes ORDER BY id');
    });
  }

  void _checkpointAndClose() {
    final db = _db;
    if (db == null) return;
    db.checkpoint();
    db.close();
    setState(() {
      _db = null;
      _status = 'Closed';
      _rows = const [];
    });
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _db?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final open = _db != null;
    return Scaffold(
      appBar: AppBar(title: const Text('DecentDB')),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Text(_status, style: Theme.of(context).textTheme.titleMedium),
          const SizedBox(height: 16),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              FilledButton(
                  onPressed: open ? null : _open, child: const Text('Open')),
              FilledButton(
                  onPressed: open ? _insert : null,
                  child: const Text('Insert')),
              OutlinedButton(
                  onPressed: open ? _export : null,
                  child: const Text('Export')),
              OutlinedButton(
                  onPressed: open ? _syncDemo : null,
                  child: const Text('Sync')),
              TextButton(
                onPressed: open ? _checkpointAndClose : null,
                child: const Text('Close'),
              ),
            ],
          ),
          const SizedBox(height: 16),
          for (final row in _rows)
            ListTile(
              dense: true,
              title: Text(row['body'] as String),
              leading: Text('${row['id']}'),
            ),
        ],
      ),
    );
  }
}
