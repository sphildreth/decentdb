import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

void main() {
  test('redacts sensitive native open-option values', () {
    final sanitized = sanitizeOpenOptions(
      'cache_mb=8;encryption_key=plain encryption_key_hex=001122,'
      'tde_key=alias;tde_key_hex=aabbcc;write_queue_enabled=true',
    );

    expect(sanitized.hasRedactions, isTrue);
    expect(
      sanitized.redactedKeys,
      containsAll(<String>{
        'encryption_key',
        'encryption_key_hex',
        'tde_key',
        'tde_key_hex',
      }),
    );
    expect(sanitized.redacted, contains('cache_mb=8'));
    expect(sanitized.redacted, contains('write_queue_enabled=true'));
    expect(sanitized.redacted, isNot(contains('plain')));
    expect(sanitized.redacted, isNot(contains('001122')));
    expect(sanitized.redacted, contains('encryption_key=<redacted>'));
    expect(sanitized.redacted, contains('tde_key_hex=<redacted>'));
  });

  test('redaction handles null and empty option strings', () {
    expect(redactSensitiveOpenOptions(null), isEmpty);
    expect(redactSensitiveOpenOptions(''), isEmpty);
    expect(sanitizeOpenOptions(null).hasRedactions, isFalse);
  });
}
