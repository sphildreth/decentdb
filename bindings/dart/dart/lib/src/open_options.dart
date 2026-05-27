const _sensitiveOpenOptionKeys = <String>{
  'encryption_key',
  'encryption_key_hex',
  'tde_key',
  'tde_key_hex',
};

/// Redacted view of a DecentDB native open-options string.
final class SanitizedOpenOptions {
  const SanitizedOpenOptions(this.redacted, this.redactedKeys);

  /// The open-options string with sensitive values replaced by `<redacted>`.
  final String redacted;

  /// Sensitive option keys that were found and redacted.
  final Set<String> redactedKeys;

  /// Whether any sensitive values were removed.
  bool get hasRedactions => redactedKeys.isNotEmpty;
}

/// Redact sensitive key material from a native open-options string.
///
/// The C ABI accepts options as `key=value` tokens separated by whitespace,
/// commas, or semicolons. This helper preserves token separators while replacing
/// values for `encryption_key`, `encryption_key_hex`, `tde_key`, and
/// `tde_key_hex` with `<redacted>`.
String redactSensitiveOpenOptions(String? options) {
  return sanitizeOpenOptions(options).redacted;
}

/// Return a structured redaction result for a native open-options string.
SanitizedOpenOptions sanitizeOpenOptions(String? options) {
  if (options == null || options.isEmpty) {
    return const SanitizedOpenOptions('', <String>{});
  }

  final redactedKeys = <String>{};
  final buffer = StringBuffer();
  var index = 0;
  while (index < options.length) {
    final separatorStart = index;
    while (index < options.length &&
        _isOptionSeparator(options.codeUnitAt(index))) {
      index++;
    }
    buffer.write(options.substring(separatorStart, index));
    if (index >= options.length) {
      break;
    }

    final tokenStart = index;
    while (index < options.length &&
        !_isOptionSeparator(options.codeUnitAt(index))) {
      index++;
    }
    final token = options.substring(tokenStart, index);
    final equals = token.indexOf('=');
    if (equals <= 0) {
      buffer.write(token);
      continue;
    }

    final key = token.substring(0, equals).trim().toLowerCase();
    if (_sensitiveOpenOptionKeys.contains(key)) {
      redactedKeys.add(key);
      buffer.write('${token.substring(0, equals + 1)}<redacted>');
    } else {
      buffer.write(token);
    }
  }

  return SanitizedOpenOptions(
      buffer.toString(), Set.unmodifiable(redactedKeys));
}

bool _isOptionSeparator(int codeUnit) {
  return codeUnit == 0x2c || // comma
      codeUnit == 0x3b || // semicolon
      codeUnit == 0x09 || // tab
      codeUnit == 0x0a || // LF
      codeUnit == 0x0d || // CR
      codeUnit == 0x20; // space
}
