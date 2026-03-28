'use strict';

// Knex commonly produces SQL with `?` placeholders + a separate bindings array.
// DecentDB's engine contract is Postgres-style positional parameters: $1, $2, ...
// This helper rewrites `?` -> `$N` left-to-right, skipping quoted strings,
// line comments (--), and block comments (/* */).
function positionBindings(sql) {
  if (typeof sql !== 'string') return sql;
  let i = 0;
  let out = '';
  let inString = false;
  let quoteChar = '';

  for (let j = 0; j < sql.length; j++) {
    const c = sql[j];
    if (inString) {
      out += c;
      if (c === quoteChar) {
        if (j + 1 < sql.length && sql[j+1] === quoteChar) {
          out += sql[j+1];
          j++;
        } else {
          inString = false;
        }
      }
    } else {
      if (c === "'" || c === '"') {
        inString = true;
        quoteChar = c;
        out += c;
      } else if (c === '?') {
        i += 1;
        out += `$${i}`;
      } else if (c === '-' && j + 1 < sql.length && sql[j+1] === '-') {
        // Line comment: copy through to end of line.
        out += "--";
        j += 1;
        while (j + 1 < sql.length && sql[j+1] !== '\n') {
          j++;
          out += sql[j];
        }
      } else if (c === '/' && j + 1 < sql.length && sql[j+1] === '*') {
        // Block comment: copy through to closing */.
        out += '/*';
        j += 2;
        while (j < sql.length) {
          if (sql[j] === '*' && j + 1 < sql.length && sql[j+1] === '/') {
            out += '*/';
            j += 1;
            break;
          }
          out += sql[j];
          j++;
        }
      } else {
        out += c;
      }
    }
  }
  return out;
}

module.exports = { positionBindings };
