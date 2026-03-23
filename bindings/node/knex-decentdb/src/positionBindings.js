'use strict';

// Knex commonly produces SQL with `?` placeholders + a separate bindings array.
// DecentDB’s engine contract is Postgres-style positional parameters: $1, $2, ...
// This helper rewrites `?` → `$N` left-to-right.
//
// NOTE: This is a scaffold and does not attempt full SQL-string parsing.
// It assumes Knex-generated SQL where `?` only appears as a placeholder.
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
        out += "--";
        j += 1; // skip second -
        // consume until newline
        while (j + 1 < sql.length && sql[j+1] !== '\n') {
            j++;
            out += sql[j];
        }
      } else {
        out += c;
      }
    }
  }
  return out;
}

module.exports = { positionBindings };
