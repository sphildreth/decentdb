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
  return sql.replace(/\?/g, () => {
    i += 1;
    return `$${i}`;
  });
}

module.exports = { positionBindings };
