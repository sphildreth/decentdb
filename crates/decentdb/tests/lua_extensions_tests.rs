use std::fs;
use std::path::Path;
use std::path::PathBuf;

use decentdb::{Db, DbConfig, ExtensionTrustAnchor, ExtensionValidationOptions, Value};
use std::env;

fn write_extension_package(root: &Path) {
    fs::create_dir_all(root).expect("create extension package");
    fs::write(
        root.join("decentdb-extension.toml"),
        r#"
name = "full_tools"
version = "1.0.0"
language = "lua"
api_version = 1
entry = "main.lua"
strict_types = true

[permissions]
filesystem = false
network = false
process = false
database_read = false
database_write = false
native_modules = false
clock = false
random = false

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"

[[functions]]
name = "split_words"
export = "split_words"
kind = "table"
args = ["TEXT"]

[[functions.columns]]
name = "word"
type = "TEXT"

[[functions]]
name = "lua_sum"
kind = "aggregate"
args = ["INT64"]
returns = "INT64"
step = "lua_sum_step"
finalize = "lua_sum_final"
null_handling = "called_on_null"

[[functions]]
name = "random_value"
export = "random_value"
kind = "scalar"
args = []
returns = "FLOAT64"
volatile = true

[[functions]]
name = "rev"
export = "rev"
kind = "collation"
deterministic = true
"#,
    )
    .expect("write manifest");
    fs::write(
        root.join("main.lua"),
        r#"
local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

function M.split_words(value)
  local rows = {}
  for word in string.gmatch(value or "", "%S+") do
    table.insert(rows, { word = word })
  end
  return rows
end

function M.lua_sum_step(state, value)
  return (state or 0) + (value or 0)
end

function M.lua_sum_final(state)
  return state or 0
end

function M.random_value()
  return math.random()
end

function M.rev(left, right)
  if left == right then return 0 end
  if left > right then return -1 end
  return 1
end

return M
"#,
    )
    .expect("write lua");
}

fn write_extension_main(root: &Path) {
    fs::write(
        root.join("main.lua"),
        r#"
local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

return M
"#,
    )
    .expect("write lua");
}

fn write_extension_manifest(root: &Path, manifest_toml: &str) {
    fs::write(root.join("decentdb-extension.toml"), manifest_toml).expect("write manifest");
}

fn test_manifest_dir(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let cwd = env::current_dir().expect("cwd");
    cwd.join(format!("{prefix}-phase4-{pid}"))
}

#[test]
fn lua_extension_lifecycle_and_sql_invocation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let package_path = temp.path().join("full_tools");
    write_extension_package(&package_path);
    let report = decentdb::validate_extension_package(
        &package_path,
        ExtensionValidationOptions::unsigned_development(),
    )
    .expect("validate package");
    assert!(report.valid);
    let hash = report.content_hash.expect("content hash");

    let db_path = temp.path().join("app.ddb");
    {
        let config = DbConfig {
            extension_unsigned_development_mode: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&db_path, config).expect("open db");
        db.extensions()
            .install_with_options(
                &package_path,
                ExtensionValidationOptions::unsigned_development(),
            )
            .expect("install extension");
        db.execute("CREATE EXTENSION full_tools")
            .expect("enable extension");
    }

    let mut config = DbConfig::default();
    config
        .extension_trust_anchors
        .push(ExtensionTrustAnchor::new("full_tools", hash));
    let db = Db::open_or_create(&db_path, config).expect("reopen with trust");

    let scalar = db
        .execute("SELECT slugify('Hello, World!')")
        .expect("scalar extension");
    assert_eq!(
        scalar.rows()[0].values(),
        &[Value::Text("hello-world".to_string())]
    );

    let random_error = db
        .execute("SELECT random_value()")
        .expect_err("math.random should be disabled");
    assert!(random_error.to_string().contains("math.random is disabled"));

    let table = db
        .execute("SELECT word FROM split_words('a bb c')")
        .expect("table extension");
    assert_eq!(table.rows().len(), 3);
    assert_eq!(table.rows()[1].values(), &[Value::Text("bb".to_string())]);

    db.execute_batch("CREATE TABLE nums(x INT64); INSERT INTO nums VALUES (1),(2),(3)")
        .expect("seed nums");
    let aggregate = db
        .execute("SELECT lua_sum(x) FROM nums")
        .expect("aggregate extension");
    assert_eq!(aggregate.rows()[0].values(), &[Value::Int64(6)]);

    let wrapped_aggregate = db
        .execute("SELECT COALESCE(lua_sum(x), 0) FROM nums")
        .expect("wrapped aggregate extension");
    assert_eq!(wrapped_aggregate.rows()[0].values(), &[Value::Int64(6)]);

    let collation = db
        .execute("SELECT 'b' COLLATE rev < 'a'")
        .expect("collation extension");
    assert_eq!(collation.rows()[0].values(), &[Value::Bool(true)]);

    db.execute_batch("CREATE TABLE words(name TEXT); INSERT INTO words VALUES ('a'), ('b'), ('c')")
        .expect("seed words");
    let ordered = db
        .execute("SELECT name FROM words ORDER BY name COLLATE rev")
        .expect("order by extension collation");
    assert_eq!(ordered.rows()[0].values(), &[Value::Text("c".to_string())]);
}

#[test]
fn enabled_extension_without_connection_trust_is_not_executable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let package_path = temp.path().join("full_tools");
    write_extension_package(&package_path);
    let db_path = temp.path().join("app.ddb");
    {
        let config = DbConfig {
            extension_unsigned_development_mode: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&db_path, config).expect("open db");
        db.extensions()
            .install_with_options(
                &package_path,
                ExtensionValidationOptions::unsigned_development(),
            )
            .expect("install extension");
        db.execute("CREATE EXTENSION full_tools")
            .expect("enable extension");
    }

    let db = Db::open_or_create(&db_path, DbConfig::default()).expect("reopen without trust");
    let error = db
        .execute("SELECT slugify('Hello')")
        .expect_err("extension should not run without trust");
    assert!(error
        .to_string()
        .contains("unsupported scalar function slugify"));
}

#[test]
fn malformed_extension_manifest_returns_typed_error_and_leaves_no_functions_registered() {
    let temp = tempfile::tempdir().expect("temp dir");
    let package_path = temp.path().join("missing-name");
    fs::create_dir_all(&package_path).expect("package dir");
    write_extension_manifest(
        &package_path,
        r#"
version = "1.0.0"
language = "lua"
api_version = 1
entry = "main.lua"
strict_types = true

[permissions]
filesystem = false
network = false
process = false
database_read = false
database_write = false
native_modules = false
clock = false
random = false

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"
"#,
    );
    write_extension_main(&package_path);

    let db_path = temp.path().join("app.ddb");
    let db = Db::create(&db_path, DbConfig::default()).expect("create db");
    let db_error = db
        .extensions()
        .install_with_options(
            &package_path,
            ExtensionValidationOptions::unsigned_development(),
        )
        .expect_err("missing manifest name should fail");
    assert_eq!(db_error.code(), decentdb::DbErrorCode::Sql);

    let extensions = db.extensions().list().expect("list extensions");
    assert!(
        extensions.is_empty(),
        "failed install should not register any extensions"
    );
}

#[test]
fn malformed_extension_permissions_returns_sql_typed_error() {
    let temp = tempfile::tempdir().expect("temp dir");
    let package_path = temp.path().join("malformed-permissions");
    fs::create_dir_all(&package_path).expect("package dir");
    write_extension_manifest(
        &package_path,
        r#"
name = "bad_permissions"
version = "1.0.0"
language = "lua"
api_version = 1
entry = "main.lua"
strict_types = true

[permissions]
filesystem = "no"
network = false
process = false
database_read = false
database_write = false
native_modules = false
clock = false
random = false

[[functions]]
name = "slugify"
export = "slugify"
kind = "scalar"
args = ["TEXT"]
returns = "TEXT"
deterministic = true
null_handling = "returns_null"
"#,
    );
    write_extension_main(&package_path);

    let db_path = temp.path().join("app.ddb");
    let db = Db::create(&db_path, DbConfig::default()).expect("create db");
    let db_error = db
        .extensions()
        .install_with_options(
            &package_path,
            ExtensionValidationOptions::unsigned_development(),
        )
        .expect_err("permissions type error should fail");
    assert_eq!(db_error.code(), decentdb::DbErrorCode::Sql);
    assert!(
        db_error
            .to_string()
            .contains("invalid extension manifest TOML"),
        "expected manifest parse error details: {db_error}"
    );
}

#[test]
fn invalid_extension_error_messages_use_callers_relative_package_path() {
    let package_path = test_manifest_dir("relative-extension-manifest");
    if package_path.exists() {
        fs::remove_dir_all(&package_path).expect("remove prior package");
    }
    fs::create_dir_all(&package_path).expect("package dir");
    let package_path_str = package_path.to_string_lossy().to_string();

    let report = decentdb::validate_extension_package(
        &package_path,
        ExtensionValidationOptions::unsigned_development(),
    )
    .expect("validation report");
    assert!(!report.valid);
    assert!(
        report
            .errors
            .iter()
            .any(|entry| entry.contains(&package_path_str)),
        "error should reference caller-provided path: {package_path:?}"
    );

    fs::remove_dir_all(&package_path).expect("cleanup package");
}
