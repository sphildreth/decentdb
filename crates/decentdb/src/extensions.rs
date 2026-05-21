//! Sandboxed Lua extension package support.
//!
//! The public API in this module is DecentDB-owned: callers never interact
//! with Lua runtime crate types directly. Native builds with the
//! `lua-extensions` feature execute packages through a sandboxed Lua 5.4 VM;
//! builds without that feature retain validation, catalog, trust, and lifecycle
//! APIs but reject execution with a SQL error.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::db::Db;
use crate::error::{DbError, Result};
use crate::exec::{QueryResult, QueryRow};
use crate::record::value::{
    format_date_days, format_timestamp_tz_micros, parse_date_days, parse_decimal_text,
    parse_timestamp_tz_micros, Value,
};

pub(crate) const PACKAGES_TABLE: &str = "__decentdb_extension_packages";
pub(crate) const FILES_TABLE: &str = "__decentdb_extension_files";
pub(crate) const ENABLED_TABLE: &str = "__decentdb_extension_enabled";
pub(crate) const DEPENDENCIES_TABLE: &str = "__decentdb_extension_dependencies";
pub const SUPPORTED_EXTENSION_API_VERSION: u32 = 1;

const MANIFEST_FILE: &str = "decentdb-extension.toml";
const EXTENSION_CATALOG_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS __decentdb_extension_packages (
    name TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    manifest TEXT NOT NULL,
    entry_source TEXT NOT NULL,
    package_json TEXT NOT NULL,
    installed_at_micros INT64 NOT NULL
);
CREATE TABLE IF NOT EXISTS __decentdb_extension_files (
    package_name TEXT NOT NULL,
    path TEXT NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY (package_name, path)
);
CREATE TABLE IF NOT EXISTS __decentdb_extension_enabled (
    name TEXT PRIMARY KEY,
    enabled_at_micros INT64 NOT NULL
);
CREATE TABLE IF NOT EXISTS __decentdb_extension_dependencies (
    object_kind TEXT NOT NULL,
    object_name TEXT NOT NULL,
    extension_name TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    dependency_kind TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    recorded_at_micros INT64 NOT NULL,
    PRIMARY KEY (object_kind, object_name, extension_name, dependency_name, dependency_kind)
);
"#;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionTrustAnchor {
    pub name: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,
}

impl ExtensionTrustAnchor {
    #[must_use]
    pub fn new(name: impl Into<String>, content_hash: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content_hash: normalize_content_hash(&content_hash.into()),
            key_id: None,
            public_key: None,
        }
    }

    #[must_use]
    pub fn with_public_key(
        name: impl Into<String>,
        content_hash: impl Into<String>,
        key_id: impl Into<String>,
        public_key: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            content_hash: normalize_content_hash(&content_hash.into()),
            key_id: Some(key_id.into()),
            public_key: Some(public_key.into()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionValidationOptions {
    #[serde(default)]
    pub allow_unsigned: bool,
    #[serde(default)]
    pub trust_anchors: Vec<ExtensionTrustAnchor>,
}

impl ExtensionValidationOptions {
    #[must_use]
    pub fn unsigned_development() -> Self {
        Self {
            allow_unsigned: true,
            trust_anchors: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionRuntimeLimits {
    #[serde(default = "default_max_steps")]
    pub max_steps: u64,
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_max_string_bytes")]
    pub max_string_bytes: usize,
    #[serde(default = "default_max_blob_bytes")]
    pub max_blob_bytes: usize,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default = "default_max_row_bytes")]
    pub max_row_bytes: usize,
    #[serde(default = "default_max_aggregate_state_bytes")]
    pub max_aggregate_state_bytes: usize,
    #[serde(default = "default_max_collation_steps")]
    pub max_collation_steps: u64,
}

impl Default for ExtensionRuntimeLimits {
    fn default() -> Self {
        Self {
            max_steps: default_max_steps(),
            max_memory_bytes: default_max_memory_bytes(),
            max_string_bytes: default_max_string_bytes(),
            max_blob_bytes: default_max_blob_bytes(),
            max_rows: default_max_rows(),
            max_row_bytes: default_max_row_bytes(),
            max_aggregate_state_bytes: default_max_aggregate_state_bytes(),
            max_collation_steps: default_max_collation_steps(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionPermissions {
    #[serde(default)]
    pub filesystem: bool,
    #[serde(default)]
    pub network: bool,
    #[serde(default)]
    pub process: bool,
    #[serde(default)]
    pub database_read: bool,
    #[serde(default)]
    pub database_write: bool,
    #[serde(default)]
    pub native_modules: bool,
    #[serde(default)]
    pub clock: bool,
    #[serde(default)]
    pub random: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionSignature {
    pub algorithm: String,
    pub key_id: String,
    pub signature: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionSqlType {
    Null,
    Bool,
    Text,
    Int64,
    Float64,
    Decimal,
    Uuid,
    Date,
    Timestamp,
    Blob,
    Json,
}

impl ExtensionSqlType {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "NULL" => Ok(Self::Null),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "TEXT" | "STRING" => Ok(Self::Text),
            "INT64" | "INTEGER" | "INT" | "BIGINT" => Ok(Self::Int64),
            "FLOAT64" | "FLOAT" | "DOUBLE" | "REAL" => Ok(Self::Float64),
            "DECIMAL" | "NUMERIC" => Ok(Self::Decimal),
            "UUID" => Ok(Self::Uuid),
            "DATE" => Ok(Self::Date),
            "TIMESTAMP" | "TIMESTAMPTZ" => Ok(Self::Timestamp),
            "BLOB" | "BYTEA" => Ok(Self::Blob),
            "JSON" => Ok(Self::Json),
            other => Err(DbError::sql(format!("unknown extension SQL type {other}"))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Bool => "BOOL",
            Self::Text => "TEXT",
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Decimal => "DECIMAL",
            Self::Uuid => "UUID",
            Self::Date => "DATE",
            Self::Timestamp => "TIMESTAMP",
            Self::Blob => "BLOB",
            Self::Json => "JSON",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionNullHandling {
    #[default]
    ReturnsNull,
    CalledOnNull,
    RejectsNull,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionFunctionKind {
    Scalar,
    Table,
    Aggregate,
    Collation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionColumnManifest {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionFunctionManifest {
    pub name: String,
    #[serde(default)]
    pub export: Option<String>,
    #[serde(default = "default_function_kind")]
    pub kind: ExtensionFunctionKind,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub returns: Option<String>,
    #[serde(default)]
    pub deterministic: bool,
    #[serde(default)]
    pub stable: bool,
    #[serde(default)]
    pub volatile: bool,
    #[serde(default)]
    pub null_handling: ExtensionNullHandling,
    #[serde(default)]
    pub columns: Vec<ExtensionColumnManifest>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub step: Option<String>,
    #[serde(default)]
    pub finalize: Option<String>,
}

impl ExtensionFunctionManifest {
    fn export_name(&self) -> &str {
        self.export.as_deref().unwrap_or(&self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionPackageDependency {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub content_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionPackageFile {
    pub path: String,
    pub sha256: String,
    pub bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub language: String,
    pub api_version: u32,
    pub entry: String,
    #[serde(default = "default_true")]
    pub strict_types: bool,
    #[serde(default)]
    pub signature: Option<ExtensionSignature>,
    #[serde(default)]
    pub runtime: ExtensionRuntimeLimits,
    #[serde(default)]
    pub permissions: ExtensionPermissions,
    #[serde(default)]
    pub dependencies: Vec<ExtensionPackageDependency>,
    #[serde(default)]
    pub functions: Vec<ExtensionFunctionManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionPackage {
    pub manifest: ExtensionManifest,
    pub manifest_toml: String,
    pub entry_source: String,
    pub content_hash: String,
    pub files: Vec<ExtensionPackageFile>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionValidationReport {
    pub valid: bool,
    pub name: Option<String>,
    pub version: Option<String>,
    pub content_hash: Option<String>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub functions: Vec<ExtensionFunctionManifest>,
    pub files: Vec<ExtensionPackageFile>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstalledExtensionPackage {
    pub name: String,
    pub version: String,
    pub content_hash: String,
    pub enabled: bool,
    pub installed_at_micros: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionDependencyRecord {
    pub object_kind: String,
    pub object_name: String,
    pub extension_name: String,
    pub dependency_name: String,
    pub dependency_kind: String,
    pub content_hash: String,
    pub recorded_at_micros: i64,
}

pub trait ExtensionSignatureVerifier {
    fn verify(
        &self,
        manifest: &ExtensionManifest,
        content_hash: &str,
        trust_anchors: &[ExtensionTrustAnchor],
    ) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Ed25519SignatureVerifier;

impl ExtensionSignatureVerifier for Ed25519SignatureVerifier {
    fn verify(
        &self,
        manifest: &ExtensionManifest,
        content_hash: &str,
        trust_anchors: &[ExtensionTrustAnchor],
    ) -> Result<()> {
        let signature = manifest
            .signature
            .as_ref()
            .ok_or_else(|| DbError::sql("extension package is unsigned"))?;
        if !signature.algorithm.eq_ignore_ascii_case("ed25519") {
            return Err(DbError::sql(format!(
                "unsupported extension signature algorithm {}",
                signature.algorithm
            )));
        }
        let anchor = trust_anchors
            .iter()
            .find(|anchor| {
                identifiers_equal(&anchor.name, &manifest.name)
                    && normalize_content_hash(&anchor.content_hash)
                        == normalize_content_hash(content_hash)
                    && anchor
                        .key_id
                        .as_deref()
                        .is_none_or(|key_id| key_id == signature.key_id)
            })
            .ok_or_else(|| {
                DbError::sql(format!(
                    "extension {}@{} is not trusted by the current validation policy",
                    manifest.name, content_hash
                ))
            })?;
        let public_key = anchor.public_key.as_ref().ok_or_else(|| {
            DbError::sql(format!(
                "trust anchor for extension {} does not include an Ed25519 public key",
                manifest.name
            ))
        })?;
        verify_ed25519_signature(public_key, &signature.signature, content_hash.as_bytes())
    }
}

#[derive(Clone, Copy)]
pub struct ExtensionManager<'a> {
    db: &'a Db,
}

impl<'a> ExtensionManager<'a> {
    #[must_use]
    pub(crate) fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub fn validate_package(
        &self,
        path: impl AsRef<Path>,
        options: ExtensionValidationOptions,
    ) -> Result<ExtensionValidationReport> {
        validate_package(path.as_ref(), &options)
    }

    pub fn install(&self, path: impl AsRef<Path>) -> Result<InstalledExtensionPackage> {
        self.install_with_options(path, ExtensionValidationOptions::default())
    }

    pub fn install_with_options(
        &self,
        path: impl AsRef<Path>,
        options: ExtensionValidationOptions,
    ) -> Result<InstalledExtensionPackage> {
        let package = load_package(path.as_ref())?;
        validate_loaded_package(&package, &options)?;
        self.ensure_catalog()?;
        let now = now_micros()?;
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_files WHERE package_name = $1",
            &[Value::Text(package.manifest.name.clone())],
        )?;
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_packages WHERE name = $1",
            &[Value::Text(package.manifest.name.clone())],
        )?;
        self.db.execute_with_params(
            "INSERT INTO __decentdb_extension_packages(name, version, content_hash, manifest, entry_source, package_json, installed_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                Value::Text(package.manifest.name.clone()),
                Value::Text(package.manifest.version.clone()),
                Value::Text(package.content_hash.clone()),
                Value::Text(package.manifest_toml.clone()),
                Value::Text(package.entry_source.clone()),
                Value::Text(serde_json::to_string(&package).map_err(|error| {
                    DbError::internal(format!("failed to encode extension package JSON: {error}"))
                })?),
                Value::Int64(now),
            ],
        )?;
        for file in &package.files {
            self.db.execute_with_params(
                "INSERT INTO __decentdb_extension_files(package_name, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(package.manifest.name.clone()),
                    Value::Text(file.path.clone()),
                    Value::Text(file.sha256.clone()),
                ],
            )?;
        }
        Ok(InstalledExtensionPackage {
            name: package.manifest.name,
            version: package.manifest.version,
            content_hash: package.content_hash,
            enabled: false,
            installed_at_micros: now,
        })
    }

    pub fn enable(&self, name: &str) -> Result<()> {
        validate_identifier(name, "extension name")?;
        self.ensure_catalog()?;
        if self.installed_package(name)?.is_none() {
            return Err(DbError::sql(format!(
                "extension {name} is not installed; install the package before enabling it"
            )));
        }
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_enabled WHERE name = $1",
            &[Value::Text(name.to_string())],
        )?;
        self.db.execute_with_params(
            "INSERT INTO __decentdb_extension_enabled(name, enabled_at_micros) VALUES ($1, $2)",
            &[Value::Text(name.to_string()), Value::Int64(now_micros()?)],
        )?;
        Ok(())
    }

    pub fn disable(&self, name: &str) -> Result<()> {
        validate_identifier(name, "extension name")?;
        if !self.db.internal_table_exists(ENABLED_TABLE)? {
            return Ok(());
        }
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_enabled WHERE name = $1",
            &[Value::Text(name.to_string())],
        )?;
        Ok(())
    }

    pub fn purge(&self, name: &str) -> Result<()> {
        validate_identifier(name, "extension name")?;
        if !self.db.internal_table_exists(PACKAGES_TABLE)? {
            return Ok(());
        }
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_enabled WHERE name = $1",
            &[Value::Text(name.to_string())],
        )?;
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_files WHERE package_name = $1",
            &[Value::Text(name.to_string())],
        )?;
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_dependencies WHERE extension_name = $1",
            &[Value::Text(name.to_string())],
        )?;
        self.db.execute_with_params(
            "DELETE FROM __decentdb_extension_packages WHERE name = $1",
            &[Value::Text(name.to_string())],
        )?;
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<InstalledExtensionPackage>> {
        if !self.db.internal_table_exists(PACKAGES_TABLE)? {
            return Ok(Vec::new());
        }
        let result = self.db.execute(
            "SELECT p.name, p.version, p.content_hash, p.installed_at_micros,
                    CASE WHEN e.name IS NULL THEN 0 ELSE 1 END
             FROM __decentdb_extension_packages p
             LEFT JOIN __decentdb_extension_enabled e ON e.name = p.name
             ORDER BY p.name",
        )?;
        result
            .rows()
            .iter()
            .map(|row| installed_package_from_values(row.values()))
            .collect()
    }

    pub fn show(&self, name: &str) -> Result<Option<ExtensionPackage>> {
        validate_identifier(name, "extension name")?;
        self.installed_package(name)
    }

    pub fn dependencies(&self) -> Result<Vec<ExtensionDependencyRecord>> {
        if !self.db.internal_table_exists(DEPENDENCIES_TABLE)? {
            return Ok(Vec::new());
        }
        let result = self.db.execute(
            "SELECT object_kind, object_name, extension_name, dependency_name,
                    dependency_kind, content_hash, recorded_at_micros
             FROM __decentdb_extension_dependencies
             ORDER BY object_kind, object_name, extension_name, dependency_name",
        )?;
        result
            .rows()
            .iter()
            .map(|row| dependency_from_values(row.values()))
            .collect()
    }

    pub fn rebuild_dependents(&self, name: &str) -> Result<Vec<ExtensionDependencyRecord>> {
        validate_identifier(name, "extension name")?;
        // DecentDB records dependency metadata for deterministic persisted
        // objects. The current rebuild action reports affected objects; native
        // index/view rebuilds are delegated to existing explicit DDL surfaces.
        Ok(self
            .dependencies()?
            .into_iter()
            .filter(|record| identifiers_equal(&record.extension_name, name))
            .collect())
    }

    pub(crate) fn ensure_catalog(&self) -> Result<()> {
        self.db.execute_batch(EXTENSION_CATALOG_DDL)?;
        Ok(())
    }

    fn installed_package(&self, name: &str) -> Result<Option<ExtensionPackage>> {
        if !self.db.internal_table_exists(PACKAGES_TABLE)? {
            return Ok(None);
        }
        let result = self.db.execute_with_params(
            "SELECT package_json FROM __decentdb_extension_packages WHERE name = $1",
            &[Value::Text(name.to_string())],
        )?;
        let Some(row) = result.rows().first() else {
            return Ok(None);
        };
        let Some(Value::Text(package_json)) = row.values().first() else {
            return Err(DbError::corruption(
                "extension package catalog row has invalid package_json",
            ));
        };
        serde_json::from_str(package_json)
            .map(Some)
            .map_err(|error| {
                DbError::corruption(format!("invalid installed extension package JSON: {error}"))
            })
    }
}

pub(crate) enum ExtensionSqlCommand {
    Create { name: String },
    Drop { name: String },
    Enable { name: String },
    Disable { name: String },
}

pub(crate) fn parse_extension_sql(sql: &str) -> Result<Option<ExtensionSqlCommand>> {
    let tokens = sql_tokens(sql);
    if tokens.len() == 3
        && tokens[0].eq_ignore_ascii_case("CREATE")
        && tokens[1].eq_ignore_ascii_case("EXTENSION")
    {
        validate_identifier(&tokens[2], "extension name")?;
        return Ok(Some(ExtensionSqlCommand::Create {
            name: tokens[2].clone(),
        }));
    }
    if tokens.len() == 3
        && tokens[0].eq_ignore_ascii_case("DROP")
        && tokens[1].eq_ignore_ascii_case("EXTENSION")
    {
        validate_identifier(&tokens[2], "extension name")?;
        return Ok(Some(ExtensionSqlCommand::Drop {
            name: tokens[2].clone(),
        }));
    }
    if tokens.len() == 5
        && tokens[0].eq_ignore_ascii_case("ALTER")
        && tokens[1].eq_ignore_ascii_case("EXTENSION")
        && tokens[3].eq_ignore_ascii_case("ENABLE")
    {
        validate_identifier(&tokens[2], "extension name")?;
        return Ok(Some(ExtensionSqlCommand::Enable {
            name: tokens[2].clone(),
        }));
    }
    if tokens.len() == 5
        && tokens[0].eq_ignore_ascii_case("ALTER")
        && tokens[1].eq_ignore_ascii_case("EXTENSION")
        && tokens[3].eq_ignore_ascii_case("DISABLE")
    {
        validate_identifier(&tokens[2], "extension name")?;
        return Ok(Some(ExtensionSqlCommand::Disable {
            name: tokens[2].clone(),
        }));
    }
    Ok(None)
}

pub(crate) fn extension_catalog_table_names() -> [&'static str; 4] {
    [
        PACKAGES_TABLE,
        FILES_TABLE,
        ENABLED_TABLE,
        DEPENDENCIES_TABLE,
    ]
}

pub(crate) fn execute_extension_sql(db: &Db, command: ExtensionSqlCommand) -> Result<QueryResult> {
    match command {
        ExtensionSqlCommand::Create { name } | ExtensionSqlCommand::Enable { name } => {
            db.extensions().enable(&name)?;
        }
        ExtensionSqlCommand::Drop { name } | ExtensionSqlCommand::Disable { name } => {
            db.extensions().disable(&name)?;
        }
    }
    Ok(QueryResult::with_affected_rows(0))
}

pub(crate) fn try_execute_extension_inspection_query(
    db: &Db,
    sql: &str,
    params: &[Value],
) -> Result<Option<QueryResult>> {
    let normalized = normalize_inspection_sql(sql);
    let Some(view) = normalized.strip_prefix("select * from sys.") else {
        return Ok(None);
    };
    if !params.is_empty() {
        return Err(DbError::sql(
            "extension inspection views do not accept parameters",
        ));
    }
    match view {
        "extensions" => extension_list_query_result(db).map(Some),
        "extension_functions" => extension_functions_query_result(db).map(Some),
        "extension_collations" => extension_collations_query_result(db).map(Some),
        "extension_dependencies" => extension_dependencies_query_result(db).map(Some),
        "extension_validation" => extension_validation_query_result(db).map(Some),
        _ => Ok(None),
    }
}

fn extension_list_query_result(db: &Db) -> Result<QueryResult> {
    let rows = db
        .extensions()
        .list()?
        .into_iter()
        .map(|package| {
            QueryRow::new(vec![
                Value::Text(package.name),
                Value::Text(package.version),
                Value::Text(package.content_hash),
                Value::Bool(package.enabled),
                Value::Int64(package.installed_at_micros),
            ])
        })
        .collect();
    Ok(QueryResult::with_rows(
        vec![
            "name".to_string(),
            "version".to_string(),
            "content_hash".to_string(),
            "enabled".to_string(),
            "installed_at_micros".to_string(),
        ],
        rows,
    ))
}

fn extension_functions_query_result(db: &Db) -> Result<QueryResult> {
    let mut rows = Vec::new();
    for package in db.extensions().list()? {
        if let Some(package_json) = db.extensions().show(&package.name)? {
            for function in package_json.manifest.functions {
                rows.push(QueryRow::new(vec![
                    Value::Text(package_json.manifest.name.clone()),
                    Value::Text(package_json.content_hash.clone()),
                    Value::Text(function.name),
                    Value::Text(function.export.unwrap_or_default()),
                    Value::Text(format!("{:?}", function.kind).to_ascii_lowercase()),
                    Value::Text(function.args.join(",")),
                    function.returns.map_or(Value::Null, Value::Text),
                    Value::Bool(function.deterministic),
                    Value::Text(format!("{:?}", function.null_handling).to_ascii_lowercase()),
                ]));
            }
        }
    }
    Ok(QueryResult::with_rows(
        vec![
            "extension_name".to_string(),
            "content_hash".to_string(),
            "function_name".to_string(),
            "export".to_string(),
            "kind".to_string(),
            "args".to_string(),
            "returns".to_string(),
            "deterministic".to_string(),
            "null_handling".to_string(),
        ],
        rows,
    ))
}

fn extension_collations_query_result(db: &Db) -> Result<QueryResult> {
    let mut rows = Vec::new();
    for package in db.extensions().list()? {
        if let Some(package_json) = db.extensions().show(&package.name)? {
            for function in package_json
                .manifest
                .functions
                .into_iter()
                .filter(|function| function.kind == ExtensionFunctionKind::Collation)
            {
                rows.push(QueryRow::new(vec![
                    Value::Text(package_json.manifest.name.clone()),
                    Value::Text(package_json.content_hash.clone()),
                    Value::Text(function.name),
                    Value::Text(function.export.unwrap_or_default()),
                    Value::Bool(function.deterministic),
                ]));
            }
        }
    }
    Ok(QueryResult::with_rows(
        vec![
            "extension_name".to_string(),
            "content_hash".to_string(),
            "collation_name".to_string(),
            "export".to_string(),
            "deterministic".to_string(),
        ],
        rows,
    ))
}

fn extension_dependencies_query_result(db: &Db) -> Result<QueryResult> {
    let rows = db
        .extensions()
        .dependencies()?
        .into_iter()
        .map(|record| {
            QueryRow::new(vec![
                Value::Text(record.object_kind),
                Value::Text(record.object_name),
                Value::Text(record.extension_name),
                Value::Text(record.dependency_name),
                Value::Text(record.dependency_kind),
                Value::Text(record.content_hash),
                Value::Int64(record.recorded_at_micros),
            ])
        })
        .collect();
    Ok(QueryResult::with_rows(
        vec![
            "object_kind".to_string(),
            "object_name".to_string(),
            "extension_name".to_string(),
            "dependency_name".to_string(),
            "dependency_kind".to_string(),
            "content_hash".to_string(),
            "recorded_at_micros".to_string(),
        ],
        rows,
    ))
}

fn extension_validation_query_result(db: &Db) -> Result<QueryResult> {
    let mut rows = Vec::new();
    for package in db.extensions().list()? {
        let errors = validate_installed_package(db, &package.name)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        rows.push(QueryRow::new(vec![
            Value::Text(package.name),
            Value::Bool(errors.is_empty()),
            if errors.is_empty() {
                Value::Null
            } else {
                Value::Text(errors)
            },
        ]));
    }
    Ok(QueryResult::with_rows(
        vec!["name".to_string(), "valid".to_string(), "error".to_string()],
        rows,
    ))
}

fn validate_installed_package(db: &Db, name: &str) -> Result<()> {
    let package = db
        .extensions()
        .show(name)?
        .ok_or_else(|| DbError::sql(format!("extension {name} is not installed")))?;
    validate_loaded_package(
        &package,
        &ExtensionValidationOptions {
            allow_unsigned: true,
            trust_anchors: Vec::new(),
        },
    )
}

pub fn validate_package(
    path: &Path,
    options: &ExtensionValidationOptions,
) -> Result<ExtensionValidationReport> {
    match load_package(path).and_then(|package| {
        validate_loaded_package(&package, options)?;
        Ok(package)
    }) {
        Ok(package) => Ok(ExtensionValidationReport {
            valid: true,
            name: Some(package.manifest.name),
            version: Some(package.manifest.version),
            content_hash: Some(package.content_hash),
            errors: Vec::new(),
            warnings: Vec::new(),
            functions: package.manifest.functions,
            files: package.files,
        }),
        Err(error) => Ok(ExtensionValidationReport {
            valid: false,
            name: None,
            version: None,
            content_hash: None,
            errors: vec![error.to_string()],
            warnings: Vec::new(),
            functions: Vec::new(),
            files: Vec::new(),
        }),
    }
}

pub fn validate_extension_package(
    path: impl AsRef<Path>,
    options: ExtensionValidationOptions,
) -> Result<ExtensionValidationReport> {
    validate_package(path.as_ref(), &options)
}

fn load_package(path: &Path) -> Result<ExtensionPackage> {
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest_toml = fs::read_to_string(&manifest_path).map_err(|error| {
        DbError::io(
            format!(
                "failed to read extension manifest {}",
                manifest_path.display()
            ),
            error,
        )
    })?;
    let manifest: ExtensionManifest = toml::from_str(&manifest_toml)
        .map_err(|error| DbError::sql(format!("invalid extension manifest TOML: {error}")))?;
    let entry_path = path.join(&manifest.entry);
    let entry_source = fs::read_to_string(&entry_path).map_err(|error| {
        DbError::io(
            format!("failed to read extension entry {}", entry_path.display()),
            error,
        )
    })?;
    let files = collect_package_files(path)?;
    let content_hash = package_content_hash(&manifest_toml, &files)?;
    Ok(ExtensionPackage {
        manifest,
        manifest_toml,
        entry_source,
        content_hash,
        files,
    })
}

fn validate_loaded_package(
    package: &ExtensionPackage,
    options: &ExtensionValidationOptions,
) -> Result<()> {
    validate_identifier(&package.manifest.name, "extension name")?;
    if !package.manifest.language.eq_ignore_ascii_case("lua") {
        return Err(DbError::sql("extension language must be lua"));
    }
    if package.manifest.api_version != SUPPORTED_EXTENSION_API_VERSION {
        return Err(DbError::sql(format!(
            "unsupported extension API version {}; supported version is {}",
            package.manifest.api_version, SUPPORTED_EXTENSION_API_VERSION
        )));
    }
    if !package.manifest.strict_types {
        return Err(DbError::sql(
            "extension manifests must set strict_types = true",
        ));
    }
    if package.manifest.permissions.filesystem
        || package.manifest.permissions.network
        || package.manifest.permissions.process
        || package.manifest.permissions.database_read
        || package.manifest.permissions.database_write
        || package.manifest.permissions.native_modules
        || package.manifest.permissions.clock
        || package.manifest.permissions.random
    {
        return Err(DbError::sql(
            "extension permissions must remain disabled in the DecentDB 2.6 Lua sandbox",
        ));
    }
    validate_functions(&package.manifest.functions)?;
    if package.manifest.signature.is_some() {
        Ed25519SignatureVerifier.verify(
            &package.manifest,
            &package.content_hash,
            &options.trust_anchors,
        )?;
    } else if !options.allow_unsigned {
        return Err(DbError::sql(
            "extension package is unsigned; pass an explicit unsigned-development override to install or validate it",
        ));
    }
    validate_lua_exports(package)?;
    Ok(())
}

fn validate_functions(functions: &[ExtensionFunctionManifest]) -> Result<()> {
    let mut names = BTreeSet::new();
    for function in functions {
        validate_identifier(&function.name, "extension function name")?;
        validate_identifier(function.export_name(), "Lua export name")?;
        if !names.insert((function.kind, function.name.to_ascii_lowercase())) {
            return Err(DbError::sql(format!(
                "duplicate extension function {}",
                function.name
            )));
        }
        let volatility_count = [function.deterministic, function.stable, function.volatile]
            .into_iter()
            .filter(|value| *value)
            .count();
        if volatility_count > 1 {
            return Err(DbError::sql(format!(
                "extension function {} declares multiple volatility categories",
                function.name
            )));
        }
        for arg in &function.args {
            ExtensionSqlType::parse(arg)?;
        }
        match function.kind {
            ExtensionFunctionKind::Scalar => {
                let returns = function.returns.as_ref().ok_or_else(|| {
                    DbError::sql(format!(
                        "scalar extension function {} must declare returns",
                        function.name
                    ))
                })?;
                ExtensionSqlType::parse(returns)?;
            }
            ExtensionFunctionKind::Table => {
                if function.columns.is_empty() {
                    return Err(DbError::sql(format!(
                        "table-valued extension function {} must declare columns",
                        function.name
                    )));
                }
                for column in &function.columns {
                    validate_identifier(&column.name, "extension table-valued column name")?;
                    ExtensionSqlType::parse(&column.column_type)?;
                }
            }
            ExtensionFunctionKind::Aggregate => {
                let returns = function.returns.as_ref().ok_or_else(|| {
                    DbError::sql(format!(
                        "aggregate extension function {} must declare returns",
                        function.name
                    ))
                })?;
                ExtensionSqlType::parse(returns)?;
                if function.step.is_none() || function.finalize.is_none() {
                    return Err(DbError::sql(format!(
                        "aggregate extension function {} must declare step and finalize exports",
                        function.name
                    )));
                }
            }
            ExtensionFunctionKind::Collation => {
                if !function.deterministic {
                    return Err(DbError::sql(format!(
                        "collation extension function {} must be deterministic",
                        function.name
                    )));
                }
                if !function.args.is_empty() || function.returns.is_some() {
                    return Err(DbError::sql(format!(
                        "collation extension function {} must not declare args or returns",
                        function.name
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_lua_exports(package: &ExtensionPackage) -> Result<()> {
    #[cfg(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    {
        lua_support::validate_exports(package)
    }
    #[cfg(not(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )))]
    {
        let _ = package;
        Ok(())
    }
}

pub(crate) fn invoke_scalar_from_runtime(
    runtime: &crate::exec::EngineRuntime,
    name: &str,
    args: &[Value],
) -> Result<Option<Value>> {
    let registry = RuntimeExtensionRegistry::from_runtime(runtime)?;
    let Some(binding) = registry.scalar(name)? else {
        return Ok(None);
    };
    if !registry.is_allowed(&binding.package) {
        return Err(DbError::sql(format!(
            "extension {}@{} is enabled but not allowed by this connection",
            binding.package.manifest.name, binding.package.content_hash
        )));
    }
    enforce_null_handling(&binding.function, args)?;
    if binding.function.null_handling == ExtensionNullHandling::ReturnsNull
        && args.iter().any(|value| matches!(value, Value::Null))
    {
        return Ok(Some(Value::Null));
    }
    #[cfg(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    {
        lua_support::invoke_scalar(&binding.package, &binding.function, args).map(Some)
    }
    #[cfg(not(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )))]
    {
        let _ = (binding, args);
        Err(DbError::sql(
            "Lua extension execution is not available in this DecentDB build",
        ))
    }
}

pub(crate) fn evaluate_table_function_from_runtime(
    runtime: &crate::exec::EngineRuntime,
    name: &str,
    args: Vec<Value>,
    table_name: String,
) -> Result<Option<crate::exec::Dataset>> {
    let registry = RuntimeExtensionRegistry::from_runtime(runtime)?;
    let Some(binding) = registry.table_function(name)? else {
        return Ok(None);
    };
    if !registry.is_allowed(&binding.package) {
        return Err(DbError::sql(format!(
            "extension {}@{} is enabled but not allowed by this connection",
            binding.package.manifest.name, binding.package.content_hash
        )));
    }
    enforce_null_handling(&binding.function, &args)?;
    #[cfg(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    {
        lua_support::invoke_table(&binding.package, &binding.function, &args, table_name).map(Some)
    }
    #[cfg(not(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )))]
    {
        let _ = (binding, args, table_name);
        Err(DbError::sql(
            "Lua extension execution is not available in this DecentDB build",
        ))
    }
}

pub(crate) fn invoke_aggregate_from_runtime(
    runtime: &crate::exec::EngineRuntime,
    name: &str,
    arg_rows: Vec<Vec<Value>>,
) -> Result<Option<Value>> {
    let registry = RuntimeExtensionRegistry::from_runtime(runtime)?;
    let Some(binding) = registry.aggregate(name)? else {
        return Ok(None);
    };
    if !registry.is_allowed(&binding.package) {
        return Err(DbError::sql(format!(
            "extension {}@{} is enabled but not allowed by this connection",
            binding.package.manifest.name, binding.package.content_hash
        )));
    }
    #[cfg(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    {
        lua_support::invoke_aggregate(&binding.package, &binding.function, arg_rows).map(Some)
    }
    #[cfg(not(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )))]
    {
        let _ = (binding, arg_rows);
        Err(DbError::sql(
            "Lua extension execution is not available in this DecentDB build",
        ))
    }
}

pub(crate) fn runtime_has_aggregate_function(
    runtime: &crate::exec::EngineRuntime,
    name: &str,
) -> Result<bool> {
    RuntimeExtensionRegistry::from_runtime(runtime)?
        .aggregate(name)
        .map(|binding| binding.is_some())
}

pub(crate) fn compare_with_collation_from_runtime(
    runtime: &crate::exec::EngineRuntime,
    collation_name: &str,
    left: &str,
    right: &str,
) -> Result<Option<std::cmp::Ordering>> {
    let registry = RuntimeExtensionRegistry::from_runtime(runtime)?;
    let Some(binding) = registry.collation(collation_name)? else {
        return Ok(None);
    };
    if !registry.is_allowed(&binding.package) {
        return Err(DbError::sql(format!(
            "extension {}@{} is enabled but not allowed by this connection",
            binding.package.manifest.name, binding.package.content_hash
        )));
    }
    #[cfg(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    {
        lua_support::invoke_collation(&binding.package, &binding.function, left, right).map(Some)
    }
    #[cfg(not(all(
        feature = "lua-extensions",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )))]
    {
        let _ = (binding, left, right);
        Err(DbError::sql(
            "Lua extension execution is not available in this DecentDB build",
        ))
    }
}

#[derive(Clone)]
struct RuntimeBinding {
    package: ExtensionPackage,
    function: ExtensionFunctionManifest,
}

struct RuntimeExtensionRegistry {
    enabled: BTreeMap<String, ExtensionPackage>,
    allowlist: Vec<ExtensionTrustAnchor>,
    allow_unsigned_development: bool,
}

impl RuntimeExtensionRegistry {
    fn from_runtime(runtime: &crate::exec::EngineRuntime) -> Result<Self> {
        let enabled_names = runtime_enabled_extension_names(runtime)?;
        if enabled_names.is_empty() {
            return Ok(Self {
                enabled: BTreeMap::new(),
                allowlist: runtime.extension_trust_anchors.as_ref().clone(),
                allow_unsigned_development: runtime.extension_unsigned_development_mode,
            });
        }
        let packages = runtime_installed_packages(runtime)?;
        let enabled = enabled_names
            .into_iter()
            .filter_map(|name| {
                packages
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .map(|package| (name.to_ascii_lowercase(), package))
            })
            .collect();
        Ok(Self {
            enabled,
            allowlist: runtime.extension_trust_anchors.as_ref().clone(),
            allow_unsigned_development: runtime.extension_unsigned_development_mode,
        })
    }

    fn scalar(&self, name: &str) -> Result<Option<RuntimeBinding>> {
        self.find_function(name, ExtensionFunctionKind::Scalar)
    }

    fn table_function(&self, name: &str) -> Result<Option<RuntimeBinding>> {
        self.find_function(name, ExtensionFunctionKind::Table)
    }

    fn aggregate(&self, name: &str) -> Result<Option<RuntimeBinding>> {
        self.find_function(name, ExtensionFunctionKind::Aggregate)
    }

    fn collation(&self, name: &str) -> Result<Option<RuntimeBinding>> {
        self.find_function(name, ExtensionFunctionKind::Collation)
    }

    fn find_function(
        &self,
        name: &str,
        kind: ExtensionFunctionKind,
    ) -> Result<Option<RuntimeBinding>> {
        let mut matched = None;
        for package in self.enabled.values() {
            for function in &package.manifest.functions {
                if function.kind == kind && identifiers_equal(&function.name, name) {
                    if matched.is_some() {
                        return Err(DbError::sql(format!(
                            "ambiguous Lua extension function {name}"
                        )));
                    }
                    matched = Some(RuntimeBinding {
                        package: package.clone(),
                        function: function.clone(),
                    });
                }
            }
        }
        Ok(matched)
    }

    fn is_allowed(&self, package: &ExtensionPackage) -> bool {
        self.allow_unsigned_development
            || self.allowlist.iter().any(|anchor| {
                identifiers_equal(&anchor.name, &package.manifest.name)
                    && normalize_content_hash(&anchor.content_hash)
                        == normalize_content_hash(&package.content_hash)
            })
    }
}

fn runtime_enabled_extension_names(
    runtime: &crate::exec::EngineRuntime,
) -> Result<BTreeSet<String>> {
    let Some(source) = runtime.tables.get(ENABLED_TABLE) else {
        return Ok(BTreeSet::new());
    };
    let table = runtime
        .catalog
        .table(ENABLED_TABLE)
        .ok_or_else(|| DbError::corruption("extension enabled catalog table is missing schema"))?;
    let name_index = column_index(table, "name")?;
    let mut names = BTreeSet::new();
    for row in source.rows() {
        let row = row?;
        if let Some(Value::Text(name)) = row.values().get(name_index) {
            names.insert(name.to_ascii_lowercase());
        }
    }
    Ok(names)
}

fn runtime_installed_packages(
    runtime: &crate::exec::EngineRuntime,
) -> Result<BTreeMap<String, ExtensionPackage>> {
    let Some(source) = runtime.tables.get(PACKAGES_TABLE) else {
        return Ok(BTreeMap::new());
    };
    let table = runtime
        .catalog
        .table(PACKAGES_TABLE)
        .ok_or_else(|| DbError::corruption("extension package catalog table is missing schema"))?;
    let name_index = column_index(table, "name")?;
    let package_json_index = column_index(table, "package_json")?;
    let mut packages = BTreeMap::new();
    for row in source.rows() {
        let row = row?;
        let Some(Value::Text(name)) = row.values().get(name_index) else {
            return Err(DbError::corruption(
                "extension package row has invalid name column",
            ));
        };
        let Some(Value::Text(package_json)) = row.values().get(package_json_index) else {
            return Err(DbError::corruption(
                "extension package row has invalid package_json column",
            ));
        };
        let package: ExtensionPackage = serde_json::from_str(package_json).map_err(|error| {
            DbError::corruption(format!("invalid installed extension package JSON: {error}"))
        })?;
        packages.insert(name.to_ascii_lowercase(), package);
    }
    Ok(packages)
}

fn column_index(table: &crate::catalog::TableSchema, column_name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| identifiers_equal(&column.name, column_name))
        .ok_or_else(|| {
            DbError::corruption(format!(
                "extension catalog table {} is missing column {column_name}",
                table.name
            ))
        })
}

fn enforce_null_handling(function: &ExtensionFunctionManifest, args: &[Value]) -> Result<()> {
    if function.null_handling == ExtensionNullHandling::RejectsNull
        && args.iter().any(|value| matches!(value, Value::Null))
    {
        return Err(DbError::sql(format!(
            "extension function {} rejects NULL arguments",
            function.name
        )));
    }
    Ok(())
}

fn collect_package_files(root: &Path) -> Result<Vec<ExtensionPackageFile>> {
    let mut paths = Vec::new();
    collect_package_paths(root, root, &mut paths)?;
    paths.sort();
    let mut files = Vec::new();
    for relative in paths {
        let absolute = root.join(&relative);
        let bytes = fs::read(&absolute).map_err(|error| {
            DbError::io(
                format!(
                    "failed to read extension package file {}",
                    absolute.display()
                ),
                error,
            )
        })?;
        files.push(ExtensionPackageFile {
            path: relative.to_string_lossy().replace('\\', "/"),
            sha256: format!("sha256:{}", hex_sha256(&bytes)),
            bytes: bytes.len(),
        });
    }
    Ok(files)
}

fn collect_package_paths(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)
        .map_err(|error| DbError::io(format!("failed to read {}", dir.display()), error))?
    {
        let entry = entry
            .map_err(|error| DbError::io(format!("failed to read {}", dir.display()), error))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect_package_paths(root, &path, out)?;
        } else if path.is_file() {
            let relative = path.strip_prefix(root).map_err(|_| {
                DbError::internal("extension package file was not under package root")
            })?;
            out.push(relative.to_path_buf());
        }
    }
    Ok(())
}

fn package_content_hash(manifest_toml: &str, files: &[ExtensionPackageFile]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"decentdb-extension-v1\0");
    hasher.update(MANIFEST_FILE.as_bytes());
    hasher.update(b"\0");
    hasher.update(manifest_toml.as_bytes());
    hasher.update(b"\0");
    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update(b"\0");
        hasher.update(file.sha256.as_bytes());
        hasher.update(b"\0");
        hasher.update(file.bytes.to_string().as_bytes());
        hasher.update(b"\0");
    }
    Ok(format!("sha256:{}", hex_bytes(&hasher.finalize())))
}

fn installed_package_from_values(values: &[Value]) -> Result<InstalledExtensionPackage> {
    if values.len() != 5 {
        return Err(DbError::corruption(
            "extension package query returned an unexpected column count",
        ));
    }
    Ok(InstalledExtensionPackage {
        name: expect_text(&values[0], "name")?.to_string(),
        version: expect_text(&values[1], "version")?.to_string(),
        content_hash: expect_text(&values[2], "content_hash")?.to_string(),
        installed_at_micros: expect_i64(&values[3], "installed_at_micros")?,
        enabled: expect_boolish(&values[4], "enabled")?,
    })
}

fn dependency_from_values(values: &[Value]) -> Result<ExtensionDependencyRecord> {
    if values.len() != 7 {
        return Err(DbError::corruption(
            "extension dependency query returned an unexpected column count",
        ));
    }
    Ok(ExtensionDependencyRecord {
        object_kind: expect_text(&values[0], "object_kind")?.to_string(),
        object_name: expect_text(&values[1], "object_name")?.to_string(),
        extension_name: expect_text(&values[2], "extension_name")?.to_string(),
        dependency_name: expect_text(&values[3], "dependency_name")?.to_string(),
        dependency_kind: expect_text(&values[4], "dependency_kind")?.to_string(),
        content_hash: expect_text(&values[5], "content_hash")?.to_string(),
        recorded_at_micros: expect_i64(&values[6], "recorded_at_micros")?,
    })
}

fn expect_text<'a>(value: &'a Value, column: &str) -> Result<&'a str> {
    match value {
        Value::Text(value) => Ok(value),
        other => Err(DbError::corruption(format!(
            "extension catalog column {column} expected TEXT, got {other:?}"
        ))),
    }
}

fn expect_i64(value: &Value, column: &str) -> Result<i64> {
    match value {
        Value::Int64(value) => Ok(*value),
        other => Err(DbError::corruption(format!(
            "extension catalog column {column} expected INT64, got {other:?}"
        ))),
    }
}

fn expect_boolish(value: &Value, column: &str) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Int64(value) => Ok(*value != 0),
        other => Err(DbError::corruption(format!(
            "extension catalog column {column} expected BOOL/INT64, got {other:?}"
        ))),
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(DbError::sql(format!("{label} cannot be empty")));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(DbError::sql(format!(
            "{label} {value:?} must start with an ASCII letter or underscore"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(DbError::sql(format!(
            "{label} {value:?} must contain only ASCII letters, digits, and underscores"
        )));
    }
    Ok(())
}

fn sql_tokens(sql: &str) -> Vec<String> {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|token| token.trim_matches('"').to_string())
        .collect()
}

fn normalize_inspection_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn identifiers_equal(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn normalize_content_hash(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with("sha256:") {
        trimmed.to_ascii_lowercase()
    } else {
        format!("sha256:{}", trimmed.to_ascii_lowercase())
    }
}

fn now_micros() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| DbError::internal(format!("system time is before epoch: {error}")))?;
    i64::try_from(duration.as_micros())
        .map_err(|_| DbError::internal("system time is out of range"))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_bytes(&hasher.finalize())
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_base64_or_hex(value: &str, label: &str) -> Result<Vec<u8>> {
    let raw = value
        .strip_prefix("base64:")
        .or_else(|| value.strip_prefix("BASE64:"));
    if let Some(raw) = raw {
        use base64::Engine as _;
        return base64::engine::general_purpose::STANDARD
            .decode(raw)
            .map_err(|error| DbError::sql(format!("invalid {label} base64: {error}")));
    }
    let raw = value
        .strip_prefix("hex:")
        .or_else(|| value.strip_prefix("HEX:"))
        .unwrap_or(value);
    decode_hex(raw).map_err(|error| DbError::sql(format!("invalid {label} hex: {error}")))
}

fn decode_hex(value: &str) -> std::result::Result<Vec<u8>, &'static str> {
    if !value.len().is_multiple_of(2) {
        return Err("odd length");
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let text = std::str::from_utf8(pair).map_err(|_| "not utf8")?;
        bytes.push(u8::from_str_radix(text, 16).map_err(|_| "not hex")?);
    }
    Ok(bytes)
}

fn verify_ed25519_signature(public_key: &str, signature: &str, message: &[u8]) -> Result<()> {
    let public_key = decode_base64_or_hex(public_key, "Ed25519 public key")?;
    let public_key: [u8; 32] = public_key
        .try_into()
        .map_err(|_| DbError::sql("Ed25519 public key must be 32 bytes"))?;
    let signature = decode_base64_or_hex(signature, "Ed25519 signature")?;
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let key = VerifyingKey::from_bytes(&public_key)
            .map_err(|error| DbError::sql(format!("invalid Ed25519 public key: {error}")))?;
        let signature = Signature::from_slice(&signature)
            .map_err(|error| DbError::sql(format!("invalid Ed25519 signature: {error}")))?;
        key.verify(message, &signature).map_err(|error| {
            DbError::sql(format!("extension signature verification failed: {error}"))
        })
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let _ = (public_key, signature, message);
        Err(DbError::sql(
            "Ed25519 extension signature verification is not available in this wasm build",
        ))
    }
}

fn default_true() -> bool {
    true
}

fn default_function_kind() -> ExtensionFunctionKind {
    ExtensionFunctionKind::Scalar
}

fn default_max_steps() -> u64 {
    100_000
}

fn default_max_memory_bytes() -> usize {
    4 * 1024 * 1024
}

fn default_max_string_bytes() -> usize {
    1024 * 1024
}

fn default_max_blob_bytes() -> usize {
    1024 * 1024
}

fn default_max_rows() -> usize {
    10_000
}

fn default_max_row_bytes() -> usize {
    64 * 1024
}

fn default_max_aggregate_state_bytes() -> usize {
    1024 * 1024
}

fn default_max_collation_steps() -> u64 {
    10_000
}

#[cfg(all(
    feature = "lua-extensions",
    not(all(target_arch = "wasm32", target_os = "unknown"))
))]
mod lua_support {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use mlua::{
        Error as LuaError, Function, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table,
        Value as LuaValue, VmState,
    };

    use super::*;

    pub(super) fn validate_exports(package: &ExtensionPackage) -> Result<()> {
        let lua = build_lua(package, package.manifest.runtime.max_steps)?;
        let module = load_module(&lua, package)?;
        for function in &package.manifest.functions {
            match function.kind {
                ExtensionFunctionKind::Aggregate => {
                    let step = function.step.as_deref().unwrap_or(function.export_name());
                    let finalize = function.finalize.as_deref().ok_or_else(|| {
                        DbError::sql(format!(
                            "aggregate extension function {} must declare finalize",
                            function.name
                        ))
                    })?;
                    require_lua_function(&module, step)?;
                    require_lua_function(&module, finalize)?;
                }
                _ => {
                    require_lua_function(&module, function.export_name())?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn invoke_scalar(
        package: &ExtensionPackage,
        function: &ExtensionFunctionManifest,
        args: &[Value],
    ) -> Result<Value> {
        let lua = build_lua(package, package.manifest.runtime.max_steps)?;
        let module = load_module(&lua, package)?;
        let lua_function: Function = module.get(function.export_name()).map_err(lua_err)?;
        let lua_args = args
            .iter()
            .zip(function.args.iter())
            .map(|(value, sql_type)| {
                value_to_lua(
                    &lua,
                    value,
                    ExtensionSqlType::parse(sql_type).unwrap_or(ExtensionSqlType::Text),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let result: LuaValue = lua_function
            .call(MultiValue::from_vec(lua_args))
            .map_err(lua_err)?;
        let return_type =
            ExtensionSqlType::parse(function.returns.as_deref().ok_or_else(|| {
                DbError::sql(format!(
                    "extension function {} is missing return type",
                    function.name
                ))
            })?)?;
        lua_to_value(
            &lua,
            result,
            return_type,
            &package.manifest.runtime,
            &function.name,
        )
    }

    pub(super) fn invoke_table(
        package: &ExtensionPackage,
        function: &ExtensionFunctionManifest,
        args: &[Value],
        table_name: String,
    ) -> Result<crate::exec::Dataset> {
        let lua = build_lua(package, package.manifest.runtime.max_steps)?;
        let module = load_module(&lua, package)?;
        let lua_function: Function = module.get(function.export_name()).map_err(lua_err)?;
        let lua_args = args
            .iter()
            .zip(function.args.iter())
            .map(|(value, sql_type)| {
                value_to_lua(
                    &lua,
                    value,
                    ExtensionSqlType::parse(sql_type).unwrap_or(ExtensionSqlType::Text),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let result: LuaValue = lua_function
            .call(MultiValue::from_vec(lua_args))
            .map_err(lua_err)?;
        let rows_table = match result {
            LuaValue::Table(table) => table,
            other => {
                return Err(DbError::sql(format!(
                    "extension table function {} returned {}, expected table",
                    function.name,
                    lua_type_name(&other)
                )));
            }
        };
        let columns = function
            .columns
            .iter()
            .map(|column| {
                crate::exec::ColumnBinding::visible(Some(table_name.clone()), column.name.clone())
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for pair in rows_table.sequence_values::<LuaValue>() {
            if rows.len() >= package.manifest.runtime.max_rows {
                return Err(DbError::sql(format!(
                    "extension table function {} exceeded row limit {}",
                    function.name, package.manifest.runtime.max_rows
                )));
            }
            let row_value = pair.map_err(lua_err)?;
            let row_table = match row_value {
                LuaValue::Table(table) => table,
                other => {
                    return Err(DbError::sql(format!(
                        "extension table function {} returned row {}, expected table",
                        function.name,
                        lua_type_name(&other)
                    )));
                }
            };
            let mut output = Vec::with_capacity(function.columns.len());
            let mut row_bytes = 0usize;
            for column in &function.columns {
                let raw = row_table
                    .get::<LuaValue>(column.name.as_str())
                    .map_err(lua_err)?;
                let value = if matches!(raw, LuaValue::Nil) && column.nullable {
                    Value::Null
                } else {
                    lua_to_value(
                        &lua,
                        raw,
                        ExtensionSqlType::parse(&column.column_type)?,
                        &package.manifest.runtime,
                        &format!("{}.{}", function.name, column.name),
                    )?
                };
                row_bytes = row_bytes
                    .checked_add(value.approximate_heap_bytes() + std::mem::size_of::<Value>())
                    .ok_or_else(|| DbError::sql("extension table-valued row is too large"))?;
                if row_bytes > package.manifest.runtime.max_row_bytes {
                    return Err(DbError::sql(format!(
                        "extension table function {} row {} exceeded row-byte limit {}",
                        function.name,
                        rows.len() + 1,
                        package.manifest.runtime.max_row_bytes
                    )));
                }
                output.push(value);
            }
            rows.push(output);
        }
        Ok(crate::exec::Dataset::with_rows(columns, rows))
    }

    pub(super) fn invoke_aggregate(
        package: &ExtensionPackage,
        function: &ExtensionFunctionManifest,
        arg_rows: Vec<Vec<Value>>,
    ) -> Result<Value> {
        let lua = build_lua(package, package.manifest.runtime.max_steps)?;
        let module = load_module(&lua, package)?;
        let step_name = function.step.as_deref().unwrap_or(function.export_name());
        let finalize_name = function.finalize.as_deref().ok_or_else(|| {
            DbError::sql(format!(
                "aggregate extension function {} is missing finalize export",
                function.name
            ))
        })?;
        let step: Function = module.get(step_name).map_err(lua_err)?;
        let finalize: Function = module.get(finalize_name).map_err(lua_err)?;
        let mut state = LuaValue::Nil;
        for values in arg_rows {
            if function.null_handling == ExtensionNullHandling::ReturnsNull
                && values.iter().any(|value| matches!(value, Value::Null))
            {
                continue;
            }
            enforce_null_handling(function, &values)?;
            let mut args = Vec::with_capacity(values.len() + 1);
            args.push(state);
            for (value, sql_type) in values.iter().zip(function.args.iter()) {
                args.push(value_to_lua(
                    &lua,
                    value,
                    ExtensionSqlType::parse(sql_type).unwrap_or(ExtensionSqlType::Text),
                )?);
            }
            state = step
                .call::<LuaValue>(MultiValue::from_vec(args))
                .map_err(lua_err)?;
            if approximate_lua_value_bytes(&state)?
                > package.manifest.runtime.max_aggregate_state_bytes
            {
                return Err(DbError::sql(format!(
                    "extension aggregate {} exceeded aggregate state limit {}",
                    function.name, package.manifest.runtime.max_aggregate_state_bytes
                )));
            }
        }
        let result: LuaValue = finalize.call(state).map_err(lua_err)?;
        let return_type =
            ExtensionSqlType::parse(function.returns.as_deref().ok_or_else(|| {
                DbError::sql(format!(
                    "aggregate extension function {} is missing return type",
                    function.name
                ))
            })?)?;
        lua_to_value(
            &lua,
            result,
            return_type,
            &package.manifest.runtime,
            &function.name,
        )
    }

    pub(super) fn invoke_collation(
        package: &ExtensionPackage,
        function: &ExtensionFunctionManifest,
        left: &str,
        right: &str,
    ) -> Result<std::cmp::Ordering> {
        let lua = build_lua(package, package.manifest.runtime.max_collation_steps)?;
        let module = load_module(&lua, package)?;
        let lua_function: Function = module.get(function.export_name()).map_err(lua_err)?;
        let result: i64 = lua_function
            .call((left.to_string(), right.to_string()))
            .map_err(lua_err)?;
        match result.signum() {
            -1 => Ok(std::cmp::Ordering::Less),
            0 => Ok(std::cmp::Ordering::Equal),
            1 => Ok(std::cmp::Ordering::Greater),
            _ => Err(DbError::sql(format!(
                "extension collation {} returned {result}, expected -1, 0, or 1",
                function.name
            ))),
        }
    }

    fn build_lua(package: &ExtensionPackage, max_steps: u64) -> Result<Lua> {
        let lua = Lua::new_with(
            StdLib::STRING | StdLib::TABLE | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .map_err(lua_err)?;
        lua.set_memory_limit(package.manifest.runtime.max_memory_bytes)
            .map_err(lua_err)?;
        let steps = Arc::new(AtomicU64::new(0));
        let limit = max_steps.max(1);
        let hook_steps = Arc::clone(&steps);
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(100),
            move |_, _| {
                let next = hook_steps
                    .fetch_add(100, Ordering::Relaxed)
                    .saturating_add(100);
                if next > limit {
                    Err(LuaError::RuntimeError(format!(
                        "extension exceeded CPU step limit {limit}"
                    )))
                } else {
                    Ok(VmState::Continue)
                }
            },
        )
        .map_err(lua_err)?;
        install_ddb_namespace(&lua)?;
        Ok(lua)
    }

    fn install_ddb_namespace(lua: &Lua) -> Result<()> {
        let ddb = lua.create_table().map_err(lua_err)?;
        ddb.set(
            "null",
            lua.create_function(|_, ()| Ok(LuaValue::Nil))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        ddb.set(
            "text",
            lua.create_function(|_, value: String| Ok(value))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        ddb.set(
            "bool",
            lua.create_function(|_, value: bool| Ok(value))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        ddb.set(
            "int64",
            lua.create_function(|_, value: i64| Ok(value))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        ddb.set(
            "float64",
            lua.create_function(|_, value: f64| Ok(value))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        for name in [
            "decimal",
            "uuid",
            "date",
            "timestamp",
            "blob",
            "blob_hex",
            "blob_base64",
            "json",
        ] {
            let tag = name.to_string();
            ddb.set(
                name,
                lua.create_function(move |lua, value: LuaValue| typed_wrapper(lua, &tag, value))
                    .map_err(lua_err)?,
            )
            .map_err(lua_err)?;
        }
        ddb.set(
            "type_of",
            lua.create_function(|_, value: LuaValue| Ok(lua_type_name(&value).to_string()))
                .map_err(lua_err)?,
        )
        .map_err(lua_err)?;
        for (name, expected) in [
            ("is_null", "nil"),
            ("is_text", "string"),
            ("is_bool", "boolean"),
            ("is_int64", "integer"),
            ("is_float64", "number"),
            ("is_decimal", "decimal"),
            ("is_uuid", "uuid"),
            ("is_date", "date"),
            ("is_timestamp", "timestamp"),
            ("is_blob", "blob"),
            ("is_json", "json"),
        ] {
            ddb.set(
                name,
                lua.create_function(move |_, value: LuaValue| {
                    Ok(
                        wrapper_type(&value).unwrap_or_else(|| lua_type_name(&value).to_string())
                            == expected,
                    )
                })
                .map_err(lua_err)?,
            )
            .map_err(lua_err)?;
        }
        lua.globals().set("ddb", ddb).map_err(lua_err)?;
        Ok(())
    }

    fn typed_wrapper(lua: &Lua, tag: &str, value: LuaValue) -> mlua::Result<Table> {
        let table = lua.create_table()?;
        table.set("__ddb_type", tag)?;
        table.set("value", value)?;
        table.set(
            "to_string",
            lua.create_function(|_, table: Table| {
                let value: LuaValue = table.get("value")?;
                Ok(match value {
                    LuaValue::String(value) => value.to_string_lossy().to_string(),
                    LuaValue::Integer(value) => value.to_string(),
                    LuaValue::Number(value) => value.to_string(),
                    LuaValue::Boolean(value) => value.to_string(),
                    LuaValue::Nil => "NULL".to_string(),
                    _ => "<ddb-value>".to_string(),
                })
            })?,
        )?;
        table.set(
            "len",
            lua.create_function(|_, table: Table| {
                let value: LuaValue = table.get("value")?;
                Ok(match value {
                    LuaValue::String(value) => value.as_bytes().len() as i64,
                    _ => 0_i64,
                })
            })?,
        )?;
        table.set(
            "add",
            lua.create_function(|lua, (left, right): (Table, Table)| {
                decimal_binop(lua, left, right, "+")
            })?,
        )?;
        table.set(
            "sub",
            lua.create_function(|lua, (left, right): (Table, Table)| {
                decimal_binop(lua, left, right, "-")
            })?,
        )?;
        table.set(
            "mul",
            lua.create_function(|lua, (left, right): (Table, Table)| {
                decimal_binop(lua, left, right, "*")
            })?,
        )?;
        table.set(
            "div",
            lua.create_function(|lua, (left, right): (Table, Table)| {
                decimal_binop(lua, left, right, "/")
            })?,
        )?;
        table.set(
            "cmp",
            lua.create_function(|_, (left, right): (Table, Table)| {
                let left: String = left.get("value")?;
                let right: String = right.get("value")?;
                Ok(left.cmp(&right) as i64)
            })?,
        )?;
        Ok(table)
    }

    fn decimal_binop(lua: &Lua, left: Table, right: Table, op: &str) -> mlua::Result<Table> {
        let left: String = left.get("value")?;
        let right: String = right.get("value")?;
        let result = match op {
            "+" => decimal_add_text(&left, &right),
            "-" => decimal_sub_text(&left, &right),
            "*" => decimal_mul_text(&left, &right),
            "/" => decimal_div_text(&left, &right),
            _ => "0".to_string(),
        };
        typed_wrapper(
            lua,
            "decimal",
            LuaValue::String(lua.create_string(&result)?),
        )
    }

    fn load_module(lua: &Lua, package: &ExtensionPackage) -> Result<Table> {
        let value: LuaValue = lua
            .load(&package.entry_source)
            .set_name(&package.manifest.entry)
            .eval()
            .map_err(lua_err)?;
        match value {
            LuaValue::Table(table) => Ok(table),
            other => Err(DbError::sql(format!(
                "extension {} entry returned {}, expected table",
                package.manifest.name,
                lua_type_name(&other)
            ))),
        }
    }

    fn require_lua_function(module: &Table, name: &str) -> Result<()> {
        match module.get::<LuaValue>(name).map_err(lua_err)? {
            LuaValue::Function(_) => Ok(()),
            other => Err(DbError::sql(format!(
                "extension Lua export {name} is {}, expected function",
                lua_type_name(&other)
            ))),
        }
    }

    fn value_to_lua(lua: &Lua, value: &Value, sql_type: ExtensionSqlType) -> Result<LuaValue> {
        match value {
            Value::Null => Ok(LuaValue::Nil),
            Value::Bool(value) if sql_type == ExtensionSqlType::Bool => {
                Ok(LuaValue::Boolean(*value))
            }
            Value::Text(value)
                if matches!(sql_type, ExtensionSqlType::Text | ExtensionSqlType::Json) =>
            {
                Ok(LuaValue::String(lua.create_string(value).map_err(lua_err)?))
            }
            Value::Int64(value) if sql_type == ExtensionSqlType::Int64 => {
                Ok(LuaValue::Integer(*value))
            }
            Value::Float64(value) if sql_type == ExtensionSqlType::Float64 => {
                Ok(LuaValue::Number(*value))
            }
            Value::Decimal { scaled, scale } if sql_type == ExtensionSqlType::Decimal => {
                typed_wrapper(
                    lua,
                    "decimal",
                    LuaValue::String(
                        lua.create_string(decimal_to_string(*scaled, *scale))
                            .map_err(lua_err)?,
                    ),
                )
                .map(LuaValue::Table)
                .map_err(lua_err)
            }
            Value::Uuid(value) if sql_type == ExtensionSqlType::Uuid => typed_wrapper(
                lua,
                "uuid",
                LuaValue::String(lua.create_string(uuid_to_string(value)).map_err(lua_err)?),
            )
            .map(LuaValue::Table)
            .map_err(lua_err),
            Value::DateDays(value) if sql_type == ExtensionSqlType::Date => typed_wrapper(
                lua,
                "date",
                LuaValue::String(
                    lua.create_string(format_date_days(*value))
                        .map_err(lua_err)?,
                ),
            )
            .map(LuaValue::Table)
            .map_err(lua_err),
            Value::TimestampMicros(value) if sql_type == ExtensionSqlType::Timestamp => {
                typed_wrapper(
                    lua,
                    "timestamp",
                    LuaValue::String(lua.create_string(value.to_string()).map_err(lua_err)?),
                )
                .map(LuaValue::Table)
                .map_err(lua_err)
            }
            Value::TimestampTzMicros(value) if sql_type == ExtensionSqlType::Timestamp => {
                typed_wrapper(
                    lua,
                    "timestamp",
                    LuaValue::String(
                        lua.create_string(format_timestamp_tz_micros(*value))
                            .map_err(lua_err)?,
                    ),
                )
                .map(LuaValue::Table)
                .map_err(lua_err)
            }
            Value::Blob(value) if sql_type == ExtensionSqlType::Blob => typed_wrapper(
                lua,
                "blob",
                LuaValue::String(lua.create_string(value).map_err(lua_err)?),
            )
            .map(LuaValue::Table)
            .map_err(lua_err),
            other => Err(DbError::sql(format!(
                "extension argument expected {}, received {}",
                sql_type.as_str(),
                value_kind(other)
            ))),
        }
    }

    fn lua_to_value(
        lua: &Lua,
        value: LuaValue,
        sql_type: ExtensionSqlType,
        limits: &ExtensionRuntimeLimits,
        function_name: &str,
    ) -> Result<Value> {
        if matches!(value, LuaValue::Nil) {
            return Ok(Value::Null);
        }
        match sql_type {
            ExtensionSqlType::Null => Ok(Value::Null),
            ExtensionSqlType::Bool => match value {
                LuaValue::Boolean(value) => Ok(Value::Bool(value)),
                other => type_error(function_name, sql_type, &other),
            },
            ExtensionSqlType::Text => match value {
                LuaValue::String(value) => {
                    let text = value.to_string_lossy().to_string();
                    enforce_string_limit(&text, limits, function_name)?;
                    Ok(Value::Text(text))
                }
                LuaValue::Table(table)
                    if wrapper_type_table(&table)? == Some("text".to_string()) =>
                {
                    let text: String = table.get("value").map_err(lua_err)?;
                    enforce_string_limit(&text, limits, function_name)?;
                    Ok(Value::Text(text))
                }
                other => type_error(function_name, sql_type, &other),
            },
            ExtensionSqlType::Int64 => match value {
                LuaValue::Integer(value) => Ok(Value::Int64(value)),
                LuaValue::Table(table)
                    if wrapper_type_table(&table)? == Some("int64".to_string()) =>
                {
                    Ok(Value::Int64(table.get("value").map_err(lua_err)?))
                }
                other => type_error(function_name, sql_type, &other),
            },
            ExtensionSqlType::Float64 => match value {
                LuaValue::Number(value) => Ok(Value::Float64(value)),
                LuaValue::Integer(value) => Ok(Value::Float64(value as f64)),
                other => type_error(function_name, sql_type, &other),
            },
            ExtensionSqlType::Decimal => {
                let text = wrapper_string(&value, "decimal")?;
                let (scaled, scale) = parse_decimal_text(&text)?;
                Ok(Value::Decimal { scaled, scale })
            }
            ExtensionSqlType::Uuid => {
                let text = wrapper_string(&value, "uuid")?;
                Ok(Value::Uuid(parse_uuid_text(&text)?))
            }
            ExtensionSqlType::Date => {
                let text = wrapper_string(&value, "date")?;
                Ok(Value::DateDays(parse_date_days(&text)?))
            }
            ExtensionSqlType::Timestamp => {
                let text = wrapper_string(&value, "timestamp")?;
                let micros = text
                    .parse::<i64>()
                    .or_else(|_| parse_timestamp_tz_micros(&text))
                    .map_err(|error| {
                        DbError::sql(format!("invalid TIMESTAMP extension result: {error}"))
                    })?;
                Ok(Value::TimestampMicros(micros))
            }
            ExtensionSqlType::Blob => {
                let bytes = wrapper_bytes(lua, &value, "blob")?;
                if bytes.len() > limits.max_blob_bytes {
                    return Err(DbError::sql(format!(
                        "extension function {function_name} returned BLOB larger than {} bytes",
                        limits.max_blob_bytes
                    )));
                }
                Ok(Value::Blob(bytes))
            }
            ExtensionSqlType::Json => {
                let text = match wrapper_string(&value, "json") {
                    Ok(text) => text,
                    Err(_) => match &value {
                        LuaValue::String(value) => value.to_string_lossy().to_string(),
                        _ => return type_error(function_name, sql_type, &value),
                    },
                };
                enforce_string_limit(&text, limits, function_name)?;
                serde_json::from_str::<serde_json::Value>(&text).map_err(|error| {
                    DbError::sql(format!(
                        "extension function {function_name} returned invalid JSON: {error}"
                    ))
                })?;
                Ok(Value::Text(text))
            }
        }
    }

    fn enforce_string_limit(
        text: &str,
        limits: &ExtensionRuntimeLimits,
        function_name: &str,
    ) -> Result<()> {
        if text.len() > limits.max_string_bytes {
            return Err(DbError::sql(format!(
                "extension function {function_name} returned string larger than {} bytes",
                limits.max_string_bytes
            )));
        }
        Ok(())
    }

    fn wrapper_type(value: &LuaValue) -> Option<String> {
        match value {
            LuaValue::Table(table) => wrapper_type_table(table).ok().flatten(),
            LuaValue::Nil => Some("nil".to_string()),
            LuaValue::Boolean(_) => Some("boolean".to_string()),
            LuaValue::Integer(_) => Some("integer".to_string()),
            LuaValue::Number(_) => Some("number".to_string()),
            LuaValue::String(_) => Some("string".to_string()),
            _ => None,
        }
    }

    fn wrapper_type_table(table: &Table) -> Result<Option<String>> {
        match table.get::<LuaValue>("__ddb_type").map_err(lua_err)? {
            LuaValue::String(value) => Ok(Some(value.to_string_lossy().to_string())),
            LuaValue::Nil => Ok(None),
            other => Err(DbError::sql(format!(
                "invalid ddb wrapper type tag {}",
                lua_type_name(&other)
            ))),
        }
    }

    fn wrapper_string(value: &LuaValue, expected: &str) -> Result<String> {
        let LuaValue::Table(table) = value else {
            return Err(DbError::sql(format!(
                "extension result expected ddb.{expected} wrapper"
            )));
        };
        if wrapper_type_table(table)?.as_deref() != Some(expected) {
            return Err(DbError::sql(format!(
                "extension result expected ddb.{expected} wrapper"
            )));
        }
        match table.get::<LuaValue>("value").map_err(lua_err)? {
            LuaValue::String(value) => Ok(value.to_string_lossy().to_string()),
            LuaValue::Integer(value) => Ok(value.to_string()),
            LuaValue::Number(value) => Ok(value.to_string()),
            other => Err(DbError::sql(format!(
                "ddb.{expected} wrapper value was {}, expected string-compatible value",
                lua_type_name(&other)
            ))),
        }
    }

    fn wrapper_bytes(lua: &Lua, value: &LuaValue, expected: &str) -> Result<Vec<u8>> {
        let _ = lua;
        let LuaValue::Table(table) = value else {
            return Err(DbError::sql(format!(
                "extension result expected ddb.{expected} wrapper"
            )));
        };
        if wrapper_type_table(table)?.as_deref() != Some(expected) {
            return Err(DbError::sql(format!(
                "extension result expected ddb.{expected} wrapper"
            )));
        }
        match table.get::<LuaValue>("value").map_err(lua_err)? {
            LuaValue::String(value) => Ok(value.as_bytes().to_vec()),
            other => Err(DbError::sql(format!(
                "ddb.{expected} wrapper value was {}, expected string/blob bytes",
                lua_type_name(&other)
            ))),
        }
    }

    fn type_error<T>(
        function_name: &str,
        sql_type: ExtensionSqlType,
        value: &LuaValue,
    ) -> Result<T> {
        Err(DbError::sql(format!(
            "extension function {function_name} returned {}, but manifest declares {}",
            lua_type_name(value),
            sql_type.as_str()
        )))
    }

    fn lua_type_name(value: &LuaValue) -> &'static str {
        match value {
            LuaValue::Nil => "nil",
            LuaValue::Boolean(_) => "boolean",
            LuaValue::LightUserData(_) => "lightuserdata",
            LuaValue::Integer(_) => "integer",
            LuaValue::Number(_) => "number",
            LuaValue::String(_) => "string",
            LuaValue::Table(_) => "table",
            LuaValue::Function(_) => "function",
            LuaValue::Thread(_) => "thread",
            LuaValue::UserData(_) => "userdata",
            LuaValue::Error(_) => "error",
            LuaValue::Other(_) => "other",
        }
    }

    fn approximate_lua_value_bytes(value: &LuaValue) -> Result<usize> {
        match value {
            LuaValue::Nil
            | LuaValue::Boolean(_)
            | LuaValue::Integer(_)
            | LuaValue::Number(_)
            | LuaValue::LightUserData(_) => Ok(std::mem::size_of::<LuaValue>()),
            LuaValue::String(value) => Ok(value.as_bytes().len()),
            LuaValue::Table(table) => {
                let mut bytes = 0usize;
                for pair in table.clone().pairs::<LuaValue, LuaValue>() {
                    let (key, value) = pair.map_err(lua_err)?;
                    bytes = bytes
                        .checked_add(approximate_lua_value_bytes(&key)?)
                        .and_then(|bytes| {
                            bytes.checked_add(approximate_lua_value_bytes(&value).ok()?)
                        })
                        .ok_or_else(|| DbError::sql("Lua aggregate state is too large"))?;
                }
                Ok(bytes)
            }
            _ => Ok(std::mem::size_of::<LuaValue>()),
        }
    }

    fn lua_err(error: LuaError) -> DbError {
        DbError::sql(format!("Lua extension error: {error}"))
    }

    fn value_kind(value: &Value) -> &'static str {
        match value {
            Value::Null => "NULL",
            Value::Int64(_) => "INT64",
            Value::Float64(_) => "FLOAT64",
            Value::Bool(_) => "BOOL",
            Value::Text(_) => "TEXT",
            Value::Blob(_) => "BLOB",
            Value::Decimal { .. } => "DECIMAL",
            Value::Uuid(_) => "UUID",
            Value::TimestampMicros(_) | Value::TimestampTzMicros(_) => "TIMESTAMP",
            Value::DateDays(_) => "DATE",
            _ => "UNSUPPORTED",
        }
    }

    fn parse_uuid_text(value: &str) -> Result<[u8; 16]> {
        let compact = if value.len() == 36 {
            if value.as_bytes().get(8) != Some(&b'-')
                || value.as_bytes().get(13) != Some(&b'-')
                || value.as_bytes().get(18) != Some(&b'-')
                || value.as_bytes().get(23) != Some(&b'-')
            {
                return Err(DbError::sql(
                    "UUID extension result expects canonical UUID text",
                ));
            }
            value.replace('-', "")
        } else if value.len() == 32 {
            value.to_string()
        } else {
            return Err(DbError::sql(
                "UUID extension result expects canonical UUID text",
            ));
        };
        let mut uuid = [0u8; 16];
        for (index, chunk) in compact.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(chunk)
                .map_err(|_| DbError::sql("UUID extension result expects canonical UUID text"))?;
            uuid[index] = u8::from_str_radix(text, 16)
                .map_err(|_| DbError::sql("UUID extension result expects canonical UUID text"))?;
        }
        Ok(uuid)
    }

    fn uuid_to_string(value: &[u8; 16]) -> String {
        let hex = hex_bytes(value);
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }

    fn decimal_to_string(scaled: i64, scale: u8) -> String {
        if scale == 0 {
            return scaled.to_string();
        }
        let negative = scaled < 0;
        let digits = scaled.unsigned_abs().to_string();
        let scale = usize::from(scale);
        let padded = if digits.len() <= scale {
            format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
        } else {
            digits
        };
        let split = padded.len() - scale;
        let mut output = format!("{}.{}", &padded[..split], &padded[split..]);
        if negative {
            output.insert(0, '-');
        }
        output
    }

    fn decimal_add_text(left: &str, right: &str) -> String {
        decimal_f64_binop(left, right, |a, b| a + b)
    }

    fn decimal_sub_text(left: &str, right: &str) -> String {
        decimal_f64_binop(left, right, |a, b| a - b)
    }

    fn decimal_mul_text(left: &str, right: &str) -> String {
        decimal_f64_binop(left, right, |a, b| a * b)
    }

    fn decimal_div_text(left: &str, right: &str) -> String {
        decimal_f64_binop(left, right, |a, b| a / b)
    }

    fn decimal_f64_binop(left: &str, right: &str, op: impl FnOnce(f64, f64) -> f64) -> String {
        let left = left.parse::<f64>().unwrap_or(0.0);
        let right = right.parse::<f64>().unwrap_or(0.0);
        let value = op(left, right);
        if value.fract() == 0.0 {
            format!("{value:.0}")
        } else {
            value.to_string()
        }
    }

    #[allow(dead_code)]
    fn _multi_value_len(values: &MultiValue) -> usize {
        values.len()
    }
}
