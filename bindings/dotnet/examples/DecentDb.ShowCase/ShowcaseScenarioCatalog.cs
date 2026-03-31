namespace DecentDb.ShowCase;

internal static partial class ShowcaseScenarioCatalog
{
    public static IReadOnlyList<ShowcaseScenario> All { get; } =
    [
        new("DECENTDB METADATA & VERSION", DemonstrateDecentDBMetadata),
        new("DATABASE CREATE & SCHEMA", DemonstrateDatabaseOperations),
        new("EF CORE MIGRATIONS & SCHEMA LIFECYCLE", DemonstrateMigrationsAndSchemaLifecycle),
        new("EF CORE ADVANCED MODELING", DemonstrateAdvancedModeling),
        new("EF CORE BASIC CRUD OPERATIONS", DemonstrateEFCoreBasicCRUD),
        new("NULLABLE TYPE COMPARISONS", DemonstrateNullableComparisons),
        new("LINQ QUERIES", DemonstrateLinqQueries),
        new("STRING OPERATIONS TRANSLATION", DemonstrateStringOperations),
        new("MATH OPERATIONS TRANSLATION", DemonstrateMathOperations),
        new("DATETIME OPERATIONS", DemonstrateDateTimeOperations),
        new("NODATIME OPERATIONS (Instant, LocalDate, LocalDateTime)", DemonstrateNodaTimeOperations),
        new("PRIMITIVE COLLECTIONS (JSON ARRAYS)", DemonstratePrimitiveCollections),
        new("TRANSACTIONS", DemonstrateTransactions),
        new("OPERATIONAL BEHAVIORS (Isolation, SaveAs, Vacuum)", DemonstrateOperationalBehaviors),
        new("CONCURRENCY CONTROL", DemonstrateConcurrencyControl),
        new("FAILURE PATHS & RECOVERY", DemonstrateFailurePathsAndRecovery),
        new("SCHEMA INTROSPECTION", DemonstrateSchemaIntrospection),
        new("RAW SQL EXECUTION", DemonstrateRawSql),
        new("CHANGE TRACKING", DemonstrateChangeTracking),
        new("BULK OPERATIONS", DemonstrateBulkOperations),
        new("PATTERN MATCHING (EF.Functions.Like)", DemonstrateLikePatternMatching),
        new("SET OPERATIONS (Union, Concat, Intersect, Except)", DemonstrateSetOperations),
        new("EXPLICIT JOIN QUERIES", DemonstrateExplicitJoins),
        new("SUBQUERIES", DemonstrateSubqueries),
        new("INCLUDE / THENINCLUDE (Relationship Loading)", DemonstrateIncludeThenInclude),
        new("EXISTENCE & CHILD FILTERS", DemonstrateExistenceAndChildFilters),
        new("COMPOSITE FOREIGN KEYS", DemonstrateCompositeForeignKeys),
        new("CONDITIONAL LOGIC (Ternary, Coalesce)", DemonstrateConditionalLogic),
        new("QUERY COMPOSITION (Reusable IQueryable)", DemonstrateQueryComposition),
        new("SELECTMANY (Flatten Collections)", DemonstrateSelectMany),
        new("CLIENT VS SERVER EVALUATION", DemonstrateClientVsServerEvaluation),
        new("WINDOW FUNCTIONS", DemonstrateWindowFunctions),
        new("EDGE CASES & NOTES", DemonstrateUnsupportedCases),
        new("ALL BUILT-IN C# TYPES", DemonstrateAllBuiltInTypes),
        new("ALL NULLABLE BUILT-IN C# TYPES", DemonstrateAllNullableBuiltInTypes),
        new("PERFORMANCE PATTERNS", DemonstratePerformancePatterns),
    ];
}
