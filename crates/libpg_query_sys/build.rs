fn main() {
    // The pinned `pg_query` crate vendors and builds libpg_query itself.
    // Keep this wrapper build script intentionally empty so local builds do
    // not depend on an untracked repo-local vendor tree.
}
