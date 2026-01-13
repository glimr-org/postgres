//// PostgreSQL Cache Pool
////
//// Provides a cache pool wrapper around an existing PostgreSQL
//// database pool. The cache uses a database table to store
//// cached values with expiration timestamps.

import glimr/cache/driver.{type CacheStore, DatabaseStore}
import glimr_postgres/db/pool.{type Pool as DbPool}

// ------------------------------------------------------------- Public Types

/// A PostgreSQL cache pool. Wraps an existing database pool
/// and stores the table name for cache operations. Created
/// via start_pool with a DatabaseStore configuration.
///
pub opaque type Pool {
  Pool(db_pool: DbPool, table: String)
}

// ------------------------------------------------------------- Public Functions

/// Creates a cache pool from an existing database pool.
/// The store configuration provides the table name used
/// for storing cache entries.
///
pub fn start_pool(db_pool: DbPool, store: CacheStore) -> Pool {
  let table = extract_table(store)
  Pool(db_pool: db_pool, table: table)
}

/// Returns the underlying database pool for executing queries.
/// Used internally by cache operations to access the
/// PostgreSQL connection.
///
pub fn get_db_pool(pool: Pool) -> DbPool {
  pool.db_pool
}

/// Returns the cache table name. Used internally by cache
/// operations to construct SQL queries for the correct
/// table.
///
pub fn get_table(pool: Pool) -> String {
  pool.table
}

// -------------------------------------------------- Internal Public Functions

/// Stops the cache pool. Currently a no-op since the database
/// pool lifecycle is managed separately by the application
/// code.
///
@internal
pub fn stop_pool(_pool: Pool) -> Nil {
  Nil
}

// ------------------------------------------------------------- Private Functions

/// Extracts the table name from a DatabaseStore config.
/// Panics if called with a non-Database store type like
/// FileStore or RedisStore.
///
fn extract_table(store: CacheStore) -> String {
  case store {
    DatabaseStore(_, _, table) -> table
    _ -> panic as "Cannot create PostgreSQL cache pool from non-Database store"
  }
}
