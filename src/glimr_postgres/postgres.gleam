//// PostgreSQL Connection Management
////
//// Application boot needs to wire up database pools, cache
//// pools, and session stores, but the underlying config parsing
//// and pool construction are spread across several internal
//// modules. This module is the public entry point that ties
//// them together — each function takes a name or pool and
//// returns a ready-to-use resource, so the app's main module
//// reads as a simple sequence of start calls rather than
//// manual config loading and plumbing.
////

import glimr/cache/driver.{type CacheStore} as cache_driver
import glimr/config/database
import glimr/db/driver
import glimr/db/pool_connection.{type Pool}
import glimr/session/session.{type Session}
import glimr/session/store
import glimr_postgres/cache/pool.{type Pool as CachePool} as cache_pool
import glimr_postgres/db/pool
import glimr_postgres/db/query
import glimr_postgres/session/session_store

// ------------------------------------------------------------- Public Functions

/// Loads database.toml, finds the named connection, and starts
/// a pool in one call so the app boot code doesn't need to
/// touch config parsing or driver types directly. The assert
/// on pool start crashes intentionally — a missing or broken
/// database connection at boot is unrecoverable, and crashing
/// early gives a clear stack trace instead of propagating
/// errors through every downstream function.
///
pub fn start(name: String) -> Pool {
  let connections = database.load()
  let conn = driver.find_by_name(name, connections)
  let config = driver.to_config(conn)
  start_from_config(config)
}

/// Cache pools share the database connection pool rather than
/// opening their own connections, avoiding double the connection
/// count when both db and cache are in use. The store list is
/// passed in so this function can look up the named store's
/// table and TTL config without re-reading the TOML file.
///
pub fn start_cache(
  db_pool: Pool,
  name: String,
  stores: List(CacheStore),
) -> CachePool {
  let store = cache_driver.find_by_name(name, stores)

  cache_pool.start_pool(db_pool, store)
}

/// Registers the PostgreSQL session store in persistent_term so
/// the session middleware can load, save, and destroy sessions
/// without knowing which backend is active. This must be called
/// at boot before any requests arrive — once cached, the store
/// is available to every BEAM process without being threaded
/// through function arguments.
///
pub fn start_session(pool: Pool) -> Session {
  let session = session_store.create(pool)
  store.cache_store(session)

  session.empty()
}

// ------------------------------------------------------------- Internal Public Functions

/// Tests and benchmarks need pools without reading
/// database.toml, so accepting a pre-built Config lets them
/// bypass file I/O and supply custom connection parameters
/// directly.
///
pub fn start_from_config(config: pool_connection.Config) -> Pool {
  let assert Ok(db_pool) = pool.start_pool(config)
  wrap_pool(db_pool)
}

/// The core Pool type uses Dynamic-typed vtable callbacks so it
/// works across drivers. Wrapping the Postgres-specific pool
/// here wires in the query and exec implementations so the
/// rest of the framework can use it without knowing the driver.
///
pub fn wrap_pool(db_pool: pool.Pool) -> Pool {
  let #(checkout, stop) = pool.raw_checkout(db_pool)

  pool_connection.new_pool(
    driver: pool_connection.Postgres,
    query_fn: pool_connection.to_dynamic(query.vtable_query),
    exec_fn: pool_connection.to_dynamic(query.vtable_exec),
    checkout: fn() {
      case checkout() {
        Ok(#(conn, release)) -> Ok(#(pool_connection.to_dynamic(conn), release))
        Error(msg) -> Error(msg)
      }
    },
    stop: stop,
  )
}
