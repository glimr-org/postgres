//// PostgreSQL Adapter
////
//// Application boot needs to wire up database pools, cache
//// pools, and session stores, but the underlying config
//// parsing and pool construction are spread across several
//// internal modules. This is the public entry point that ties
//// them together — each function takes a name or pool and
//// returns a ready-to-use resource, so the app's main module
//// reads as a simple sequence of start calls rather than
//// manual config loading and plumbing.
////

import gleam/string
import glimr/cache/cache.{type CachePool}
import glimr/cache/database as cache_database
import glimr/config/database
import glimr/db/db.{type DbPool}
import glimr/db/driver
import glimr/session/session.{type Session}
import glimr/session/store
import glimr_postgres/db/pool
import glimr_postgres/db/query
import glimr_postgres/session/session_store

// ------------------------------------------------------------- Public Functions

/// The one-liner every app's main module calls at boot. Loads
/// database.toml, finds the named connection, and starts a pool
/// — no config parsing or driver types to deal with. The assert
/// crash is intentional: a broken database connection at
/// startup is unrecoverable, and crashing immediately gives a
/// clear stack trace instead of propagating errors through
/// every downstream function that tries to use the pool.
///
pub fn start(name: String) -> DbPool {
  let connections = database.load()
  let conn = driver.find_by_name(name, connections)
  let config = driver.to_config(conn)
  start_from_config(config)
}

/// If you're already running PostgreSQL for your app, you can
/// use the same database for caching instead of adding Redis as
/// another dependency. This wires your existing database pool
/// into the framework's cache system using a regular SQL table
/// — same CachePool API, no extra infrastructure to manage.
///
pub fn start_cache(db_pool: DbPool, name: String) -> CachePool {
  cache_database.start(db_pool, name)
}

/// Registers the PostgreSQL session store in persistent_term so
/// the session middleware can load, save, and destroy sessions
/// without knowing which backend is active. This must be called
/// at boot before any requests arrive — once registered, the
/// store is available to every BEAM process without being
/// threaded through function arguments.
///
pub fn start_session(pool: DbPool) -> Session {
  let session = session_store.create(pool)
  store.cache_store(session)

  session.empty()
}

// ------------------------------------------------------------- Internal Public Functions

/// Console commands like `db:migrate` need to start a pool but
/// shouldn't crash on failure — they should print a helpful
/// error message instead. This is the non-panicking variant of
/// start_from_config that returns a Result so the command can
/// handle the error gracefully.
///
@internal
pub fn try_start_from_config(config: db.Config) -> Result(DbPool, String) {
  case pool.start_pool(config) {
    Ok(db_pool) -> Ok(wrap_pool(db_pool))
    Error(e) -> Error(string.inspect(e))
  }
}

/// Tests and benchmarks need pools without reading
/// database.toml — they supply custom connection parameters
/// (in-memory databases, test-specific credentials, etc.)
/// directly. Accepting a pre-built Config skips the file I/O
/// and config lookup.
///
@internal
pub fn start_from_config(config: db.Config) -> DbPool {
  let assert Ok(db_pool) = pool.start_pool(config)
  wrap_pool(db_pool)
}

/// The framework's Pool type uses dynamic-typed vtable
/// callbacks so it can work with any database driver without
/// knowing the concrete types. This wires in the
/// Postgres-specific query and exec implementations so code
/// that receives a Pool can call db.query or
/// db.exec and have it route to pgo under the
/// hood.
///
@internal
pub fn wrap_pool(db_pool: pool.Pool) -> DbPool {
  let #(checkout, stop) = pool.raw_checkout(db_pool)

  db.new_pool(
    driver: db.Postgres,
    query_fn: db.to_dynamic(query.vtable_query),
    exec_fn: db.to_dynamic(query.vtable_exec),
    checkout: fn() {
      case checkout() {
        Ok(#(conn, release)) -> Ok(#(db.to_dynamic(conn), release))
        Error(msg) -> Error(msg)
      }
    },
    stop: stop,
  )
}
