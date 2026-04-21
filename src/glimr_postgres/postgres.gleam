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
import glimr/cache.{type CachePool}
import glimr/db/db.{type DbPool}
import glimr/db/driver
import glimr/session.{type SessionStore}
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
  let connections = driver.load_connections()
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
  cache.database_start(db_pool, name)
}

/// If you're already running Postgres, storing sessions there
/// avoids adding Redis just for session state. Pass the result
/// to `session.setup()` in your bootstrap and sessions live in
/// the same database as your application data — one fewer
/// service to manage in production.
///
pub fn session_store(pool: DbPool) -> SessionStore {
  session_store.create(pool)
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

/// The rest of the framework talks to databases through a
/// generic DbPool — it doesn't know or care whether it's
/// Postgres or SQLite underneath. This is where we plug pgo's
/// query and exec functions into that generic interface, so
/// `db.query` on a Postgres pool routes to the right driver
/// without any caller needing to know.
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
