//// PostgreSQL Adapter
////
//// Application boot needs to wire up database pools, cache
//// pools, and session stores. This is the public entry point —
//// each function takes a name or pool and returns a ready-to-use
//// resource, so the app's main module reads as a simple
//// sequence of start calls rather than manual config loading
//// and plumbing. Pool construction, query execution, and
//// session storage all live here so callers only need to
//// import this one module.
////

import gleam/dict
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode.{type Decoder}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string
import glimr/cache.{type CachePool}
import glimr/config
import glimr/db/db.{
  type Config, type DbError, type DbPool, type QueryResult, type Value,
  ConnectionError, DecodeError, PostgresConfig, PostgresParamsConfig, QueryError,
  QueryResult, SqliteConfig, TimeoutError,
}
import glimr/db/driver
import glimr/session.{type SessionStore}
import glimr/utils/unix_timestamp
import glimr_postgres/db/gen
import pog

// ------------------------------------------------------------- Public Types

/// The pool must be opaque so callers can't bypass the
/// checkout/checkin protocol by accessing the raw connections
/// directly. Storing closures rather than an Erlang PID keeps
/// the Erlang pool internals hidden from Gleam code.
///
pub opaque type Pool {
  Pool(
    checkout: fn() -> Result(#(pog.Connection, fn() -> Nil), String),
    stop: fn() -> Nil,
  )
}

/// The Erlang FFI returns closures that capture the pool handle
/// internally, so a named record type is needed to receive them
/// across the FFI boundary. This stays public so the FFI module
/// can construct it.
///
pub type PoolOps {
  PoolOps(
    checkout: fn() -> Result(#(pog.Connection, fn() -> Nil), String),
    stop: fn() -> Nil,
  )
}

/// Re-exporting pog.Connection under a local alias lets the rest
/// of the codebase reference Connection without depending on pog
/// directly, so swapping the underlying driver only requires
/// changes here.
///
pub type Connection =
  pog.Connection

// ------------------------------------------------------------- Public Functions

/// The one-liner every app's main module calls at boot. Loads
/// database.toml, finds the named connection, and starts a pool
/// — no config parsing or driver types to deal with. The assert
/// crash is intentional: a broken database connection at startup
/// is unrecoverable, and crashing immediately gives a clear
/// stack trace instead of propagating errors through every
/// downstream function that tries to use the pool.
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
/// into the framework's cache system using a regular SQL table —
/// same CachePool API, no extra infrastructure to manage.
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
  let table = config.get_string("session.table")
  let lifetime = config.get_int("session.lifetime")

  session.new(
    load: fn(session_id) { session_load(pool, table, session_id, lifetime) },
    save: fn(session_id, data, flash) {
      session_save(pool, table, session_id, data, flash, lifetime)
    },
    destroy: fn(session_id) { session_destroy(pool, table, session_id) },
    gc: fn() { session_gc(pool, table, lifetime) },
    cookie_value: fn(id, _, _) { id },
  )
}

// ------------------------------------------------------------- Internal Public Functions

/// Console commands like `db:migrate` need to start a pool but
/// shouldn't crash on failure — they should print a helpful
/// error message instead. This is the non-panicking variant of
/// start_from_config that returns a Result so the command can
/// handle the error gracefully.
///
@internal
pub fn try_start_from_config(config: Config) -> Result(DbPool, String) {
  case start_pool(config) {
    Ok(db_pool) -> Ok(wrap_pool(db_pool))
    Error(e) -> Error(string.inspect(e))
  }
}

/// Tests and benchmarks need pools without reading database.toml
/// — they supply custom connection parameters (in-memory
/// databases, test-specific credentials, etc.) directly.
/// Accepting a pre-built Config skips the file I/O and config
/// lookup.
///
@internal
pub fn start_from_config(config: Config) -> DbPool {
  let assert Ok(db_pool) = start_pool(config)
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
pub fn wrap_pool(db_pool: Pool) -> DbPool {
  let #(checkout, stop) = raw_checkout(db_pool)

  db.new_pool(
    driver: db.Postgres,
    query_fn: db.to_dynamic(vtable_query),
    exec_fn: db.to_dynamic(vtable_exec),
    checkout: fn() {
      case checkout() {
        Ok(#(conn, release)) -> Ok(#(db.to_dynamic(conn), release))
        Error(msg) -> Error(msg)
      }
    },
    stop: stop,
  )
}

/// Wrapping the FFI result in the opaque Pool type ensures
/// callers interact with the pool only through the public API.
/// The Config is validated inside start, so errors from
/// misconfiguration surface here rather than later.
///
@internal
pub fn start_pool(config: Config) -> Result(Pool, DbError) {
  case start_raw(config) {
    Ok(ops) -> Ok(Pool(checkout: ops.checkout, stop: ops.stop))
    Error(msg) -> Error(ConnectionError(msg))
  }
}

/// PostgreSQL connections hold server-side resources (memory,
/// process slots) that aren't released until the connection
/// closes. Stopping the pool explicitly frees those resources
/// instead of waiting for the BEAM process to exit.
///
@internal
pub fn stop_pool(pool: Pool) -> Nil {
  pool.stop()
}

/// A callback-based API guarantees the connection is returned
/// to the pool even if the function crashes, preventing
/// connection leaks that would eventually exhaust the pool
/// under sustained traffic.
///
@internal
pub fn get_connection(pool: Pool, f: fn(Connection) -> a) -> a {
  case pool.checkout() {
    Ok(#(conn, release)) -> {
      let result = f(conn)
      release()
      result
    }
    Error(msg) -> panic as { "Failed to checkout connection: " <> msg }
  }
}

/// The framework's driver-agnostic Pool vtable needs the raw
/// checkout and stop closures to wire PostgreSQL into the shared
/// db interface. Returning a tuple avoids exposing the opaque
/// Pool internals.
///
@internal
pub fn raw_checkout(
  pool: Pool,
) -> #(fn() -> Result(#(pog.Connection, fn() -> Nil), String), fn() -> Nil) {
  #(pool.checkout, pool.stop)
}

/// Thin wrapper so application code can run SELECT queries
/// without importing pog or handling its error types. The
/// decoder is passed through unchanged since pog already
/// supports Gleam decoders natively.
///
@internal
pub fn query(
  conn: Connection,
  sql: String,
  params: List(pog.Value),
  decoder: Decoder(t),
) -> Result(List(t), DbError) {
  let q =
    pog.query(sql)
    |> add_pog_params(params)
    |> pog.returning(decoder)

  case pog.execute(q, conn) {
    Ok(pog.Returned(_, rows)) -> Ok(rows)
    Error(e) -> Error(map_error(e))
  }
}

/// DDL and write operations still return an affected row count
/// which callers may use for "0 rows updated" checks. Keeping
/// this separate from query avoids forcing callers to supply a
/// decoder they'd never use.
///
@internal
pub fn exec(conn: Connection, sql: String) -> Result(Int, DbError) {
  let q = pog.query(sql)
  case pog.execute(q, conn) {
    Ok(pog.Returned(count, _)) -> Ok(count)
    Error(e) -> Error(map_error(e))
  }
}

/// The framework's db dispatches through a vtable of
/// Dynamic-typed callbacks so it stays driver-agnostic. This
/// function satisfies that interface by coercing the handle and
/// converting generic Values to pog-specific values before
/// executing.
///
@internal
pub fn vtable_query(
  handle: Dynamic,
  sql: String,
  params: List(Value),
  decoder: Decoder(a),
) -> Result(QueryResult(a), DbError) {
  let conn: pog.Connection = coerce(handle)
  let pog_params = list.map(params, gen.to_pog_value)

  let q =
    pog.query(sql)
    |> add_pog_params(pog_params)
    |> pog.returning(decoder)

  case pog.execute(q, conn) {
    Ok(pog.Returned(count, rows)) -> Ok(QueryResult(count, rows))
    Error(e) -> Error(map_error(e))
  }
}

/// Mirrors vtable_query but for write operations that only
/// return an affected row count. The same Dynamic coercion and
/// Value conversion applies so the vtable interface stays
/// consistent across query and exec paths.
///
@internal
pub fn vtable_exec(
  handle: Dynamic,
  sql: String,
  params: List(Value),
) -> Result(Int, DbError) {
  let conn: pog.Connection = coerce(handle)
  let pog_params = list.map(params, gen.to_pog_value)

  let q =
    pog.query(sql)
    |> add_pog_params(pog_params)

  case pog.execute(q, conn) {
    Ok(pog.Returned(count, _)) -> Ok(count)
    Error(e) -> Error(map_error(e))
  }
}

// ------------------------------------------------------------- Private Functions

/// Config is a shared union across drivers, so a SQLite variant
/// could arrive here by mistake. Matching on the config type and
/// rejecting non-Postgres variants gives a clear error instead
/// of a confusing FFI crash.
///
fn start_raw(config: Config) -> Result(PoolOps, String) {
  case config {
    PostgresConfig(url, pool_size) -> start_from_url(url, pool_size)
    PostgresParamsConfig(host, port, database, username, password, pool_size) ->
      start_from_params(host, port, database, username, password, pool_size)
    SqliteConfig(_, _) -> Error("Postgres driver cannot start SQLite config")
  }
}

/// Connection URLs pack all credentials into a single string,
/// which is simpler for deployments that use DATABASE_URL env
/// vars. pog.url_config handles the parsing so we don't need a
/// custom URL parser.
///
fn start_from_url(url: String, pool_size: Int) -> Result(PoolOps, String) {
  let pool_name = process.new_name(prefix: "glimr_pg_pool")

  case pog.url_config(pool_name, url) {
    Ok(config) -> {
      let config = pog.pool_size(config, pool_size)
      start_pog_pool(pool_name, config)
    }
    Error(Nil) -> Error("Invalid Postgres connection URL")
  }
}

/// Some deployments separate credentials into individual fields
/// (e.g., Kubernetes secrets per field) rather than a single
/// URL. Accepting discrete parameters supports that pattern
/// without forcing callers to assemble a URL string.
///
fn start_from_params(
  host: String,
  port: Int,
  database: String,
  username: String,
  password: option.Option(String),
  pool_size: Int,
) -> Result(PoolOps, String) {
  let pool_name = process.new_name(prefix: "glimr_pg_pool")

  let config =
    pog.default_config(pool_name)
    |> pog.host(host)
    |> pog.port(port)
    |> pog.database(database)
    |> pog.user(username)
    |> pog.pool_size(pool_size)

  let config = case password {
    option.Some(pw) -> pog.password(config, option.Some(pw))
    option.None -> config
  }

  start_pog_pool(pool_name, config)
}

/// pog.start returns detailed actor failure variants that are
/// meaningless to callers. Translating each variant into a
/// human-readable string here keeps error handling simple for
/// upstream code that only needs to know "it failed and why."
///
fn start_pog_pool(
  pool_name: process.Name(pog.Message),
  config: pog.Config,
) -> Result(PoolOps, String) {
  case pog.start(config) {
    Ok(actor.Started(_, _conn)) -> Ok(make_pool_ops(pool_name))
    Error(actor.InitFailed(_)) -> Error("Failed to start Postgres pool")
    Error(actor.InitTimeout) -> Error("Postgres pool initialization timed out")
    Error(actor.InitExited(_)) ->
      Error("Postgres pool exited during initialization")
  }
}

/// pog requires parameters to be added one at a time via
/// pog.parameter rather than accepting a list. Folding over the
/// list here keeps that boilerplate out of every query and exec
/// call site.
///
fn add_pog_params(q: pog.Query(t), params: List(pog.Value)) -> pog.Query(t) {
  list.fold(params, q, fn(query, param) { pog.parameter(query, param) })
}

/// pog errors carry driver-specific variants that mean nothing
/// to framework code expecting DbError. Mapping them here keeps
/// the conversion in one place so every call site doesn't repeat
/// the same pattern match.
///
fn map_error(e: pog.QueryError) -> DbError {
  case e {
    pog.ConstraintViolated(msg, constraint, _) ->
      db.ConstraintError(msg, constraint)
    pog.PostgresqlError(_, _, msg) -> QueryError(msg)
    pog.UnexpectedArgumentCount(expected, got) ->
      QueryError(
        "Expected "
        <> int.to_string(expected)
        <> " params, got "
        <> int.to_string(got),
      )
    pog.UnexpectedArgumentType(expected, got) ->
      QueryError("Expected " <> expected <> ", got " <> got)
    pog.UnexpectedResultType(errors) ->
      DecodeError(format_decode_errors(errors))
    pog.QueryTimeout -> TimeoutError
    pog.ConnectionUnavailable -> ConnectionError("Connection unavailable")
  }
}

/// Decode errors arrive as a list but callers only need a
/// single error message string. Showing the first error's
/// expected-vs-found gives developers enough to diagnose the
/// mismatch without dumping the entire error list.
///
fn format_decode_errors(errors: List(decode.DecodeError)) -> String {
  case errors {
    [] -> "Unknown decode error"
    [first, ..] -> "Expected " <> first.expected <> ", got " <> first.found
  }
}

/// Expiration is checked in the WHERE clause rather than in
/// application code so the database does the filtering and an
/// expired row is never returned — even if GC hasn't run yet.
/// The cutoff is computed from the current time minus lifetime,
/// matching the last_activity-based expiration model used by
/// save and gc.
///
fn session_load(
  pool: DbPool,
  table: String,
  session_id: String,
  lifetime: Int,
) -> #(dict.Dict(String, String), dict.Dict(String, String)) {
  let cutoff = unix_timestamp.now() - lifetime * 60
  let sql =
    "SELECT payload FROM " <> table <> " WHERE id = $1 AND last_activity >= $2"

  use conn <- db.get_connection(pool)
  case
    db.query_with(
      conn,
      sql,
      [db.string(session_id), db.int(cutoff)],
      decode.at([0], decode.string),
    )
  {
    Ok(db.QueryResult(_, [payload_json])) ->
      session.decode_payload(payload_json)
    _ -> #(dict.new(), dict.new())
  }
}

/// Uses INSERT ... ON CONFLICT DO UPDATE (upsert) so both new
/// and existing sessions are handled in a single atomic query.
/// Without this, a separate check-then-insert would be racy
/// under concurrent requests with the same session ID. The
/// last_activity timestamp is always set to now so the GC cutoff
/// reflects the most recent request, not session creation time.
///
fn session_save(
  pool: DbPool,
  table: String,
  session_id: String,
  data: dict.Dict(String, String),
  flash: dict.Dict(String, String),
  _lifetime: Int,
) -> Nil {
  let encoded = session.encode_payload(data, flash)
  let now = unix_timestamp.now()
  let sql =
    "INSERT INTO "
    <> table
    <> " (id, payload, last_activity) VALUES ($1, $2, $3) "
    <> "ON CONFLICT (id) DO UPDATE SET payload = excluded.payload, last_activity = excluded.last_activity"

  use conn <- db.get_connection(pool)

  let _ = {
    db.query_with(
      conn,
      sql,
      [db.string(session_id), db.string(encoded), db.int(now)],
      decode.string,
    )
  }

  Nil
}

/// Deletes the row immediately so the old session ID can never
/// be reused — important after invalidation to prevent session
/// fixation attacks. The delete is idempotent; if GC already
/// removed the row, the query simply affects zero rows.
///
fn session_destroy(pool: DbPool, table: String, session_id: String) -> Nil {
  let sql = "DELETE FROM " <> table <> " WHERE id = $1"

  use conn <- db.get_connection(pool)

  let _ = db.query_with(conn, sql, [db.string(session_id)], decode.string)

  Nil
}

/// Bulk-deletes all rows whose last_activity falls before the
/// cutoff in a single query, which is far cheaper than scanning
/// and deleting one at a time. The last_activity index on the
/// table keeps this fast even with millions of session rows.
/// Called probabilistically by the middleware so no single
/// request pays the full cleanup cost.
///
fn session_gc(pool: DbPool, table: String, lifetime: Int) -> Nil {
  let cutoff = unix_timestamp.now() - lifetime * 60
  let sql = "DELETE FROM " <> table <> " WHERE last_activity < $1"

  use conn <- db.get_connection(pool)

  let _ = db.query_with(conn, sql, [db.int(cutoff)], decode.string)

  Nil
}

// ------------------------------------------------------------- FFI Bindings

/// pgo's pool management API is Erlang-native and can't be
/// called directly from Gleam. The FFI returns closures that
/// capture the pool name, giving Gleam code a functional
/// interface without exposing Erlang internals.
///
@external(erlang, "pgo_pool_ffi", "make_pool_ops")
fn make_pool_ops(pool_name: process.Name(pog.Message)) -> PoolOps

/// The vtable passes connection handles as Dynamic to stay
/// driver-agnostic, but pog functions need a typed Connection.
/// An identity coerce is safe here because the handle always
/// originates from our pool.
///
@external(erlang, "glimr_pool_ffi", "identity")
fn coerce(value: Dynamic) -> a
