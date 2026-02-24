//// PostgreSQL Connection Pool
////
//// Opening a new PostgreSQL connection per query adds TCP
//// handshake and authentication overhead on every request.
//// A pool keeps a fixed set of connections open and lends
//// them out on demand, so queries execute against already-
//// authenticated sockets and callers never need to manage
//// connection lifecycle themselves.

import gleam/erlang/process
import gleam/option
import gleam/otp/actor
import glimr/db/pool_connection.{
  type Config, ConnectionError, PostgresConfig, PostgresParamsConfig,
  SqliteConfig,
}
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

/// The Erlang FFI returns closures that capture the pool
/// handle internally, so a named record type is needed to
/// receive them across the FFI boundary. This stays public
/// so the FFI module can construct it.
///
pub type PoolOps {
  PoolOps(
    checkout: fn() -> Result(#(pog.Connection, fn() -> Nil), String),
    stop: fn() -> Nil,
  )
}

/// Re-exporting pog.Connection under a local alias lets the
/// rest of the codebase reference Connection without depending
/// on pog directly, so swapping the underlying driver only
/// requires changes here.
///
pub type Connection =
  pog.Connection

/// Re-exporting DbError here keeps downstream modules from
/// importing pool_connection just for the error type,
/// reducing coupling to the framework internals.
///
pub type DbError =
  pool_connection.DbError

// ------------------------------------------------------------- Public Functions

/// Wrapping the FFI result in the opaque Pool type ensures
/// callers interact with the pool only through the public
/// API. The Config is validated inside start, so errors from
/// misconfiguration surface here rather than later.
///
pub fn start_pool(config: Config) -> Result(Pool, DbError) {
  case start(config) {
    Ok(ops) -> Ok(Pool(checkout: ops.checkout, stop: ops.stop))
    Error(msg) -> Error(ConnectionError(msg))
  }
}

/// PostgreSQL connections hold server-side resources (memory,
/// process slots) that aren't released until the connection
/// closes. Stopping the pool explicitly frees those resources
/// instead of waiting for the BEAM process to exit.
///
pub fn stop_pool(pool: Pool) -> Nil {
  pool.stop()
}

/// A callback-based API guarantees the connection is returned
/// to the pool even if the function crashes, preventing
/// connection leaks that would eventually exhaust the pool
/// under sustained traffic.
///
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
/// checkout and stop closures to wire PostgreSQL into the
/// shared pool_connection interface. Returning a tuple avoids
/// exposing the opaque Pool internals.
///
pub fn raw_checkout(
  pool: Pool,
) -> #(fn() -> Result(#(pog.Connection, fn() -> Nil), String), fn() -> Nil) {
  #(pool.checkout, pool.stop)
}

// ------------------------------------------------------------- Private Functions

/// Config is a shared union across drivers, so a SQLite variant
/// could arrive here by mistake. Matching on the config type
/// and rejecting non-Postgres variants gives a clear error
/// instead of a confusing FFI crash.
///
fn start(config: Config) -> Result(PoolOps, String) {
  case config {
    PostgresConfig(url, pool_size) -> start_from_url(url, pool_size)
    PostgresParamsConfig(host, port, database, username, password, pool_size) ->
      start_from_params(host, port, database, username, password, pool_size)
    SqliteConfig(_, _) -> Error("Postgres driver cannot start SQLite config")
  }
}

/// Connection URLs pack all credentials into a single string,
/// which is simpler for deployments that use DATABASE_URL env
/// vars. pog.url_config handles the parsing so we don't need
/// a custom URL parser.
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

// ------------------------------------------------------------- FFI Bindings

/// pgo's pool management API is Erlang-native and can't be
/// called directly from Gleam. The FFI returns closures that
/// capture the pool name, giving Gleam code a functional
/// interface without exposing Erlang internals.
///
@external(erlang, "pgo_pool_ffi", "make_pool_ops")
fn make_pool_ops(pool_name: process.Name(pog.Message)) -> PoolOps
