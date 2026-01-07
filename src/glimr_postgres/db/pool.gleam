//// PostgreSQL connection pool management.
////
//// Provides connection pooling for PostgreSQL databases. Pools manage
//// a set of reusable connections and handle checkout/checkin
//// automatically through the get_connection function.

import gleam/erlang/process
import gleam/option
import gleam/otp/actor
import glimr/db/pool_connection.{
  type Config, ConnectionError, PostgresConfig, PostgresParamsConfig,
  SqliteConfig,
}
import pog

// ------------------------------------------------------------- Public Types

/// A PostgreSQL connection pool. Manages reusable database connections
/// and handles checkout/checkin automatically. Created with
/// start_pool and should be stopped with stop_pool when done.
///
pub opaque type Pool {
  Pool(
    checkout: fn() -> Result(#(pog.Connection, fn() -> Nil), String),
    stop: fn() -> Nil,
  )
}

/// Pool operations returned from FFI. Contains closures that
/// capture the internal pool handle, providing checkout and
/// stop functionality without exposing Erlang internals.
///
pub type PoolOps {
  PoolOps(
    checkout: fn() -> Result(#(pog.Connection, fn() -> Nil), String),
    stop: fn() -> Nil,
  )
}

/// A PostgreSQL database connection. Obtained through get_connection
/// and should not be stored or used outside the callback. This
/// is an alias for the underlying pog.Connection type.
///
pub type Connection =
  pog.Connection

/// Represents errors that can occur during pool operations.
/// Re-exported from pool_connection for convenience.
///
pub type DbError =
  pool_connection.DbError

// ------------------------------------------------------------- Public Functions

/// Creates a new connection pool from the given configuration.
/// The pool manages a set of reusable database connections and
/// handles checkout/checkin automatically.
///
pub fn start_pool(config: Config) -> Result(Pool, DbError) {
  case start(config) {
    Ok(ops) -> Ok(Pool(checkout: ops.checkout, stop: ops.stop))
    Error(msg) -> Error(ConnectionError(msg))
  }
}

/// Stops a connection pool and closes all connections. Should
/// be called when the pool is no longer needed to free
/// resources. Any connections still in use will be closed.
///
pub fn stop_pool(pool: Pool) -> Nil {
  pool.stop()
}

/// Executes a function with a connection from the pool. The
/// connection is automatically checked out before the function
/// runs and returned to the pool when it completes.
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

// ------------------------------------------------------------- Internal Functions

/// Starts a PostgreSQL connection pool from the given configuration.
/// Only accepts PostgresConfig, returns an error for SQLite
/// configurations. Returns PoolOps with closures on success.
///
fn start(config: Config) -> Result(PoolOps, String) {
  case config {
    PostgresConfig(url, pool_size) -> start_from_url(url, pool_size)
    PostgresParamsConfig(host, port, database, username, password, pool_size) ->
      start_from_params(host, port, database, username, password, pool_size)
    SqliteConfig(_, _) -> Error("Postgres driver cannot start SQLite config")
  }
}

// ------------------------------------------------------------- Private Functions

/// Starts a pool from a connection URL. Parses the URL using
/// pog.url_config and configures the pool size before starting
/// the underlying pog pool.
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

/// Starts a pool from individual connection parameters. Builds
/// a pog config from host, port, database, username, password,
/// and pool size settings.
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

/// Starts the underlying pog pool with the given config. Creates
/// PoolOps via FFI on success or returns an error message
/// describing the failure.
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

/// Creates PoolOps with closures that capture the pool name.
/// The Erlang side returns checkout and stop functions that
/// manage the underlying pgo connection pool.
///
@external(erlang, "pgo_pool_ffi", "make_pool_ops")
fn make_pool_ops(pool_name: process.Name(pog.Message)) -> PoolOps
