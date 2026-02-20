//// PostgreSQL Command Support
////
//// Console commands that touch the database need a connection
//// pool, but pool lifecycle management (start, use, stop) is
//// boilerplate that every command would repeat. This module
//// wraps that lifecycle so command authors just receive a live
//// pool and focus on their logic. It also validates that the
//// configured connection is actually PostgreSQL before starting
//// the pool, giving a clear error instead of a cryptic driver
//// crash.
////
//// ## Example
////
//// ```gleam
//// import glimr/console/command
//// import glimr_postgres/command as postgres_command
////
//// pub fn my_command() -> command.Command {
////   command.new()
////   |> command.name("my:command")
////   |> command.description("Does database stuff")
////   |> postgres_command.handler(fn(args, pool) {
////     // pool is glimr_postgres.Pool - fully typed!
////     use conn <- glimr_postgres.with_connection(pool)
////     // ...
////   })
//// }
//// ```

import gleam/list
import gleam/option.{None, Some}
import gleam/string
import glimr/cache/driver.{type CacheStore} as _cache_driver
import glimr/console/command.{
  type Args, type Command, CommandWithCache, CommandWithDb,
}
import glimr/console/console
import glimr/db/driver.{PostgresConnection, PostgresUriConnection}
import glimr/db/pool_connection
import glimr_postgres/db/pool.{type Pool}

// ------------------------------------------------------------- Public Functions

/// Replaces the command's run function with one that handles
/// the full pool lifecycle automatically. The --database flag
/// is injected so users can select which configured connection
/// to use at runtime. The handler receives a fully typed Pool
/// rather than a generic connection, so command authors get
/// compile-time safety without manual downcasting.
///
pub fn handler(cmd: Command, db_handler: fn(Args, Pool) -> Nil) -> Command {
  let new_args = list.append(cmd.args, [command.db_option()])

  CommandWithDb(
    name: cmd.name,
    description: cmd.description,
    args: new_args,
    driver_type: driver.Postgres,
    run_with_pool: fn(args, conn) {
      case connection_config(conn) {
        Ok(config) -> with_pool(config, fn(p) { db_handler(args, p) })
        Error(msg) -> print_error(msg)
      }
    },
  )
}

/// Cache-related commands (like cache:clear or cache:table)
/// need both a database pool and the list of configured cache
/// stores so they know which tables to operate on. This variant
/// injects both dependencies, keeping the same pool lifecycle
/// and connection validation as handler while also threading
/// through the cache store configuration.
///
pub fn cache_handler(
  cmd: Command,
  cache_db_handler: fn(Args, Pool, List(CacheStore)) -> Nil,
) -> Command {
  let new_args = list.append(cmd.args, [command.db_option()])

  CommandWithCache(
    name: cmd.name,
    description: cmd.description,
    args: new_args,
    driver_type: driver.Postgres,
    run_with_cache: fn(args, conn, cache_stores) {
      case connection_config(conn) {
        Ok(config) ->
          with_pool(config, fn(p) { cache_db_handler(args, p, cache_stores) })
        Error(msg) -> print_error(msg)
      }
    },
  )
}

// ------------------------------------------------------------- Private Functions

/// The framework's connection type is a union that covers both
/// PostgreSQL and SQLite variants. This function narrows it to
/// just the Postgres cases and extracts the fields needed to
/// start a pool. Catching missing fields here produces a human-
/// readable error instead of letting the pool driver fail with
/// an opaque crash.
///
fn connection_config(
  conn: driver.Connection,
) -> Result(pool_connection.Config, String) {
  case conn {
    PostgresUriConnection(_, Ok(url), Ok(pool_size)) ->
      Ok(pool_connection.PostgresConfig(url, pool_size))

    PostgresConnection(_, Ok(h), Ok(p), Ok(db), Ok(user), password, Ok(ps)) -> {
      let pw = case password {
        Ok(pw) -> Some(pw)
        Error(_) -> None
      }
      Ok(pool_connection.PostgresParamsConfig(h, p, db, user, pw, ps))
    }

    PostgresUriConnection(_, _, _) | PostgresConnection(_, _, _, _, _, _, _) ->
      Error("PostgreSQL connection is missing required configuration.")

    _ -> Error("Connection is not PostgreSQL. Use sqlite:* commands instead.")
  }
}

/// The pool must be stopped after the callback returns so
/// console commands don't leak connections. Wrapping start
/// and stop around the callback here means command authors
/// can't accidentally forget cleanup, and a failed start
/// prints a useful error instead of panicking.
///
fn with_pool(config: pool_connection.Config, run: fn(Pool) -> Nil) -> Nil {
  case pool.start_pool(config) {
    Ok(p) -> {
      run(p)
      pool.stop_pool(p)
    }
    Error(e) -> {
      print_error("Failed to start PostgreSQL pool:")
      console.output()
      |> console.line(string.inspect(e))
      |> console.print()
    }
  }
}

/// Centralizes error output so every failure path in this
/// module uses the same styled format. Without this, each
/// call site would repeat the console output boilerplate.
///
fn print_error(msg: String) -> Nil {
  console.output()
  |> console.line_error(msg)
  |> console.print()
}
