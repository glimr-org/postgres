//// PostgreSQL Command Support
////
//// Provides helpers for creating console commands that need
//// PostgreSQL database access. The handler function wraps your
//// command logic with automatic pool management.
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
import glimr/console/command.{type Command, type ParsedArgs, CommandWithDb}
import glimr/console/console
import glimr/db/driver.{PostgresConnection, PostgresUriConnection}
import glimr/db/pool_connection
import glimr_postgres/db/pool.{type Pool}

/// Sets a database handler for a command. Automatically:
/// - Adds the --database option
/// - Validates the connection exists and is PostgreSQL
/// - Starts a typed pool
/// - Calls your handler with the pool
/// - Stops the pool when done
///
/// Your handler receives a fully typed `glimr_postgres.Pool`.
///
pub fn handler(cmd: Command, db_handler: fn(ParsedArgs, Pool) -> Nil) -> Command {
  // Add --database option to existing args
  let new_args = list.append(cmd.args, [command.db_option()])

  CommandWithDb(
    name: cmd.name,
    description: cmd.description,
    args: new_args,
    driver_type: driver.Postgres,
    run_with_pool: fn(args, conn) {
      case conn {
        PostgresUriConnection(_, url, pool_size) -> {
          case url, pool_size {
            Ok(u), Ok(ps) -> {
              let config = pool_connection.PostgresConfig(u, ps)
              start_pool_and_run(config, args, db_handler)
            }
            _, _ -> {
              console.output()
              |> console.line_error(
                "PostgreSQL connection is missing required configuration.",
              )
              |> console.print()
            }
          }
        }

        PostgresConnection(
          _,
          host,
          port,
          database,
          username,
          password,
          pool_size,
        ) -> {
          case host, port, database, username, pool_size {
            Ok(h), Ok(p), Ok(db), Ok(user), Ok(ps) -> {
              let pw = case password {
                Ok(pw) -> Some(pw)
                Error(_) -> None
              }
              let config =
                pool_connection.PostgresParamsConfig(h, p, db, user, pw, ps)
              start_pool_and_run(config, args, db_handler)
            }
            _, _, _, _, _ -> {
              console.output()
              |> console.line_error(
                "PostgreSQL connection is missing required configuration.",
              )
              |> console.print()
            }
          }
        }

        _ -> {
          console.output()
          |> console.line_error(
            "Connection is not PostgreSQL. Use sqlite:* commands instead.",
          )
          |> console.print()
        }
      }
    },
  )
}

/// Helper to start pool, run handler, and stop pool.
///
fn start_pool_and_run(
  config: pool_connection.Config,
  args: ParsedArgs,
  db_handler: fn(ParsedArgs, Pool) -> Nil,
) -> Nil {
  case pool.start_pool(config) {
    Ok(p) -> {
      db_handler(args, p)
      pool.stop_pool(p)
    }
    Error(e) -> {
      console.output()
      |> console.line_error("Failed to start PostgreSQL pool:")
      |> console.line(string.inspect(e))
      |> console.print()
    }
  }
}
