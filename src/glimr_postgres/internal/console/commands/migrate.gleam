//// PostgreSQL Migration Command
////
//// Runs pending migrations for PostgreSQL databases.

import glimr/console/command.{type Command, type ParsedArgs, Flag}
import glimr_postgres/console/command as command_postgres
import glimr_postgres/db/pool.{type Pool}
import glimr_postgres/internal/actions/run_fresh
import glimr_postgres/internal/actions/run_migrate

/// The name of the console command.
const name = "postgres:migrate"

/// The console command description.
const description = "Run pending PostgreSQL migrations"

/// Creates the postgres:migrate command.
///
pub fn command() -> Command {
  command.new()
  |> command.name(name)
  |> command.description(description)
  |> command.args([
    Flag(
      name: "fresh",
      short: "f",
      description: "Drop all tables and re-run all migrations",
    ),
    Flag(
      name: "status",
      short: "s",
      description: "Show migration status without running",
    ),
  ])
  |> command_postgres.handler(run)
}

/// Executes the migrate command.
///
fn run(args: ParsedArgs, pool: Pool) -> Nil {
  let database = command.get_option(args, "database")
  let fresh = command.has_flag(args, "fresh")
  let status = command.has_flag(args, "status")

  case status {
    True -> run_migrate.show_status(pool, database)
    False -> {
      case fresh {
        True -> run_fresh.run(pool, database)
        False -> run_migrate.run(pool, database)
      }
    }
  }
}
