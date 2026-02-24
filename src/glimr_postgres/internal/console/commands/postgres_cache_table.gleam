import glimr/cache/driver.{type CacheStore, DatabaseStore}
import glimr/console/command.{type Args, type Command, Flag}
import glimr/console/console
import glimr/db/pool_connection.{type Pool}
import glimr_postgres/console/command as command_postgres
import glimr_postgres/internal/actions/gen_cache_table
import glimr_postgres/internal/actions/run_migrate

/// The console command description.
const description = "Generate cache table migration for PostgreSQL"

/// Define the console command and its properties.
///
pub fn command() -> Command {
  command.new()
  |> command.description(description)
  |> command.args([
    Flag(
      name: "migrate",
      short: "m",
      description: "Run migrations after generating",
    ),
  ])
  |> command_postgres.cache_handler(run)
}

/// Execute the console command.
///
fn run(args: Args, pool: Pool, cache_stores: List(CacheStore)) -> Nil {
  let database = command.get_option(args, "database")
  let should_migrate = command.has_flag(args, "migrate")

  case driver.find_database_store(database, cache_stores) {
    Ok(DatabaseStore(_, _, table)) -> {
      gen_cache_table.run(database, table)

      case should_migrate {
        True -> run_migrate.run(pool, database)
        False -> Nil
      }
    }
    Ok(_) -> {
      console.output()
      |> console.line_error("Unexpected cache store type")
      |> console.print()
    }
    Error(msg) -> {
      console.output()
      |> console.line_error(msg)
      |> console.print()
    }
  }
}

/// Console command's entry point
///
pub fn main() {
  command.run(command())
}
