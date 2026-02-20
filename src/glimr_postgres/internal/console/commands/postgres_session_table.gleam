import glimr/config/session as session_config
import glimr/console/command.{type Args, type Command, Flag}
import glimr_postgres/console/command as command_postgres
import glimr_postgres/db/pool.{type Pool}
import glimr_postgres/internal/actions/gen_session_table
import glimr_postgres/internal/actions/run_migrate

/// The console command description.
const description = "Generate session table migration for PostgreSQL"

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
  |> command_postgres.handler(run)
}

/// Execute the console command.
///
fn run(args: Args, pool: Pool) -> Nil {
  let database = command.get_option(args, "database")
  let should_migrate = command.has_flag(args, "migrate")
  let config = session_config.load()

  gen_session_table.run(database, config.table)

  case should_migrate {
    True -> run_migrate.run(pool, database)
    False -> Nil
  }
}

/// Console command's entry point
///
pub fn main() {
  command.run(command())
}
