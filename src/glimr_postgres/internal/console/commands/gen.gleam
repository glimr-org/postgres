//// PostgreSQL Code Generation Command
////
//// Generates PostgreSQL-specific repository and migration code
//// from schema definitions.

import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import glimr/console/command.{
  type Command, type ParsedArgs, Flag, Option as CmdOption,
}
import glimr/console/console
import glimr/db/gen as db_gen
import glimr/db/gen/migrate as gen_migrate
import glimr_postgres/console/command as command_postgres
import glimr_postgres/db/pool.{type Pool}
import glimr_postgres/internal/actions/run_migrate
import simplifile

/// The name of the console command.
const name = "postgres:gen"

/// The console command description.
const description = "Generate repository and migration code for PostgreSQL"

/// Creates the postgres:gen command.
///
pub fn command() -> Command {
  command.new()
  |> command.name(name)
  |> command.description(description)
  |> command.args([
    CmdOption(
      name: "model",
      description: "Comma-separated list of models to generate",
      default: "",
    ),
    Flag(
      name: "migrate",
      short: "m",
      description: "Run migrations after generating",
    ),
  ])
  |> command_postgres.handler(run)
}

/// Executes the gen command.
///
fn run(args: ParsedArgs, pool: Pool) -> Nil {
  let database = command.get_option(args, "database")
  let model_option = command.get_option(args, "model")
  let should_migrate = command.has_flag(args, "migrate")

  // Parse comma-separated models into a list, or None if empty
  let model_filter = parse_model_filter(model_option)

  // Validate models exist if specified
  let models_path = "src/data/" <> database <> "/models"
  case validate_models(models_path, model_filter) {
    Error(invalid) -> {
      console.output()
      |> console.line_error(
        "Model(s) not found in "
        <> models_path
        <> ": "
        <> string.join(invalid, ", "),
      )
      |> console.print()
    }
    Ok(validated_filter) -> {
      // Generate migrations with "postgres" driver type
      gen_migrate.run(database, "postgres", validated_filter)

      // Generate repositories with "postgres" driver type
      db_gen.run(database, "postgres", validated_filter)

      // Optionally run migrations
      case should_migrate {
        True -> run_migrate.run(pool, database)
        False -> Nil
      }
    }
  }
}

/// Parses the model option string into an Option(List(String)).
/// Returns None if the string is empty, Some(list) otherwise.
///
fn parse_model_filter(model_option: String) -> Option(List(String)) {
  case string.trim(model_option) {
    "" -> None
    value -> {
      let models =
        value
        |> string.split(",")
        |> list.map(string.trim)
        |> list.filter(fn(s) { s != "" })

      case models {
        [] -> None
        _ -> Some(models)
      }
    }
  }
}

/// Validates that specified model directories exist.
///
fn validate_models(
  models_path: String,
  models: Option(List(String)),
) -> Result(Option(List(String)), List(String)) {
  case models {
    None -> Ok(None)
    Some(model_list) -> {
      let #(valid, invalid) =
        list.partition(model_list, fn(model) {
          case simplifile.is_directory(models_path <> "/" <> model) {
            Ok(True) -> True
            _ -> False
          }
        })
      case invalid {
        [] -> Ok(Some(valid))
        _ -> Error(invalid)
      }
    }
  }
}
