//// Run PostgreSQL Migrations
////
//// Console action for running pending migrations. Handles
//// user output and delegates to db/migrate for database
//// operations.

import gleam/int
import gleam/list
import gleam/string
import glimr/console/console
import glimr/db/migrate as framework_migrate
import glimr_postgres/db/migrate
import glimr_postgres/db/pool.{type Pool, get_connection}

// ------------------------------------------------------------- Public Functions

/// Runs all pending migrations for a PostgreSQL database.
/// Outputs progress and results to the console.
///
pub fn run(pool: Pool, database: String) -> Nil {
  use conn <- get_connection(pool)

  case migrate.ensure_table(conn) {
    Error(e) -> {
      console.output()
      |> console.line_error("Failed to create migrations table:")
      |> console.line(string.inspect(e))
      |> console.print()
    }
    Ok(_) -> {
      case migrate.get_applied(conn) {
        Error(e) -> {
          console.output()
          |> console.line_error("Failed to get applied migrations:")
          |> console.line(string.inspect(e))
          |> console.print()
        }
        Ok(applied) -> {
          case framework_migrate.load_all_migrations(database) {
            Error(e) -> {
              console.output()
              |> console.line_error("Failed to load migrations:")
              |> console.line(e)
              |> console.print()
            }
            Ok(all) -> {
              let pending =
                framework_migrate.get_pending_migrations(all, applied)

              case pending {
                [] -> {
                  console.output()
                  |> console.line("No pending migrations.")
                  |> console.print()
                }
                _ -> {
                  case migrate.apply_pending(conn, pending) {
                    Ok(applied_versions) -> {
                      let count = int.to_string(list.length(applied_versions))
                      let output =
                        console.output()
                        |> console.line_success(
                          "Applied " <> count <> " migration(s):",
                        )

                      let output =
                        list.fold(applied_versions, output, fn(out, version) {
                          console.line(out, "  ✓ " <> version)
                        })

                      console.print(output)
                    }
                    Error(e) -> {
                      console.output()
                      |> console.line_error("Migration failed:")
                      |> console.line(string.inspect(e))
                      |> console.print()
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

/// Shows the status of migrations without running them.
/// Displays which migrations are applied and which are pending.
///
pub fn show_status(pool: Pool, database: String) -> Nil {
  use conn <- get_connection(pool)

  case migrate.ensure_table(conn) {
    Error(_) -> Nil
    Ok(_) -> {
      case migrate.get_applied(conn) {
        Error(_) -> Nil
        Ok(applied) -> {
          case framework_migrate.load_all_migrations(database) {
            Error(_) -> Nil
            Ok(all) -> {
              let output =
                console.output()
                |> console.line_success("Migration Status:")
                |> console.blank_line(1)

              let output =
                list.fold(all, output, fn(out, m) {
                  let status = case list.contains(applied, m.version) {
                    True -> "✓"
                    False -> "○"
                  }
                  console.line(
                    out,
                    "  " <> status <> " " <> m.version <> "_" <> m.name,
                  )
                })

              let pending =
                framework_migrate.get_pending_migrations(all, applied)
              let output =
                output
                |> console.blank_line(1)
                |> console.line(
                  "  Applied: " <> int.to_string(list.length(applied)),
                )
                |> console.line(
                  "  Pending: " <> int.to_string(list.length(pending)),
                )

              console.print(output)
            }
          }
        }
      }
    }
  }
}
