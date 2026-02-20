import gleam/int
import gleam/list
import gleam/result
import gleam/string
import glimr/console/console
import glimr/db/migrate as framework_migrate
import glimr_postgres/db/migrate
import glimr_postgres/db/pool.{type Pool, get_connection}

/// The three setup steps are chained with result.try so a
/// failure at any point (can't create tracking table, can't
/// read applied list, can't load migration files) stops early
/// with a descriptive error instead of proceeding with partial
/// state. Each error is tagged with a header string so the
/// console output shows which step failed.
///
pub fn run(pool: Pool, database: String) -> Nil {
  use conn <- get_connection(pool)

  let setup = {
    use _ <- result.try(
      migrate.ensure_table(conn)
      |> result.map_error(fn(e) {
        #("Failed to create migrations table:", string.inspect(e))
      }),
    )
    use applied <- result.try(
      migrate.get_applied(conn)
      |> result.map_error(fn(e) {
        #("Failed to get applied migrations:", string.inspect(e))
      }),
    )
    framework_migrate.load_all_migrations(database)
    |> result.map_error(fn(e) { #("Failed to load migrations:", e) })
    |> result.map(fn(all) { #(applied, all) })
  }

  case setup {
    Error(#(header, detail)) ->
      console.output()
      |> console.line_error(header)
      |> console.line(detail)
      |> console.print()

    Ok(#(applied, all)) -> {
      let pending = framework_migrate.get_pending_migrations(all, applied)
      apply_pending(conn, pending)
    }
  }
}

/// A dry-run view that lets users check which migrations have
/// been applied and which are still pending before committing
/// to a migrate run. Errors are silently swallowed because
/// status is informational — a failed status check shouldn't
/// crash the command or leave confusing output.
///
pub fn show_status(pool: Pool, database: String) -> Nil {
  use conn <- get_connection(pool)

  {
    use _ <- result.try(migrate.ensure_table(conn) |> result.replace_error(Nil))
    use applied <- result.try(
      migrate.get_applied(conn) |> result.replace_error(Nil),
    )
    use all <- result.try(
      framework_migrate.load_all_migrations(database)
      |> result.replace_error(Nil),
    )
    print_status(all, applied)
    Ok(Nil)
  }
  |> result.unwrap(Nil)
}

/// Handles the two cases separately: an empty pending list gets
/// a quick "nothing to do" message, while actual pending
/// migrations are applied and each version is printed with a
/// checkmark so the user can see exactly what ran. On failure
/// the error is printed rather than panicking, since a partial
/// migration is better reported than silently swallowed.
///
fn apply_pending(
  conn: pool.Connection,
  pending: List(framework_migrate.Migration),
) -> Nil {
  case pending {
    [] ->
      console.output()
      |> console.line("No pending migrations.")
      |> console.print()

    _ ->
      case migrate.apply_pending(conn, pending) {
        Ok(applied_versions) -> {
          let count = int.to_string(list.length(applied_versions))
          list.fold(
            applied_versions,
            console.output()
              |> console.line_success("Applied " <> count <> " migration(s):"),
            fn(out, version) { console.line(out, "  ✓ " <> version) },
          )
          |> console.print()
        }
        Error(e) ->
          console.output()
          |> console.line_error("Migration failed:")
          |> console.line(string.inspect(e))
          |> console.print()
      }
  }
}

/// Renders a unified view of all migrations with checkmarks
/// for applied and circles for pending, plus summary counts
/// at the bottom. Showing everything in one list makes it
/// easy to spot gaps where a migration was skipped or added
/// out of order.
///
fn print_status(
  all: List(framework_migrate.Migration),
  applied: List(String),
) -> Nil {
  let pending = framework_migrate.get_pending_migrations(all, applied)

  list.fold(
    all,
    console.output()
      |> console.line_success("Migration Status:")
      |> console.blank_line(1),
    fn(out, m) {
      let status = case list.contains(applied, m.version) {
        True -> "✓"
        False -> "○"
      }
      console.line(out, "  " <> status <> " " <> m.version <> "_" <> m.name)
    },
  )
  |> console.blank_line(1)
  |> console.line("  Applied: " <> int.to_string(list.length(applied)))
  |> console.line("  Pending: " <> int.to_string(list.length(pending)))
  |> console.print()
}
