import gleam/dynamic/decode
import gleam/list
import gleam/string
import glimr/console/console
import glimr/db/pool_connection.{type Connection, type Pool}
import glimr_postgres/internal/actions/run_migrate

/// Drops first, then migrates — this order matters because
/// run_migrate expects an empty or partially-migrated database.
/// If the drop fails, migrations are skipped entirely to avoid
/// running them against a half-dropped schema that would produce
/// confusing duplicate-table errors.
///
pub fn run(pool: Pool, database: String) -> Nil {
  use conn <- pool_connection.get_connection(pool)

  // Drop all tables
  case drop_all_tables(conn) {
    Error(e) -> {
      console.output()
      |> console.line_error("Failed to drop tables:")
      |> console.line(string.inspect(e))
      |> console.print()
    }
    Ok(_) -> {
      console.output()
      |> console.blank_line(1)
      |> console.line_success("Tables dropped.")
      |> console.print()

      // Now run migrations on fresh database
      run_migrate.run(pool, database)
    }
  }
}

/// Queries pg_tables for the public schema to discover user
/// tables dynamically — no hardcoded table list that could go
/// stale. CASCADE ensures foreign key constraints don't block
/// the drop. Quoting table names with double quotes handles
/// tables whose names are SQL reserved words or contain special
/// characters.
///
fn drop_all_tables(conn: Connection) -> Result(Nil, pool_connection.DbError) {
  let decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  // Get all tables from public schema
  case
    pool_connection.query_with(
      conn,
      "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
      [],
      decoder,
    )
  {
    Ok(pool_connection.QueryResult(_, tables)) -> {
      list.each(tables, fn(table) {
        let _ =
          pool_connection.exec_with(
            conn,
            "DROP TABLE IF EXISTS \"" <> table <> "\" CASCADE",
            [],
          )
        Nil
      })
      Ok(Nil)
    }
    Error(_) -> Ok(Nil)
  }
}
