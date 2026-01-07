//// Run PostgreSQL Fresh Migration
////
//// Drops all tables and re-runs all migrations from scratch.
//// Use during development to reset the database to a clean state.

import gleam/dynamic/decode
import gleam/list
import gleam/string
import glimr/console/console
import glimr_postgres/db/pool.{type Pool, get_connection}
import glimr_postgres/internal/actions/run_migrate
import pog

// ------------------------------------------------------------- Public Functions

/// Drops all tables and re-runs all migrations.
///
pub fn run(pool: Pool, database: String) -> Nil {
  use conn <- get_connection(pool)

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
      |> console.unpadded()
      |> console.print()

      // Now run migrations on fresh database
      run_migrate.run(pool, database)
    }
  }
}

/// Drops all user tables from the PostgreSQL database.
///
fn drop_all_tables(conn: pog.Connection) -> Result(Nil, pog.QueryError) {
  let decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  // Get all tables from public schema
  let query =
    pog.query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    |> pog.returning(decoder)

  case pog.execute(query, conn) {
    Ok(pog.Returned(_, tables)) -> {
      list.each(tables, fn(table) {
        let drop_query =
          pog.query("DROP TABLE IF EXISTS \"" <> table <> "\" CASCADE")
        let _ = pog.execute(drop_query, conn)
        Nil
      })
      Ok(Nil)
    }
    Error(_) -> Ok(Nil)
  }
}
