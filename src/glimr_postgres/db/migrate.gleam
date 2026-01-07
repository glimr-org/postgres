//// PostgreSQL Migration Database Operations
////
//// Provides database operations for running migrations. Handles
//// the migrations tracking table and applying migration SQL
//// to the database.

import gleam/dynamic/decode
import gleam/list
import gleam/string
import glimr/db/migrate as framework_migrate
import glimr_postgres/db/pool.{type Connection}
import pog

// ------------------------------------------------------------- Public Functions

/// Creates the migrations tracking table if it doesn't exist.
/// Uses _glimr_migrations to track which migrations have been
/// applied to the database.
///
pub fn ensure_table(conn: Connection) -> Result(Nil, pog.QueryError) {
  let query =
    pog.query(
      "CREATE TABLE IF NOT EXISTS _glimr_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )",
    )

  case pog.execute(query, conn) {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(e)
  }
}

/// Gets the list of applied migration versions from the database.
/// Returns versions in sorted order for comparison against
/// available migration files.
///
pub fn get_applied(conn: Connection) -> Result(List(String), pog.QueryError) {
  let decoder = {
    use version <- decode.field(0, decode.string)
    decode.success(version)
  }

  let query =
    pog.query("SELECT version FROM _glimr_migrations ORDER BY version")
    |> pog.returning(decoder)

  case pog.execute(query, conn) {
    Ok(pog.Returned(_, rows)) -> Ok(rows)
    Error(e) -> Error(e)
  }
}

/// Applies a list of migrations, stopping on first error.
/// Returns the list of successfully applied version strings
/// or the first error encountered.
///
pub fn apply_pending(
  conn: Connection,
  pending: List(framework_migrate.Migration),
) -> Result(List(String), pog.QueryError) {
  do_apply_pending(conn, pending, [])
}

// ------------------------------------------------------------- Private Functions

/// Recursive implementation of apply_pending that accumulates
/// applied versions. Processes migrations one at a time and
/// stops on first error.
///
fn do_apply_pending(
  conn: Connection,
  pending: List(framework_migrate.Migration),
  applied: List(String),
) -> Result(List(String), pog.QueryError) {
  case pending {
    [] -> Ok(list.reverse(applied))
    [migration, ..rest] -> {
      case apply_single(conn, migration) {
        Ok(_) -> do_apply_pending(conn, rest, [migration.version, ..applied])
        Error(err) -> Error(err)
      }
    }
  }
}

/// Applies a single migration and records it in the tracking
/// table. Splits SQL into statements and executes each one
/// sequentially.
///
fn apply_single(
  conn: Connection,
  migration: framework_migrate.Migration,
) -> Result(Nil, pog.QueryError) {
  let sql = framework_migrate.extract_sql(migration.sql)

  let statements =
    sql
    |> string.split(";")
    |> list.map(string.trim)
    |> list.filter(fn(s) { s != "" })

  case execute_statements(conn, statements) {
    Ok(_) -> {
      let query =
        pog.query("INSERT INTO _glimr_migrations (version) VALUES ($1)")
        |> pog.parameter(pog.text(migration.version))

      case pog.execute(query, conn) {
        Ok(_) -> Ok(Nil)
        Error(err) -> Error(err)
      }
    }
    Error(err) -> Error(err)
  }
}

/// Executes a list of SQL statements sequentially. Processes
/// each statement in order and stops on first error, returning
/// the error to the caller.
///
fn execute_statements(
  conn: Connection,
  statements: List(String),
) -> Result(Nil, pog.QueryError) {
  case statements {
    [] -> Ok(Nil)
    [stmt, ..rest] -> {
      let query = pog.query(stmt)
      case pog.execute(query, conn) {
        Ok(_) -> execute_statements(conn, rest)
        Error(err) -> Error(err)
      }
    }
  }
}
