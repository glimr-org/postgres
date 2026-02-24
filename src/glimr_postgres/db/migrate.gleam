//// PostgreSQL Migration Database Operations
////
//// The console migration commands need to read which migrations
//// have already run and apply new ones, but that logic is
//// driver-specific — PostgreSQL supports multi-statement
//// execution differently from SQLite and uses dollar-quoted
//// statements. Keeping the SQL execution here separates the
//// database concerns from the console output and file scanning
//// in the action modules.

import gleam/dynamic/decode
import gleam/list
import gleam/string
import glimr/db/migrate as framework_migrate
import glimr/db/pool_connection.{type Connection, type DbError}

// ------------------------------------------------------------- Public Functions

/// The tracking table must exist before any migration can be
/// recorded. IF NOT EXISTS makes this idempotent — safe to call
/// on every migrate run without checking whether the table was
/// already created on a previous run.
///
pub fn ensure_table(conn: Connection) -> Result(Nil, DbError) {
  pool_connection.exec_with(
    conn,
    "CREATE TABLE IF NOT EXISTS _glimr_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )",
    [],
  )
  |> result_to_nil
}

/// Sorting by version ensures the returned list matches the
/// order of migration files on disk, so the caller can diff
/// the two lists to find pending migrations without additional
/// sorting logic.
///
pub fn get_applied(conn: Connection) -> Result(List(String), DbError) {
  let decoder = {
    use version <- decode.field(0, decode.string)
    decode.success(version)
  }

  case
    pool_connection.query_with(
      conn,
      "SELECT version FROM _glimr_migrations ORDER BY version",
      [],
      decoder,
    )
  {
    Ok(pool_connection.QueryResult(_, rows)) -> Ok(rows)
    Error(e) -> Error(e)
  }
}

/// Stopping on the first error prevents later migrations from
/// running against an inconsistent schema. Returning the list
/// of successfully applied versions lets the console command
/// report exactly which migrations succeeded before the failure.
///
pub fn apply_pending(
  conn: Connection,
  pending: List(framework_migrate.Migration),
) -> Result(List(String), DbError) {
  do_apply_pending(conn, pending, [])
}

// ------------------------------------------------------------- Private Functions

/// Separated from apply_pending to hide the accumulator from
/// the public API. Processing one migration at a time ensures
/// each version is recorded in the tracking table before the
/// next one starts, so a crash mid-run leaves a clean state.
///
fn do_apply_pending(
  conn: Connection,
  pending: List(framework_migrate.Migration),
  applied: List(String),
) -> Result(List(String), DbError) {
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

/// Migration files can contain multiple statements separated by
/// semicolons. Each statement must run individually so errors
/// surface at the right statement. Recording the version in the
/// tracking table only after all statements succeed prevents a
/// partially-applied migration from being marked as done.
///
fn apply_single(
  conn: Connection,
  migration: framework_migrate.Migration,
) -> Result(Nil, DbError) {
  let sql = framework_migrate.extract_sql(migration.sql)

  let statements =
    sql
    |> string.split(";")
    |> list.map(string.trim)
    |> list.filter(fn(s) { s != "" })

  case execute_statements(conn, statements) {
    Ok(_) -> {
      pool_connection.exec_with(
        conn,
        "INSERT INTO _glimr_migrations (version) VALUES ($1)",
        [pool_connection.string(migration.version)],
      )
      |> result_to_nil
    }
    Error(err) -> Error(err)
  }
}

/// Stopping on first error ensures later statements that depend
/// on earlier schema changes don't run against a broken state.
/// The error propagates up to apply_single which skips the
/// tracking table insert, leaving the migration unapplied.
///
fn execute_statements(
  conn: Connection,
  statements: List(String),
) -> Result(Nil, DbError) {
  case statements {
    [] -> Ok(Nil)
    [stmt, ..rest] -> {
      case pool_connection.exec_with(conn, stmt, []) {
        Ok(_) -> execute_statements(conn, rest)
        Error(err) -> Error(err)
      }
    }
  }
}

/// pool_connection.exec_with returns the affected row count but
/// callers here only care about success or failure. Discarding
/// the Int keeps the return types consistent across all
/// functions in this module.
///
fn result_to_nil(result: Result(Int, DbError)) -> Result(Nil, DbError) {
  case result {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(e)
  }
}
