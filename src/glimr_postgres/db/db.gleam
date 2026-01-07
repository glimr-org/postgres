//// Database transaction support for PostgreSQL.
////
//// Provides transaction execution with automatic retry on
//// deadlock errors. Transactions are committed on success
//// or rolled back on error.

import gleam/erlang/process
import gleam/string
import glimr/db/pool_connection.{type DbError, ConnectionError, QueryError}
import glimr_postgres/db/pool.{type Connection, type Pool, get_connection}
import pog

// ------------------------------------------------------------- Public Functions

/// Executes a function within a database transaction. The
/// transaction is committed on success or rolled back on error.
/// The retries parameter controls how many times to retry on
/// deadlock errors.
///
pub fn transaction(
  pool: Pool,
  retries: Int,
  callback: fn(Connection) -> Result(a, DbError),
) -> Result(a, DbError) {
  case retries < 0 {
    True -> Error(ConnectionError("Transaction retries cannot be negative"))
    False -> do_transaction(pool, retries, callback)
  }
}

// ------------------------------------------------------------- Private Functions

/// Internal implementation of transaction execution. Checks out
/// a connection, runs BEGIN/COMMIT/ROLLBACK, and delegates to
/// maybe_retry on failure for deadlock handling.
///
fn do_transaction(
  pool: Pool,
  retries_remaining: Int,
  callback: fn(Connection) -> Result(a, DbError),
) -> Result(a, DbError) {
  get_connection(pool, fn(conn) {
    let begin_query = pog.query("BEGIN")
    case pog.execute(begin_query, conn) {
      Error(e) -> Error(map_error(e))
      Ok(_) -> {
        case callback(conn) {
          Ok(value) -> {
            let commit_query = pog.query("COMMIT")
            case pog.execute(commit_query, conn) {
              Ok(_) -> Ok(value)
              Error(e) -> {
                let rollback_query = pog.query("ROLLBACK")
                let _ = pog.execute(rollback_query, conn)
                maybe_retry(pool, retries_remaining, callback, map_error(e))
              }
            }
          }
          Error(e) -> {
            let rollback_query = pog.query("ROLLBACK")
            let _ = pog.execute(rollback_query, conn)
            maybe_retry(pool, retries_remaining, callback, e)
          }
        }
      }
    }
  })
}

/// Handles retry logic for failed transactions. If the error is
/// a deadlock and retries remain, waits with backoff and retries.
/// Otherwise returns the error immediately.
///
fn maybe_retry(
  pool: Pool,
  retries_remaining: Int,
  callback: fn(Connection) -> Result(a, DbError),
  error: DbError,
) -> Result(a, DbError) {
  case is_deadlock_error(error) && retries_remaining > 0 {
    True -> {
      process.sleep(50 * retries_remaining)
      do_transaction(pool, retries_remaining - 1, callback)
    }
    False -> Error(error)
  }
}

/// Checks if an error indicates a deadlock or lock contention.
/// Looks for common PostgreSQL lock-related keywords in the error
/// message to determine if a retry might succeed.
///
fn is_deadlock_error(error: DbError) -> Bool {
  case error {
    QueryError(msg) -> {
      let lower = string.lowercase(msg)
      string.contains(lower, "deadlock")
      || string.contains(lower, "lock")
      || string.contains(lower, "serialization")
    }
    _ -> False
  }
}

/// Converts a pog.QueryError to a DbError. Maps PostgreSQL
/// specific errors to the generic DbError type used across
/// the application for consistent error handling.
///
fn map_error(e: pog.QueryError) -> DbError {
  case e {
    pog.PostgresqlError(_, _, msg) -> QueryError(msg)
    pog.ConnectionUnavailable -> ConnectionError("Connection unavailable")
    _ -> QueryError("Query failed")
  }
}
