//// Query execution for PostgreSQL databases.
////
//// Provides functions for executing SELECT queries and
//// statements that don't return rows like INSERT, UPDATE,
//// DELETE, and DDL statements.

import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/list
import glimr/db/pool_connection.{
  type DbError, ConnectionError, DecodeError, QueryError, TimeoutError,
}
import glimr_postgres/db/pool.{type Connection}
import pog

// ------------------------------------------------------------- Public Functions

/// Executes a SELECT query and decodes the results using the
/// provided decoder. Returns a list of decoded rows on success
/// or a database error on failure.
///
pub fn query(
  conn: Connection,
  sql: String,
  params: List(pog.Value),
  decoder: Decoder(t),
) -> Result(List(t), DbError) {
  let q =
    pog.query(sql)
    |> add_pog_params(params)
    |> pog.returning(decoder)

  case pog.execute(q, conn) {
    Ok(pog.Returned(_, rows)) -> Ok(rows)
    Error(e) -> Error(map_error(e))
  }
}

/// Executes a SQL statement that does not return rows, such as
/// INSERT, UPDATE, DELETE, or DDL statements. Returns the
/// affected row count on success or a database error on failure.
///
pub fn exec(conn: Connection, sql: String) -> Result(Int, DbError) {
  let q = pog.query(sql)
  case pog.execute(q, conn) {
    Ok(pog.Returned(count, _)) -> Ok(count)
    Error(e) -> Error(map_error(e))
  }
}

// ------------------------------------------------------------- Private Functions

/// Adds parameters to a pog query. Folds over the parameter
/// list and adds each one to the query using pog.parameter
/// for proper binding.
///
fn add_pog_params(q: pog.Query(t), params: List(pog.Value)) -> pog.Query(t) {
  list.fold(params, q, fn(query, param) { pog.parameter(query, param) })
}

/// Converts a pog.QueryError to a DbError. Maps PostgreSQL
/// specific errors to the generic DbError type used across
/// the application.
///
fn map_error(e: pog.QueryError) -> DbError {
  case e {
    pog.ConstraintViolated(msg, constraint, _) ->
      pool_connection.ConstraintError(msg, constraint)
    pog.PostgresqlError(_, _, msg) -> QueryError(msg)
    pog.UnexpectedArgumentCount(expected, got) ->
      QueryError(
        "Expected "
        <> int.to_string(expected)
        <> " params, got "
        <> int.to_string(got),
      )
    pog.UnexpectedArgumentType(expected, got) ->
      QueryError("Expected " <> expected <> ", got " <> got)
    pog.UnexpectedResultType(errors) ->
      DecodeError(format_decode_errors(errors))
    pog.QueryTimeout -> TimeoutError
    pog.ConnectionUnavailable -> ConnectionError("Connection unavailable")
  }
}

/// Formats decode errors into a readable string. Takes the
/// first error from the list and formats it as an expected
/// vs found message for debugging.
///
fn format_decode_errors(errors: List(decode.DecodeError)) -> String {
  case errors {
    [] -> "Unknown decode error"
    [first, ..] -> "Expected " <> first.expected <> ", got " <> first.found
  }
}
