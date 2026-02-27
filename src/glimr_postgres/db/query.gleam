//// PostgreSQL Query Execution
////
//// The pog library returns its own error types but the rest
//// of the framework expects DbError. This module bridges
//// that gap — wrapping pog calls so all query errors surface
//// as the framework's unified DbError type and callers never
//// depend on pog directly.

import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/list
import glimr/db/db.{
  type DbError, type QueryResult, type Value, ConnectionError, DecodeError,
  QueryError, QueryResult, TimeoutError,
}
import glimr_postgres/db/gen
import pog

// ------------------------------------------------------------- Public Types

/// Aliasing pog.Connection locally lets downstream code
/// reference Connection without importing pog directly, so
/// the driver can be swapped without touching every call site.
///
pub type Connection =
  pog.Connection

// ------------------------------------------------------------- Public Functions

/// Thin wrapper so application code can run SELECT queries
/// without importing pog or handling its error types. The
/// decoder is passed through unchanged since pog already
/// supports Gleam decoders natively.
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

/// DDL and write operations still return an affected row count
/// which callers may use for "0 rows updated" checks. Keeping
/// this separate from query avoids forcing callers to supply a
/// decoder they'd never use.
///
pub fn exec(conn: Connection, sql: String) -> Result(Int, DbError) {
  let q = pog.query(sql)
  case pog.execute(q, conn) {
    Ok(pog.Returned(count, _)) -> Ok(count)
    Error(e) -> Error(map_error(e))
  }
}

/// The framework's db dispatches through a vtable
/// of Dynamic-typed callbacks so it stays driver-agnostic.
/// This function satisfies that interface by coercing the
/// handle and converting generic Values to pog-specific values
/// before executing.
///
pub fn vtable_query(
  handle: Dynamic,
  sql: String,
  params: List(Value),
  decoder: Decoder(a),
) -> Result(QueryResult(a), DbError) {
  let conn: pog.Connection = coerce(handle)
  let pog_params = list.map(params, gen.to_pog_value)

  let q =
    pog.query(sql)
    |> add_pog_params(pog_params)
    |> pog.returning(decoder)

  case pog.execute(q, conn) {
    Ok(pog.Returned(count, rows)) -> Ok(QueryResult(count, rows))
    Error(e) -> Error(map_error(e))
  }
}

/// Mirrors vtable_query but for write operations that only
/// return an affected row count. The same Dynamic coercion
/// and Value conversion applies so the vtable interface stays
/// consistent across query and exec paths.
///
pub fn vtable_exec(
  handle: Dynamic,
  sql: String,
  params: List(Value),
) -> Result(Int, DbError) {
  let conn: pog.Connection = coerce(handle)
  let pog_params = list.map(params, gen.to_pog_value)

  let q =
    pog.query(sql)
    |> add_pog_params(pog_params)

  case pog.execute(q, conn) {
    Ok(pog.Returned(count, _)) -> Ok(count)
    Error(e) -> Error(map_error(e))
  }
}

// ------------------------------------------------------------- Private Functions

/// pog requires parameters to be added one at a time via
/// pog.parameter rather than accepting a list. Folding over
/// the list here keeps that boilerplate out of every query
/// and exec call site.
///
fn add_pog_params(q: pog.Query(t), params: List(pog.Value)) -> pog.Query(t) {
  list.fold(params, q, fn(query, param) { pog.parameter(query, param) })
}

/// pog errors carry driver-specific variants that mean nothing
/// to framework code expecting DbError. Mapping them here
/// keeps the conversion in one place so every call site
/// doesn't repeat the same pattern match.
///
fn map_error(e: pog.QueryError) -> DbError {
  case e {
    pog.ConstraintViolated(msg, constraint, _) ->
      db.ConstraintError(msg, constraint)
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

/// Decode errors arrive as a list but callers only need a
/// single error message string. Showing the first error's
/// expected-vs-found gives developers enough to diagnose the
/// mismatch without dumping the entire error list.
///
fn format_decode_errors(errors: List(decode.DecodeError)) -> String {
  case errors {
    [] -> "Unknown decode error"
    [first, ..] -> "Expected " <> first.expected <> ", got " <> first.found
  }
}

// ------------------------------------------------------------- FFI

/// The vtable passes connection handles as Dynamic to stay
/// driver-agnostic, but pog functions need a typed Connection.
/// An identity coerce is safe here because the handle always
/// originates from our pool.
///
@external(erlang, "glimr_pool_ffi", "identity")
fn coerce(value: Dynamic) -> a
