//// PostgreSQL Session Store
////
//// File-based sessions don't survive container restarts and
//// cookie sessions are limited to ~4KB. PostgreSQL gives
//// durable, scalable session storage that works across
//// multiple app instances sharing the same database. Sessions
//// are stored as JSON payloads with a last_activity timestamp
//// so expiration is checked on read and stale rows are cleaned
//// up by GC without parsing the payload.
////

import gleam/dict
import gleam/dynamic/decode
import glimr/config
import glimr/db/db.{type DbPool}
import glimr/session/payload
import glimr/session/store.{type SessionStore}
import glimr/utils/unix_timestamp

// ------------------------------------------------------------- Internal Public Functions

/// Captures the pool, table name, and lifetime in closures so
/// the session middleware never touches PostgreSQL directly —
/// it just calls the store interface. The cookie_value callback
/// returns the bare session ID because the actual data lives in
/// the database, not in the cookie. Config is read once here at
/// boot rather than on every request.
///
@internal
pub fn create(pool: DbPool) -> SessionStore {
  let table = config.get_string("session.table")
  let lifetime = config.get_int("session.lifetime")

  store.new(
    load: fn(session_id) { load(pool, table, session_id, lifetime) },
    save: fn(session_id, data, flash) {
      save(pool, table, session_id, data, flash, lifetime)
    },
    destroy: fn(session_id) { destroy(pool, table, session_id) },
    gc: fn() { gc(pool, table, lifetime) },
    cookie_value: fn(id, _, _) { id },
  )
}

// ------------------------------------------------------------- Private Functions

/// Expiration is checked in the WHERE clause rather than in
/// application code so the database does the filtering and an
/// expired row is never returned — even if GC hasn't run yet.
/// The cutoff is computed from the current time minus lifetime,
/// matching the last_activity-based expiration model used by
/// save and gc.
///
fn load(
  pool: DbPool,
  table: String,
  session_id: String,
  lifetime: Int,
) -> #(dict.Dict(String, String), dict.Dict(String, String)) {
  let cutoff = unix_timestamp.now() - lifetime * 60
  let sql =
    "SELECT payload FROM " <> table <> " WHERE id = $1 AND last_activity >= $2"

  use conn <- db.get_connection(pool)
  case
    db.query_with(
      conn,
      sql,
      [db.string(session_id), db.int(cutoff)],
      decode.at([0], decode.string),
    )
  {
    Ok(db.QueryResult(_, [payload_json])) -> payload.decode(payload_json)
    _ -> #(dict.new(), dict.new())
  }
}

/// Uses INSERT ... ON CONFLICT DO UPDATE (upsert) so both new
/// and existing sessions are handled in a single atomic query.
/// Without this, a separate check-then-insert would be racy
/// under concurrent requests with the same session ID. The
/// last_activity timestamp is always set to now so the GC
/// cutoff reflects the most recent request, not session
/// creation time.
///
fn save(
  pool: DbPool,
  table: String,
  session_id: String,
  data: dict.Dict(String, String),
  flash: dict.Dict(String, String),
  _lifetime: Int,
) -> Nil {
  let encoded = payload.encode(data, flash)
  let now = unix_timestamp.now()
  let sql =
    "INSERT INTO "
    <> table
    <> " (id, payload, last_activity) VALUES ($1, $2, $3) "
    <> "ON CONFLICT (id) DO UPDATE SET payload = excluded.payload, last_activity = excluded.last_activity"

  use conn <- db.get_connection(pool)

  let _ = {
    db.query_with(
      conn,
      sql,
      [
        db.string(session_id),
        db.string(encoded),
        db.int(now),
      ],
      decode.string,
    )
  }

  Nil
}

/// Deletes the row immediately so the old session ID can never
/// be reused — important after invalidation to prevent session
/// fixation attacks. The delete is idempotent; if GC already
/// removed the row, the query simply affects zero rows.
///
fn destroy(pool: DbPool, table: String, session_id: String) -> Nil {
  let sql = "DELETE FROM " <> table <> " WHERE id = $1"

  use conn <- db.get_connection(pool)

  let _ = db.query_with(conn, sql, [db.string(session_id)], decode.string)

  Nil
}

/// Bulk-deletes all rows whose last_activity falls before the
/// cutoff in a single query, which is far cheaper than scanning
/// and deleting one at a time. The last_activity index on the
/// table keeps this fast even with millions of session rows.
/// Called probabilistically by the middleware so no single
/// request pays the full cleanup cost.
///
fn gc(pool: DbPool, table: String, lifetime: Int) -> Nil {
  let cutoff = unix_timestamp.now() - lifetime * 60
  let sql = "DELETE FROM " <> table <> " WHERE last_activity < $1"

  use conn <- db.get_connection(pool)

  let _ = db.query_with(conn, sql, [db.int(cutoff)], decode.string)

  Nil
}
