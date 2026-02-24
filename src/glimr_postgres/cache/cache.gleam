//// PostgreSQL Cache Operations
////
//// In-memory caches like ETS don't survive restarts and can't 
//// be shared across nodes. PostgreSQL provides durable key-
//// value storage with TTL support using a single table, 
//// keeping the deployment simple — no external services like 
//// Redis needed. Expired entries are cleaned up lazily rather 
//// than eagerly so reads stay fast and cleanup can run on a 
//// schedule.

import gleam/dynamic/decode
import gleam/int
import gleam/json.{type Json}
import gleam/result
import gleam/string
import glimr/cache/cache.{
  type CacheError, ComputeError, ConnectionError, NotFound, SerializationError,
}
import glimr/db/pool_connection
import glimr/utils/unix_timestamp
import glimr_postgres/cache/pool.{type Pool}

// ------------------------------------------------------------- Public Functions

/// Called during startup so the cache is ready before any
/// request tries to read or write. IF NOT EXISTS makes this
/// idempotent — safe to call on every boot without checking
/// whether the table already exists.
///
pub fn create_table(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "CREATE TABLE IF NOT EXISTS " <> table <> " (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expiration BIGINT NOT NULL
  )"

  case pool_connection.exec(pool.get_db_pool(pool), sql, []) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError(
        "Failed to create cache table: " <> string.inspect(e),
      ))
  }
}

/// Filtering by expiration in the WHERE clause means expired
/// entries are invisible to callers without needing a separate
/// cleanup pass on every read. Entries with expiration = 0 are
/// permanent and always returned regardless of the current
/// timestamp.
///
pub fn get(pool: Pool, key: String) -> Result(String, CacheError) {
  let table = pool.get_table(pool)
  let now = unix_timestamp.now()
  let sql =
    "SELECT value FROM "
    <> table
    <> " WHERE key = $1 AND (expiration = 0 OR expiration > $2)"

  case
    pool_connection.query(
      pool.get_db_pool(pool),
      sql,
      [pool_connection.string(key), pool_connection.int(now)],
      decode.at([0], decode.string),
    )
  {
    Ok(pool_connection.QueryResult(_, [value])) -> Ok(value)
    Ok(_) -> Error(NotFound)
    Error(e) ->
      Error(ConnectionError("Failed to get cache key: " <> string.inspect(e)))
  }
}

/// UPSERT via ON CONFLICT means callers don't need to check
/// whether a key exists before writing — new keys are inserted
/// and existing keys are updated in a single atomic operation.
/// The TTL is converted to an absolute timestamp at write time
/// so reads only need a simple comparison.
///
pub fn put(
  pool: Pool,
  key: String,
  value: String,
  ttl_seconds: Int,
) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let expiration = unix_timestamp.now() + ttl_seconds
  let sql =
    "INSERT INTO " <> table <> " (key, value, expiration) VALUES ($1, $2, $3)
    ON CONFLICT (key) DO UPDATE SET value = $2, expiration = $3"

  case
    pool_connection.exec(pool.get_db_pool(pool), sql, [
      pool_connection.string(key),
      pool_connection.string(value),
      pool_connection.int(expiration),
    ])
  {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> string.inspect(e)))
  }
}

/// Configuration values and other data that should survive
/// cache cleanup need permanent storage. Using 0 as a sentinel
/// for "never expires" keeps the schema simple — no nullable
/// column needed, and the get query already handles the check.
///
pub fn put_forever(
  pool: Pool,
  key: String,
  value: String,
) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql =
    "INSERT INTO " <> table <> " (key, value, expiration) VALUES ($1, $2, 0)
    ON CONFLICT (key) DO UPDATE SET value = $2, expiration = 0"

  case
    pool_connection.exec(pool.get_db_pool(pool), sql, [
      pool_connection.string(key),
      pool_connection.string(value),
    ])
  {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> string.inspect(e)))
  }
}

/// Returning Ok for missing keys makes forget idempotent —
/// callers can invalidate a key without first checking whether
/// it exists, avoiding a race condition between the check and
/// the delete in concurrent environments.
///
pub fn forget(pool: Pool, key: String) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "DELETE FROM " <> table <> " WHERE key = $1"

  case
    pool_connection.exec(pool.get_db_pool(pool), sql, [
      pool_connection.string(key),
    ])
  {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to delete cache key: " <> string.inspect(e)))
  }
}

/// A Bool check is simpler than matching on a Result when the
/// caller only needs to branch on presence — like deciding
/// whether to show a cached banner or fetch a fresh one.
/// Delegates to get so expiration logic isn't duplicated.
///
pub fn has(pool: Pool, key: String) -> Bool {
  case get(pool, key) {
    Ok(_) -> True
    Error(_) -> False
  }
}

/// A full cache reset is needed during deployments or when
/// cached data becomes stale across the board. Deleting all
/// rows rather than dropping the table avoids needing to
/// recreate the schema afterward.
///
pub fn flush(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "DELETE FROM " <> table

  case pool_connection.exec(pool.get_db_pool(pool), sql, []) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to flush cache: " <> string.inspect(e)))
  }
}

/// Most cached data is structured — lists, records, config
/// objects. Combining retrieval and JSON decoding in one call
/// avoids forcing callers to manually parse the string after
/// every get. SerializationError distinguishes decode failures
/// from missing keys so callers can handle each case.
///
pub fn get_json(
  pool: Pool,
  key: String,
  decoder: decode.Decoder(a),
) -> Result(a, CacheError) {
  use value <- result.try(get(pool, key))

  case json.parse(value, decoder) {
    Ok(decoded) -> Ok(decoded)
    Error(_) -> Error(SerializationError("Failed to decode JSON"))
  }
}

/// Encoding to JSON before storage ensures the value can be
/// decoded back by get_json without format mismatches. The
/// encoder function is caller-provided so any Gleam type can
/// be cached without the cache module knowing its shape.
///
pub fn put_json(
  pool: Pool,
  key: String,
  value: a,
  encoder: fn(a) -> Json,
  ttl_seconds: Int,
) -> Result(Nil, CacheError) {
  let json_string = json.to_string(encoder(value))
  put(pool, key, json_string, ttl_seconds)
}

/// Mirrors put_json but for permanent entries — configuration
/// data or computed results that are expensive to regenerate
/// and don't go stale. Delegates to put_forever so the
/// expiration = 0 sentinel is set consistently.
///
pub fn put_json_forever(
  pool: Pool,
  key: String,
  value: a,
  encoder: fn(a) -> Json,
) -> Result(Nil, CacheError) {
  let json_string = json.to_string(encoder(value))
  put_forever(pool, key, json_string)
}

/// One-time tokens like email verification codes or CSRF
/// nonces should be consumed on use — reading without deleting
/// would let the same token be replayed. Getting then deleting
/// in sequence is safe because a second concurrent pull would
/// see NotFound after the first delete.
///
pub fn pull(pool: Pool, key: String) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> {
      let _ = forget(pool, key)
      Ok(value)
    }
    Error(e) -> Error(e)
  }
}

/// Rate limiters and counters need atomic-like increments
/// without losing TTL information. Starting from 0 on missing
/// keys avoids a separate "initialize then increment" pattern,
/// and preserving the original expiration prevents a counter
/// bump from accidentally extending a rate limit window.
///
pub fn increment(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  case get(pool, key) {
    Ok(value) -> {
      case int.parse(value) {
        Ok(current) -> {
          let new_value = current + by
          // Preserve expiration by reading it first
          case get_expiration(pool, key) {
            Ok(exp) -> {
              case
                put_with_expiration(pool, key, int.to_string(new_value), exp)
              {
                Ok(_) -> Ok(new_value)
                Error(e) -> Error(e)
              }
            }
            Error(_) -> {
              case put_forever(pool, key, int.to_string(new_value)) {
                Ok(_) -> Ok(new_value)
                Error(e) -> Error(e)
              }
            }
          }
        }
        Error(_) -> Error(SerializationError("Value is not a number"))
      }
    }
    Error(NotFound) -> {
      case put_forever(pool, key, int.to_string(by)) {
        Ok(_) -> Ok(by)
        Error(e) -> Error(e)
      }
    }
    Error(e) -> Error(e)
  }
}

/// Delegating to increment with a negated value avoids
/// duplicating the TTL-preservation and missing-key logic.
/// Useful for countdown-style caches like remaining API quota 
/// or available inventory slots.
///
pub fn decrement(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  increment(pool, key, -by)
}

/// The most common caching pattern — check cache first, then
/// fall back to an expensive computation and store the result.
/// Wrapping this in a single call prevents the "check, miss,
/// compute, store" boilerplate from appearing at every call
/// site. The compute function returns a Result so database
/// or API errors propagate cleanly.
///
pub fn remember(
  pool: Pool,
  key: String,
  ttl_seconds: Int,
  compute: fn() -> Result(String, e),
) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> Ok(value)
    Error(NotFound) -> {
      case compute() {
        Ok(value) -> {
          let _ = put(pool, key, value, ttl_seconds)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

/// Mirrors remember but for permanent entries — expensive
/// computations whose results never go stale, like compiled
/// templates or static configuration derived from the
/// database. Avoids requiring a TTL when none applies.
///
pub fn remember_forever(
  pool: Pool,
  key: String,
  compute: fn() -> Result(String, e),
) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> Ok(value)
    Error(NotFound) -> {
      case compute() {
        Ok(value) -> {
          let _ = put_forever(pool, key, value)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

/// Combines remember semantics with JSON serialization so
/// structured data can be cached and computed in one call.
/// Also retries on SerializationError in case the cached
/// format changed between deployments — recomputing is safer
/// than failing on stale JSON.
///
pub fn remember_json(
  pool: Pool,
  key: String,
  ttl_seconds: Int,
  decoder: decode.Decoder(a),
  compute: fn() -> Result(a, e),
  encoder: fn(a) -> Json,
) -> Result(a, CacheError) {
  case get_json(pool, key, decoder) {
    Ok(value) -> Ok(value)
    Error(NotFound) | Error(SerializationError(_)) -> {
      case compute() {
        Ok(value) -> {
          let _ = put_json(pool, key, value, encoder, ttl_seconds)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

/// Expired entries are invisible to reads but still occupy
/// disk space. Running this on a schedule (e.g., hourly via
/// a BEAM timer) keeps the table from growing unbounded
/// without adding cleanup overhead to every read operation.
///
pub fn cleanup_expired(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let now = unix_timestamp.now()
  let sql =
    "DELETE FROM " <> table <> " WHERE expiration > 0 AND expiration <= $1"

  case
    pool_connection.exec(pool.get_db_pool(pool), sql, [
      pool_connection.int(now),
    ])
  {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError(
        "Failed to cleanup expired entries: " <> string.inspect(e),
      ))
  }
}

// ------------------------------------------------------------- Private Functions

/// Increment needs the original expiration so it can write
/// the updated value without accidentally resetting the TTL.
/// A separate query is needed because get only returns the
/// value, not the metadata.
///
fn get_expiration(pool: Pool, key: String) -> Result(Int, CacheError) {
  let table = pool.get_table(pool)
  let sql = "SELECT expiration FROM " <> table <> " WHERE key = $1"

  case
    pool_connection.query(
      pool.get_db_pool(pool),
      sql,
      [pool_connection.string(key)],
      decode.at([0], decode.int),
    )
  {
    Ok(pool_connection.QueryResult(_, [exp])) -> Ok(exp)
    Ok(_) -> Error(NotFound)
    Error(e) ->
      Error(ConnectionError("Failed to get expiration: " <> string.inspect(e)))
  }
}

/// Unlike put which calculates expiration from a TTL, this
/// accepts an absolute timestamp so increment can restore the
/// exact original expiration. The same UPSERT pattern as put
/// ensures atomicity.
///
fn put_with_expiration(
  pool: Pool,
  key: String,
  value: String,
  expiration: Int,
) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql =
    "INSERT INTO " <> table <> " (key, value, expiration) VALUES ($1, $2, $3)
    ON CONFLICT (key) DO UPDATE SET value = $2, expiration = $3"

  case
    pool_connection.exec(pool.get_db_pool(pool), sql, [
      pool_connection.string(key),
      pool_connection.string(value),
      pool_connection.int(expiration),
    ])
  {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> string.inspect(e)))
  }
}
