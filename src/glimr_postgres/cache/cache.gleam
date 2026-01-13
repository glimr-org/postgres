//// PostgreSQL Cache Operations
////
//// Provides cache operations using PostgreSQL as the storage backend.
//// Cache entries are stored in a database table with key, value,
//// and expiration columns. Expired entries are cleaned up lazily.

import gleam/dynamic/decode
import gleam/int
import gleam/json.{type Json}
import gleam/result
import glimr/cache/cache.{
  type CacheError, ComputeError, ConnectionError, NotFound, SerializationError,
}
import glimr/utils/unix_timestamp
import glimr_postgres/cache/pool.{type Pool}
import glimr_postgres/db/pool as db_pool
import pog

// ------------------------------------------------------------- Public Functions

/// Creates the cache table if it doesn't exist. Should be
/// called during application startup or migration to ensure
/// the cache storage is ready.
///
pub fn create_table(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "CREATE TABLE IF NOT EXISTS " <> table <> " (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expiration BIGINT NOT NULL
  )"

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case pog.query(sql) |> pog.execute(conn) {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError(
          "Failed to create cache table: " <> error_to_string(e),
        ))
    }
  })
}

/// Retrieves a value from the cache by key. Returns NotFound
/// if the key doesn't exist or has expired. Expired entries
/// remain in the table until cleanup_expired is called.
///
pub fn get(pool: Pool, key: String) -> Result(String, CacheError) {
  let table = pool.get_table(pool)
  let now = unix_timestamp.now()
  let sql =
    "SELECT value FROM "
    <> table
    <> " WHERE key = $1 AND (expiration = 0 OR expiration > $2)"

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.parameter(pog.int(now))
      |> pog.returning(decode.at([0], decode.string))
      |> pog.execute(conn)
    {
      Ok(pog.Returned(1, [value])) -> Ok(value)
      Ok(pog.Returned(_, [])) -> Error(NotFound)
      Ok(_) -> Error(NotFound)
      Error(e) ->
        Error(ConnectionError("Failed to get cache key: " <> error_to_string(e)))
    }
  })
}

/// Stores a value in the cache with a TTL (time-to-live) in
/// seconds. Overwrites any existing value for the same key
/// with the new value and expiration.
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

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.parameter(pog.text(value))
      |> pog.parameter(pog.int(expiration))
      |> pog.execute(conn)
    {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
    }
  })
}

/// Stores a value in the cache permanently (no expiration).
/// Uses expiration value of 0 to indicate the entry never
/// expires.
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

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.parameter(pog.text(value))
      |> pog.execute(conn)
    {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
    }
  })
}

/// Removes a value from the cache by key. Returns Ok even if
/// the key didn't exist, making it safe to call without
/// checking existence first.
///
pub fn forget(pool: Pool, key: String) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "DELETE FROM " <> table <> " WHERE key = $1"

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.execute(conn)
    {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError(
          "Failed to delete cache key: " <> error_to_string(e),
        ))
    }
  })
}

/// Checks if a key exists in the cache and hasn't expired.
/// Uses get internally to check both existence and
/// expiration status.
///
pub fn has(pool: Pool, key: String) -> Bool {
  case get(pool, key) {
    Ok(_) -> True
    Error(_) -> False
  }
}

/// Removes all cached values from the table. Deletes every
/// row regardless of expiration status, effectively resetting
/// the cache.
///
pub fn flush(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let sql = "DELETE FROM " <> table

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case pog.query(sql) |> pog.execute(conn) {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError("Failed to flush cache: " <> error_to_string(e)))
    }
  })
}

/// Retrieves a JSON value from the cache and decodes it.
/// Returns SerializationError if the cached value cannot
/// be parsed as valid JSON matching the decoder.
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

/// Stores a value as JSON in the cache with a TTL. Encodes
/// the value using the provided encoder function before
/// storing.
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

/// Stores a value as JSON in the cache permanently. Encodes
/// the value using the provided encoder function before
/// storing with no expiration.
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

/// Retrieves a value and removes it from the cache in one
/// operation. Useful for one-time tokens or consuming queued
/// values.
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

/// Increments a numeric value in the cache. If the key
/// doesn't exist, starts from 0. Preserves the original
/// expiration timestamp.
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

/// Decrements a numeric value in the cache. If the key
/// doesn't exist, starts from 0. Delegates to increment
/// with a negated value.
///
pub fn decrement(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  increment(pool, key, -by)
}

/// Gets a value from cache, or computes and stores it if not
/// found. The compute function returns a Result to handle
/// computation errors gracefully.
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

/// Gets a value from cache, or computes and stores it
/// permanently. Like remember but with no TTL for values
/// that should never expire.
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

/// Gets a JSON value from cache, or computes, encodes, and
/// stores it. Combines remember semantics with JSON encoding
/// and decoding.
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

/// Removes expired entries from the cache table. Can be
/// called periodically to clean up stale data and reclaim
/// storage space.
///
pub fn cleanup_expired(pool: Pool) -> Result(Nil, CacheError) {
  let table = pool.get_table(pool)
  let now = unix_timestamp.now()
  let sql =
    "DELETE FROM " <> table <> " WHERE expiration > 0 AND expiration <= $1"

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.int(now))
      |> pog.execute(conn)
    {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError(
          "Failed to cleanup expired entries: " <> error_to_string(e),
        ))
    }
  })
}

// ------------------------------------------------------------- Private Functions

/// Gets the expiration timestamp for a key. Used internally
/// by increment to preserve the original expiration when
/// updating values.
///
fn get_expiration(pool: Pool, key: String) -> Result(Int, CacheError) {
  let table = pool.get_table(pool)
  let sql = "SELECT expiration FROM " <> table <> " WHERE key = $1"

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    {
      Ok(pog.Returned(1, [exp])) -> Ok(exp)
      Ok(_) -> Error(NotFound)
      Error(e) ->
        Error(ConnectionError(
          "Failed to get expiration: " <> error_to_string(e),
        ))
    }
  })
}

/// Stores a value with a specific expiration timestamp.
/// Used internally by increment to preserve the original
/// expiration when updating values.
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

  db_pool.get_connection(pool.get_db_pool(pool), fn(conn) {
    case
      pog.query(sql)
      |> pog.parameter(pog.text(key))
      |> pog.parameter(pog.text(value))
      |> pog.parameter(pog.int(expiration))
      |> pog.execute(conn)
    {
      Ok(_) -> Ok(Nil)
      Error(e) ->
        Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
    }
  })
}

/// Converts a pog error to a string. Extracts the error
/// message from the PostgreSQL error structure for
/// inclusion in CacheError messages.
///
fn error_to_string(e: pog.QueryError) -> String {
  case e {
    pog.ConstraintViolated(msg, _, _) -> msg
    pog.PostgresqlError(_, _, msg) -> msg
    pog.UnexpectedArgumentCount(expected, got) ->
      "Expected "
      <> int.to_string(expected)
      <> " arguments, got "
      <> int.to_string(got)
    pog.UnexpectedArgumentType(_, _) -> "Unexpected argument type"
    pog.UnexpectedResultType(_) -> "Unexpected result type"
    pog.ConnectionUnavailable -> "Connection unavailable"
    pog.QueryTimeout -> "Query timed out"
  }
}
