import gleam/dynamic/decode
import gleam/json
import gleeunit/should
import glimr/cache/cache
import glimr/cache/database as cache_database
import glimr/db/pool_connection
import glimr_postgres/postgres
import test_helper

fn with_clean_cache(f: fn(cache.CachePool, pool_connection.Pool) -> a) -> a {
  let db = postgres.start_from_config(test_helper.test_config())

  use conn <- pool_connection.get_connection(db)
  let _ =
    pool_connection.exec_with(
      conn,
      "CREATE TABLE IF NOT EXISTS cache_test (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        expiration BIGINT NOT NULL
      )",
      [],
    )
  let _ = pool_connection.exec_with(conn, "TRUNCATE cache_test", [])

  let pool = cache_database.start_with_table(db, "cache_test")
  let result = f(pool, db)
  pool_connection.stop_pool(db)
  result
}

// ------------------------------------------------------------ Basic Operations

pub fn create_table_test() {
  let db = postgres.start_from_config(test_helper.test_config())
  cache_database.create_table(db, "cache_test") |> should.be_ok
  pool_connection.stop_pool(db)
}

pub fn put_and_get_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "test_key", "test_value", 3600) |> should.be_ok
    cache.get(pool, "test_key") |> should.be_ok |> should.equal("test_value")
  })
}

pub fn get_nonexistent_key_returns_not_found_test() {
  with_clean_cache(fn(pool, _db) {
    case cache.get(pool, "nonexistent") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error"
    }
  })
}

pub fn put_forever_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put_forever(pool, "permanent_key", "permanent_value")
    |> should.be_ok
    cache.get(pool, "permanent_key")
    |> should.be_ok
    |> should.equal("permanent_value")
  })
}

pub fn put_overwrites_existing_value_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "overwrite_key", "original", 3600) |> should.be_ok
    cache.put(pool, "overwrite_key", "updated", 3600) |> should.be_ok
    cache.get(pool, "overwrite_key")
    |> should.be_ok
    |> should.equal("updated")
  })
}

pub fn forget_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "forget_key", "value", 3600) |> should.be_ok
    cache.forget(pool, "forget_key") |> should.be_ok

    case cache.get(pool, "forget_key") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error after forget"
    }
  })
}

pub fn forget_nonexistent_key_test() {
  with_clean_cache(fn(pool, _db) {
    cache.forget(pool, "nonexistent") |> should.be_ok
  })
}

pub fn has_existing_key_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "has_key", "value", 3600) |> should.be_ok
    cache.has(pool, "has_key") |> should.equal(True)
  })
}

pub fn has_nonexistent_key_test() {
  with_clean_cache(fn(pool, _db) {
    cache.has(pool, "nonexistent") |> should.equal(False)
  })
}

pub fn flush_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "flush1", "v1", 3600) |> should.be_ok
    cache.put(pool, "flush2", "v2", 3600) |> should.be_ok
    cache.put(pool, "flush3", "v3", 3600) |> should.be_ok

    cache.flush(pool) |> should.be_ok

    cache.has(pool, "flush1") |> should.equal(False)
    cache.has(pool, "flush2") |> should.equal(False)
    cache.has(pool, "flush3") |> should.equal(False)
  })
}

// ------------------------------------------------------------ JSON Operations

pub fn put_json_and_get_json_test() {
  with_clean_cache(fn(pool, _db) {
    let data = #("hello", 42)
    let encoder = fn(d: #(String, Int)) {
      json.object([
        #("message", json.string(d.0)),
        #("count", json.int(d.1)),
      ])
    }
    let decoder = {
      use message <- decode.field("message", decode.string)
      use count <- decode.field("count", decode.int)
      decode.success(#(message, count))
    }

    cache.put_json(pool, "json_key", data, encoder, 3600) |> should.be_ok
    cache.get_json(pool, "json_key", decoder)
    |> should.be_ok
    |> should.equal(#("hello", 42))
  })
}

pub fn put_json_forever_test() {
  with_clean_cache(fn(pool, _db) {
    let data = #("permanent", 99)
    let encoder = fn(d: #(String, Int)) {
      json.object([
        #("message", json.string(d.0)),
        #("count", json.int(d.1)),
      ])
    }
    let decoder = {
      use message <- decode.field("message", decode.string)
      use count <- decode.field("count", decode.int)
      decode.success(#(message, count))
    }

    cache.put_json_forever(pool, "json_forever", data, encoder)
    |> should.be_ok
    cache.get_json(pool, "json_forever", decoder)
    |> should.be_ok
    |> should.equal(#("permanent", 99))
  })
}

pub fn get_json_with_invalid_json_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "invalid_json", "not json", 3600) |> should.be_ok

    let decoder = decode.string
    case cache.get_json(pool, "invalid_json", decoder) {
      Error(cache.SerializationError(_)) -> Nil
      _ -> panic as "Expected SerializationError"
    }
  })
}

// ------------------------------------------------------------ Pull Operation

pub fn pull_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "pull_key", "pull_value", 3600) |> should.be_ok

    cache.pull(pool, "pull_key")
    |> should.be_ok
    |> should.equal("pull_value")

    cache.has(pool, "pull_key") |> should.equal(False)
  })
}

pub fn pull_nonexistent_key_test() {
  with_clean_cache(fn(pool, _db) {
    case cache.pull(pool, "nonexistent") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error"
    }
  })
}

// ------------------------------------------------------------ Increment/Decrement

pub fn increment_existing_value_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "counter", "10", 3600) |> should.be_ok
    cache.increment(pool, "counter", 5) |> should.be_ok |> should.equal(15)
    cache.get(pool, "counter") |> should.be_ok |> should.equal("15")
  })
}

pub fn increment_nonexistent_key_test() {
  with_clean_cache(fn(pool, _db) {
    cache.increment(pool, "new_counter", 5)
    |> should.be_ok
    |> should.equal(5)
    cache.get(pool, "new_counter") |> should.be_ok |> should.equal("5")
  })
}

pub fn increment_non_numeric_value_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "not_number", "hello", 3600) |> should.be_ok

    case cache.increment(pool, "not_number", 1) {
      Error(cache.SerializationError(_)) -> Nil
      _ -> panic as "Expected SerializationError"
    }
  })
}

pub fn decrement_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "dec_counter", "10", 3600) |> should.be_ok
    cache.decrement(pool, "dec_counter", 3)
    |> should.be_ok
    |> should.equal(7)
  })
}

// ------------------------------------------------------------ Remember Operations

pub fn remember_with_cache_hit_test() {
  with_clean_cache(fn(pool, _db) {
    cache.put(pool, "remember_key", "cached_value", 3600) |> should.be_ok

    cache.remember(pool, "remember_key", 3600, fn() { "computed_value" })
    |> should.equal("cached_value")
  })
}

pub fn remember_with_cache_miss_test() {
  with_clean_cache(fn(pool, _db) {
    cache.remember(pool, "new_key", 3600, fn() { "computed_value" })
    |> should.equal("computed_value")

    cache.get(pool, "new_key")
    |> should.be_ok
    |> should.equal("computed_value")
  })
}

pub fn remember_forever_test() {
  with_clean_cache(fn(pool, _db) {
    cache.remember_forever(pool, "forever_key", fn() { "forever_value" })
    |> should.equal("forever_value")

    cache.get(pool, "forever_key")
    |> should.be_ok
    |> should.equal("forever_value")
  })
}

pub fn remember_json_test() {
  with_clean_cache(fn(pool, _db) {
    let encoder = fn(n: Int) { json.int(n) }
    let decoder = decode.int

    cache.remember_json(pool, "json_remember", 3600, decoder, encoder, fn() {
      42
    })
    |> should.equal(42)

    cache.get_json(pool, "json_remember", decoder)
    |> should.be_ok
    |> should.equal(42)
  })
}

// ------------------------------------------------------------ Cleanup Operations

pub fn cleanup_expired_test() {
  with_clean_cache(fn(pool, db) {
    cache.put(pool, "expired_key", "value", -1) |> should.be_ok

    cache_database.cleanup_expired(db, "cache_test") |> should.be_ok

    cache.has(pool, "expired_key") |> should.equal(False)
  })
}

pub fn cleanup_expired_keeps_valid_entries_test() {
  with_clean_cache(fn(pool, db) {
    cache.put(pool, "expired", "value", -1) |> should.be_ok
    cache.put(pool, "valid", "value", 3600) |> should.be_ok
    cache.put_forever(pool, "permanent", "value") |> should.be_ok

    cache_database.cleanup_expired(db, "cache_test") |> should.be_ok

    cache.has(pool, "expired") |> should.equal(False)
    cache.has(pool, "valid") |> should.equal(True)
    cache.has(pool, "permanent") |> should.equal(True)
  })
}
