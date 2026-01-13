import gleam/dynamic/decode
import gleam/json
import gleeunit/should
import glimr/cache/cache
import glimr/cache/driver
import glimr_postgres/cache/cache as pg_cache
import glimr_postgres/cache/pool as cache_pool
import glimr_postgres/db/pool as db_pool
import pog
import test_helper

fn with_clean_cache(f: fn(cache_pool.Pool) -> a) -> a {
  let config = test_helper.test_config()
  let assert Ok(db) = db_pool.start_pool(config)

  let store = driver.DatabaseStore("test", "main", "cache_test")
  let pool = cache_pool.start_pool(db, store)

  db_pool.get_connection(db, fn(conn) {
    let _ =
      pog.query(
        "CREATE TABLE IF NOT EXISTS cache_test (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          expiration BIGINT NOT NULL
        )",
      )
      |> pog.execute(conn)
    let _ = pog.query("TRUNCATE cache_test") |> pog.execute(conn)
    Nil
  })

  let result = f(pool)
  db_pool.stop_pool(db)
  result
}

// ------------------------------------------------------------ Basic Operations

pub fn create_table_test() {
  with_clean_cache(fn(pool) { pg_cache.create_table(pool) |> should.be_ok })
}

pub fn put_and_get_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "test_key", "test_value", 3600) |> should.be_ok
    pg_cache.get(pool, "test_key") |> should.be_ok |> should.equal("test_value")
  })
}

pub fn get_nonexistent_key_returns_not_found_test() {
  with_clean_cache(fn(pool) {
    case pg_cache.get(pool, "nonexistent") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error"
    }
  })
}

pub fn put_forever_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put_forever(pool, "permanent_key", "permanent_value")
    |> should.be_ok
    pg_cache.get(pool, "permanent_key")
    |> should.be_ok
    |> should.equal("permanent_value")
  })
}

pub fn put_overwrites_existing_value_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "overwrite_key", "original", 3600) |> should.be_ok
    pg_cache.put(pool, "overwrite_key", "updated", 3600) |> should.be_ok
    pg_cache.get(pool, "overwrite_key")
    |> should.be_ok
    |> should.equal("updated")
  })
}

pub fn forget_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "forget_key", "value", 3600) |> should.be_ok
    pg_cache.forget(pool, "forget_key") |> should.be_ok

    case pg_cache.get(pool, "forget_key") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error after forget"
    }
  })
}

pub fn forget_nonexistent_key_test() {
  with_clean_cache(fn(pool) {
    pg_cache.forget(pool, "nonexistent") |> should.be_ok
  })
}

pub fn has_existing_key_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "has_key", "value", 3600) |> should.be_ok
    pg_cache.has(pool, "has_key") |> should.equal(True)
  })
}

pub fn has_nonexistent_key_test() {
  with_clean_cache(fn(pool) {
    pg_cache.has(pool, "nonexistent") |> should.equal(False)
  })
}

pub fn flush_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "flush1", "v1", 3600) |> should.be_ok
    pg_cache.put(pool, "flush2", "v2", 3600) |> should.be_ok
    pg_cache.put(pool, "flush3", "v3", 3600) |> should.be_ok

    pg_cache.flush(pool) |> should.be_ok

    pg_cache.has(pool, "flush1") |> should.equal(False)
    pg_cache.has(pool, "flush2") |> should.equal(False)
    pg_cache.has(pool, "flush3") |> should.equal(False)
  })
}

// ------------------------------------------------------------ JSON Operations

pub fn put_json_and_get_json_test() {
  with_clean_cache(fn(pool) {
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

    pg_cache.put_json(pool, "json_key", data, encoder, 3600) |> should.be_ok
    pg_cache.get_json(pool, "json_key", decoder)
    |> should.be_ok
    |> should.equal(#("hello", 42))
  })
}

pub fn put_json_forever_test() {
  with_clean_cache(fn(pool) {
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

    pg_cache.put_json_forever(pool, "json_forever", data, encoder)
    |> should.be_ok
    pg_cache.get_json(pool, "json_forever", decoder)
    |> should.be_ok
    |> should.equal(#("permanent", 99))
  })
}

pub fn get_json_with_invalid_json_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "invalid_json", "not json", 3600) |> should.be_ok

    let decoder = decode.string
    case pg_cache.get_json(pool, "invalid_json", decoder) {
      Error(cache.SerializationError(_)) -> Nil
      _ -> panic as "Expected SerializationError"
    }
  })
}

// ------------------------------------------------------------ Pull Operation

pub fn pull_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "pull_key", "pull_value", 3600) |> should.be_ok

    pg_cache.pull(pool, "pull_key")
    |> should.be_ok
    |> should.equal("pull_value")

    pg_cache.has(pool, "pull_key") |> should.equal(False)
  })
}

pub fn pull_nonexistent_key_test() {
  with_clean_cache(fn(pool) {
    case pg_cache.pull(pool, "nonexistent") {
      Error(cache.NotFound) -> Nil
      _ -> panic as "Expected NotFound error"
    }
  })
}

// ------------------------------------------------------------ Increment/Decrement

pub fn increment_existing_value_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "counter", "10", 3600) |> should.be_ok
    pg_cache.increment(pool, "counter", 5) |> should.be_ok |> should.equal(15)
    pg_cache.get(pool, "counter") |> should.be_ok |> should.equal("15")
  })
}

pub fn increment_nonexistent_key_test() {
  with_clean_cache(fn(pool) {
    pg_cache.increment(pool, "new_counter", 5)
    |> should.be_ok
    |> should.equal(5)
    pg_cache.get(pool, "new_counter") |> should.be_ok |> should.equal("5")
  })
}

pub fn increment_non_numeric_value_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "not_number", "hello", 3600) |> should.be_ok

    case pg_cache.increment(pool, "not_number", 1) {
      Error(cache.SerializationError(_)) -> Nil
      _ -> panic as "Expected SerializationError"
    }
  })
}

pub fn decrement_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "dec_counter", "10", 3600) |> should.be_ok
    pg_cache.decrement(pool, "dec_counter", 3)
    |> should.be_ok
    |> should.equal(7)
  })
}

// ------------------------------------------------------------ Remember Operations

pub fn remember_with_cache_hit_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "remember_key", "cached_value", 3600) |> should.be_ok

    let compute_called = fn() { Ok("computed_value") }

    pg_cache.remember(pool, "remember_key", 3600, compute_called)
    |> should.be_ok
    |> should.equal("cached_value")
  })
}

pub fn remember_with_cache_miss_test() {
  with_clean_cache(fn(pool) {
    let compute = fn() { Ok("computed_value") }

    pg_cache.remember(pool, "new_key", 3600, compute)
    |> should.be_ok
    |> should.equal("computed_value")

    pg_cache.get(pool, "new_key")
    |> should.be_ok
    |> should.equal("computed_value")
  })
}

pub fn remember_with_compute_failure_test() {
  with_clean_cache(fn(pool) {
    let compute = fn() { Error("compute failed") }

    case pg_cache.remember(pool, "fail_key", 3600, compute) {
      Error(cache.ComputeError(_)) -> Nil
      _ -> panic as "Expected ComputeError"
    }
  })
}

pub fn remember_forever_test() {
  with_clean_cache(fn(pool) {
    let compute = fn() { Ok("forever_value") }

    pg_cache.remember_forever(pool, "forever_key", compute)
    |> should.be_ok
    |> should.equal("forever_value")

    pg_cache.get(pool, "forever_key")
    |> should.be_ok
    |> should.equal("forever_value")
  })
}

pub fn remember_json_test() {
  with_clean_cache(fn(pool) {
    let encoder = fn(n: Int) { json.int(n) }
    let decoder = decode.int
    let compute = fn() { Ok(42) }

    pg_cache.remember_json(
      pool,
      "json_remember",
      3600,
      decoder,
      compute,
      encoder,
    )
    |> should.be_ok
    |> should.equal(42)

    pg_cache.get_json(pool, "json_remember", decoder)
    |> should.be_ok
    |> should.equal(42)
  })
}

// ------------------------------------------------------------ Cleanup Operations

pub fn cleanup_expired_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "expired_key", "value", -1) |> should.be_ok

    pg_cache.cleanup_expired(pool) |> should.be_ok

    pg_cache.has(pool, "expired_key") |> should.equal(False)
  })
}

pub fn cleanup_expired_keeps_valid_entries_test() {
  with_clean_cache(fn(pool) {
    pg_cache.put(pool, "expired", "value", -1) |> should.be_ok
    pg_cache.put(pool, "valid", "value", 3600) |> should.be_ok
    pg_cache.put_forever(pool, "permanent", "value") |> should.be_ok

    pg_cache.cleanup_expired(pool) |> should.be_ok

    pg_cache.has(pool, "expired") |> should.equal(False)
    pg_cache.has(pool, "valid") |> should.equal(True)
    pg_cache.has(pool, "permanent") |> should.equal(True)
  })
}
