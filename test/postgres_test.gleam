import gleam/dynamic/decode
import gleeunit/should
import glimr/cache/driver.{DatabaseStore} as _cache_driver
import glimr/config/database
import glimr_postgres/cache/cache as pg_cache
import glimr_postgres/db/pool
import glimr_postgres/postgres
import pog
import simplifile

const test_url = "postgresql://test:test@localhost:5433/glimr_test"

const config_dir = "config"

const config_file = "config/database.toml"

// ------------------------------------------------------------- Helpers

fn setup_config(toml_content: String) -> Nil {
  database.clear_cache()
  let _ = simplifile.create_directory_all(config_dir)
  let _ = simplifile.write(config_file, toml_content)
  Nil
}

fn cleanup_config() -> Nil {
  let _ = simplifile.delete(config_file)
  Nil
}

fn main_connection_toml() -> String {
  "[connections.main]
  driver = \"postgres_url\"
  url = \"" <> test_url <> "\"
  pool_size = 2
"
}

fn multi_connection_toml() -> String {
  "[connections.primary]
  driver = \"postgres_url\"
  url = \"" <> test_url <> "\"
  pool_size = 2

[connections.secondary]
  driver = \"postgres_url\"
  url = \"" <> test_url <> "\"
  pool_size = 1
"
}

fn test_connection_toml() -> String {
  "[connections.test]
  driver = \"postgres_url\"
  url = \"" <> test_url <> "\"
  pool_size = 3
"
}

// ------------------------------------------------------------- start

pub fn start_with_valid_connection_test() {
  setup_config(main_connection_toml())

  let p = postgres.start("main")

  // Verify the pool works by executing a query
  let result =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 1 + 1 as result")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })

  result |> should.be_ok
  let assert Ok(response) = result
  response.rows |> should.equal([2])

  pool.stop_pool(p)
  cleanup_config()
}

pub fn start_with_multiple_connections_test() {
  setup_config(multi_connection_toml())

  // Start the secondary connection
  let p = postgres.start("secondary")

  // Verify it works
  let result =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 42")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })

  result |> should.be_ok
  let assert Ok(response) = result
  response.rows |> should.equal([42])

  pool.stop_pool(p)
  cleanup_config()
}

pub fn start_creates_usable_pool_test() {
  setup_config(test_connection_toml())

  let p = postgres.start("test")

  // Use a single connection to create temp table, insert, and query
  let result =
    pool.get_connection(p, fn(conn) {
      // Create temp table
      let assert Ok(_) =
        pog.query(
          "CREATE TEMP TABLE test_items (id SERIAL PRIMARY KEY, name TEXT)",
        )
        |> pog.execute(conn)

      // Insert data
      let assert Ok(_) =
        pog.query("INSERT INTO test_items (name) VALUES ('item1')")
        |> pog.execute(conn)

      // Query the data back
      pog.query("SELECT name FROM test_items WHERE id = 1")
      |> pog.returning(decode.at([0], decode.string))
      |> pog.execute(conn)
    })

  result |> should.be_ok
  let assert Ok(response) = result
  response.rows |> should.equal(["item1"])

  pool.stop_pool(p)
  cleanup_config()
}

// ------------------------------------------------------------- start_cache

pub fn start_cache_with_valid_store_test() {
  setup_config(main_connection_toml())

  let stores = [
    DatabaseStore(name: "cache", database: "main", table: "start_cache_test"),
  ]

  let db = postgres.start("main")

  // Create the cache table
  pool.get_connection(db, fn(conn) {
    let _ =
      pog.query(
        "CREATE TABLE IF NOT EXISTS start_cache_test (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          expiration BIGINT NOT NULL
        )",
      )
      |> pog.execute(conn)
    let _ = pog.query("TRUNCATE start_cache_test") |> pog.execute(conn)
    Nil
  })

  // Start the cache pool
  let cache = postgres.start_cache(db, "cache", stores)

  // Verify it works by doing cache operations
  pg_cache.put(cache, "test_key", "test_value", 3600) |> should.be_ok
  pg_cache.get(cache, "test_key") |> should.be_ok |> should.equal("test_value")
  pg_cache.forget(cache, "test_key") |> should.be_ok

  pool.stop_pool(db)
  cleanup_config()
}

pub fn start_cache_with_multiple_stores_test() {
  setup_config(main_connection_toml())

  let stores = [
    DatabaseStore(
      name: "primary",
      database: "main",
      table: "cache_primary_test",
    ),
    DatabaseStore(
      name: "secondary",
      database: "main",
      table: "cache_secondary_test",
    ),
  ]

  let db = postgres.start("main")

  // Create both cache tables
  pool.get_connection(db, fn(conn) {
    let _ =
      pog.query(
        "CREATE TABLE IF NOT EXISTS cache_primary_test (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          expiration BIGINT NOT NULL
        )",
      )
      |> pog.execute(conn)
    let _ =
      pog.query(
        "CREATE TABLE IF NOT EXISTS cache_secondary_test (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          expiration BIGINT NOT NULL
        )",
      )
      |> pog.execute(conn)
    let _ = pog.query("TRUNCATE cache_primary_test") |> pog.execute(conn)
    let _ = pog.query("TRUNCATE cache_secondary_test") |> pog.execute(conn)
    Nil
  })

  // Start the secondary cache pool
  let cache = postgres.start_cache(db, "secondary", stores)

  // Verify it works
  pg_cache.put(cache, "secondary_key", "secondary_value", 3600) |> should.be_ok
  pg_cache.get(cache, "secondary_key")
  |> should.be_ok
  |> should.equal("secondary_value")

  pool.stop_pool(db)
  cleanup_config()
}
