import gleam/dynamic/decode
import gleeunit/should
import glimr/cache/cache
import glimr/cache/database as cache_database
import glimr/config/database
import glimr/db/pool_connection
import glimr_postgres/postgres
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
    pool_connection.get_connection(p, fn(conn) {
      let decoder = decode.at([0], decode.int)
      pool_connection.query_with(conn, "SELECT 1 + 1 as result", [], decoder)
    })

  result |> should.be_ok
  let assert Ok(pool_connection.QueryResult(_, rows)) = result
  rows |> should.equal([2])

  pool_connection.stop_pool(p)
  cleanup_config()
}

pub fn start_with_multiple_connections_test() {
  setup_config(multi_connection_toml())

  // Start the secondary connection
  let p = postgres.start("secondary")

  // Verify it works
  let result =
    pool_connection.get_connection(p, fn(conn) {
      let decoder = decode.at([0], decode.int)
      pool_connection.query_with(conn, "SELECT 42", [], decoder)
    })

  result |> should.be_ok
  let assert Ok(pool_connection.QueryResult(_, rows)) = result
  rows |> should.equal([42])

  pool_connection.stop_pool(p)
  cleanup_config()
}

pub fn start_creates_usable_pool_test() {
  setup_config(test_connection_toml())

  let p = postgres.start("test")

  // Use a single connection to create temp table, insert, and query
  let result =
    pool_connection.get_connection(p, fn(conn) {
      // Create temp table
      let assert Ok(_) =
        pool_connection.exec_with(
          conn,
          "CREATE TEMP TABLE test_items (id SERIAL PRIMARY KEY, name TEXT)",
          [],
        )

      // Insert data
      let assert Ok(_) =
        pool_connection.exec_with(
          conn,
          "INSERT INTO test_items (name) VALUES ('item1')",
          [],
        )

      // Query the data back
      let decoder = decode.at([0], decode.string)
      pool_connection.query_with(
        conn,
        "SELECT name FROM test_items WHERE id = 1",
        [],
        decoder,
      )
    })

  result |> should.be_ok
  let assert Ok(pool_connection.QueryResult(_, rows)) = result
  rows |> should.equal(["item1"])

  pool_connection.stop_pool(p)
  cleanup_config()
}

// ------------------------------------------------------------- start_cache

pub fn start_cache_with_valid_store_test() {
  setup_config(main_connection_toml())

  let db = postgres.start("main")

  // Create the cache table
  use conn <- pool_connection.get_connection(db)
  let _ =
    pool_connection.exec_with(
      conn,
      "CREATE TABLE IF NOT EXISTS start_cache_test (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        expiration BIGINT NOT NULL
      )",
      [],
    )
  let _ = pool_connection.exec_with(conn, "TRUNCATE start_cache_test", [])

  // Start the cache pool
  let pool = cache_database.start_with_table(db, "start_cache_test")

  // Verify it works by doing cache operations
  cache.put(pool, "test_key", "test_value", 3600) |> should.be_ok
  cache.get(pool, "test_key") |> should.be_ok |> should.equal("test_value")
  cache.forget(pool, "test_key") |> should.be_ok

  pool_connection.stop_pool(db)
  cleanup_config()
}

pub fn start_cache_with_multiple_stores_test() {
  setup_config(main_connection_toml())

  let db = postgres.start("main")

  // Create both cache tables
  use conn <- pool_connection.get_connection(db)
  let _ =
    pool_connection.exec_with(
      conn,
      "CREATE TABLE IF NOT EXISTS cache_primary_test (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        expiration BIGINT NOT NULL
      )",
      [],
    )
  let _ =
    pool_connection.exec_with(
      conn,
      "CREATE TABLE IF NOT EXISTS cache_secondary_test (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        expiration BIGINT NOT NULL
      )",
      [],
    )
  let _ = pool_connection.exec_with(conn, "TRUNCATE cache_primary_test", [])
  let _ = pool_connection.exec_with(conn, "TRUNCATE cache_secondary_test", [])

  // Start the secondary cache pool
  let pool = cache_database.start_with_table(db, "cache_secondary_test")

  // Verify it works
  cache.put(pool, "secondary_key", "secondary_value", 3600) |> should.be_ok
  cache.get(pool, "secondary_key")
  |> should.be_ok
  |> should.equal("secondary_value")

  pool_connection.stop_pool(db)
  cleanup_config()
}
