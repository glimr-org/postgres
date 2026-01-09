import gleam/dynamic/decode
import gleeunit/should
import glimr/db/driver.{PostgresUriConnection}
import glimr_postgres/db/pool
import glimr_postgres/postgres
import pog

const test_url = "postgresql://test:test@localhost:5433/glimr_test"

// ------------------------------------------------------------- start

pub fn start_with_valid_connection_test() {
  let connections = [
    PostgresUriConnection(name: "main", url: Ok(test_url), pool_size: Ok(2)),
  ]

  let p = postgres.start("main", connections)

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
}

pub fn start_with_multiple_connections_test() {
  let connections = [
    PostgresUriConnection(name: "primary", url: Ok(test_url), pool_size: Ok(2)),
    PostgresUriConnection(
      name: "secondary",
      url: Ok(test_url),
      pool_size: Ok(1),
    ),
  ]

  // Start the secondary connection
  let p = postgres.start("secondary", connections)

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
}

pub fn start_creates_usable_pool_test() {
  let connections = [
    PostgresUriConnection(name: "test", url: Ok(test_url), pool_size: Ok(3)),
  ]

  let p = postgres.start("test", connections)

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
}
