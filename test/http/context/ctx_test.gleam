import gleam/dynamic/decode
import gleeunit/should
import glimr/db/driver
import glimr_postgres/db/pool
import glimr_postgres/http/context/ctx
import pog
import test_helper

pub fn load_single_default_connection_test() {
  let connections = [
    driver.PostgresUriConnection(
      name: "main",
      is_default: True,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
  ]

  let context = ctx.load(connections)

  // Should be able to use the default pool
  pool.get_connection(context.pool, fn(conn) {
    let assert Ok(pog.Returned(_, [result])) =
      pog.query("SELECT 1 + 1")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    result |> should.equal(2)
  })

  // Should also work with pool_for
  pool.get_connection(context.pool_for("main"), fn(conn) {
    let assert Ok(pog.Returned(_, [result])) =
      pog.query("SELECT 2 + 2")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    result |> should.equal(4)
  })

  // Clean up
  pool.stop_pool(context.pool)
}

pub fn load_multiple_connections_test() {
  let connections = [
    driver.PostgresUriConnection(
      name: "primary",
      is_default: True,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
    driver.PostgresUriConnection(
      name: "secondary",
      is_default: False,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
  ]

  let context = ctx.load(connections)

  // Default pool should be primary
  let _ =
    pool.get_connection(context.pool, fn(conn) {
      let assert Ok(_) =
        pog.query("DROP TABLE IF EXISTS ctx_primary_marker")
        |> pog.execute(conn)
      let assert Ok(_) =
        pog.query("CREATE TABLE ctx_primary_marker (id INTEGER)")
        |> pog.execute(conn)
    })

  // Verify primary has the table
  pool.get_connection(context.pool_for("primary"), fn(conn) {
    let decoder = decode.at([0], decode.string)
    let assert Ok(pog.Returned(_, tables)) =
      pog.query(
        "SELECT tablename FROM pg_tables WHERE tablename = 'ctx_primary_marker'",
      )
      |> pog.returning(decoder)
      |> pog.execute(conn)
    tables |> should.equal(["ctx_primary_marker"])
  })

  // Secondary should also see the table (same database)
  pool.get_connection(context.pool_for("secondary"), fn(conn) {
    let decoder = decode.at([0], decode.string)
    let assert Ok(pog.Returned(_, tables)) =
      pog.query(
        "SELECT tablename FROM pg_tables WHERE tablename = 'ctx_primary_marker'",
      )
      |> pog.returning(decoder)
      |> pog.execute(conn)
    tables |> should.equal(["ctx_primary_marker"])
  })

  // Clean up
  let _ =
    pool.get_connection(context.pool, fn(conn) {
      let assert Ok(_) =
        pog.query("DROP TABLE IF EXISTS ctx_primary_marker")
        |> pog.execute(conn)
    })

  pool.stop_pool(context.pool)
  pool.stop_pool(context.pool_for("secondary"))
}

pub fn load_filters_non_postgres_connections_test() {
  let connections = [
    driver.PostgresUriConnection(
      name: "postgres_db",
      is_default: True,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
    // This sqlite connection should be ignored
    driver.SqliteConnection(
      name: "sqlite_db",
      is_default: False,
      database: Ok("test.db"),
      pool_size: Ok(2),
    ),
  ]

  let context = ctx.load(connections)

  // Should work - only postgres connection is loaded
  pool.get_connection(context.pool, fn(conn) {
    let assert Ok(pog.Returned(_, [result])) =
      pog.query("SELECT 42")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    result |> should.equal(42)
  })

  pool.stop_pool(context.pool)
}

pub fn pool_for_returns_correct_pool_test() {
  let connections = [
    driver.PostgresUriConnection(
      name: "db_a",
      is_default: True,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
    driver.PostgresUriConnection(
      name: "db_b",
      is_default: False,
      url: Ok(test_helper.test_url),
      pool_size: Ok(2),
    ),
  ]

  let context = ctx.load(connections)

  // Both pools should be accessible and functional
  pool.get_connection(context.pool_for("db_a"), fn(conn) {
    let assert Ok(pog.Returned(_, [result])) =
      pog.query("SELECT 'a'")
      |> pog.returning(decode.at([0], decode.string))
      |> pog.execute(conn)
    result |> should.equal("a")
  })

  pool.get_connection(context.pool_for("db_b"), fn(conn) {
    let assert Ok(pog.Returned(_, [result])) =
      pog.query("SELECT 'b'")
      |> pog.returning(decode.at([0], decode.string))
      |> pog.execute(conn)
    result |> should.equal("b")
  })

  pool.stop_pool(context.pool_for("db_a"))
  pool.stop_pool(context.pool_for("db_b"))
}
// Note: Testing panic cases (no connections, no default, multiple defaults)
// would require a testing framework that can catch panics.
// These scenarios are documented but not directly testable with gleeunit.
