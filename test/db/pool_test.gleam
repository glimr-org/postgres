import gleam/dynamic/decode
import gleam/option
import gleeunit/should
import glimr/db/pool_connection
import glimr_postgres/db/pool
import pog
import test_helper

pub fn start_pool_with_url_config_test() {
  let config = test_helper.test_config()
  let result = pool.start_pool(config)

  result |> should.be_ok

  let assert Ok(p) = result
  pool.stop_pool(p)
}

pub fn start_pool_with_params_config_test() {
  let config =
    pool_connection.PostgresParamsConfig(
      host: "localhost",
      port: 5433,
      database: "glimr_test",
      username: "test",
      password: option.Some("test"),
      pool_size: 2,
    )
  let result = pool.start_pool(config)

  result |> should.be_ok

  let assert Ok(p) = result
  pool.stop_pool(p)
}

pub fn start_pool_with_invalid_url_test() {
  let config = pool_connection.PostgresConfig("invalid-url", 2)
  let result = pool.start_pool(config)

  result |> should.be_error
}

pub fn stop_pool_test() {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)

  // Should not panic
  pool.stop_pool(p)
}

pub fn get_connection_executes_query_test() {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)

  let result =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 1 + 1 as result")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })

  result |> should.be_ok
  let assert Ok(pog.Returned(_, [value])) = result
  value |> should.equal(2)

  pool.stop_pool(p)
}

pub fn get_connection_multiple_times_test() {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)

  // Get connection multiple times
  let r1 =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 1")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })
  let r2 =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 2")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })
  let r3 =
    pool.get_connection(p, fn(conn) {
      pog.query("SELECT 3")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(conn)
    })

  r1 |> should.be_ok
  r2 |> should.be_ok
  r3 |> should.be_ok

  let assert Ok(pog.Returned(_, [v1])) = r1
  let assert Ok(pog.Returned(_, [v2])) = r2
  let assert Ok(pog.Returned(_, [v3])) = r3

  v1 |> should.equal(1)
  v2 |> should.equal(2)
  v3 |> should.equal(3)

  pool.stop_pool(p)
}

pub fn start_pool_with_sqlite_config_fails_test() {
  let config = pool_connection.SqliteConfig("test.db", 2)
  let result = pool.start_pool(config)

  result |> should.be_error
}
