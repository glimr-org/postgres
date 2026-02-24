import gleam/dynamic/decode
import gleeunit/should
import glimr/db/pool_connection.{ConnectionError, QueryError}
import glimr_postgres/postgres
import test_helper

fn with_pool(f: fn(pool_connection.Pool) -> a) -> a {
  let p = postgres.start_from_config(test_helper.test_config())

  // Create test table
  let _ =
    pool_connection.get_connection(p, fn(conn) {
      let _ =
        pool_connection.exec_with(conn, "DROP TABLE IF EXISTS accounts", [])
      let assert Ok(_) =
        pool_connection.exec_with(
          conn,
          "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
          [],
        )
    })

  let result = f(p)

  // Clean up
  let _ =
    pool_connection.get_connection(p, fn(conn) {
      let _ =
        pool_connection.exec_with(conn, "DROP TABLE IF EXISTS accounts", [])
    })

  pool_connection.stop_pool(p)
  result
}

pub fn transaction_commits_on_success_test() {
  with_pool(fn(p) {
    // Insert in transaction
    let result =
      pool_connection.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (1, 100)",
            [],
          )
        Ok(Nil)
      })

    result |> should.be_ok

    // Verify data persisted
    pool_connection.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok(pool_connection.QueryResult(_, [balance])) =
        pool_connection.query_with(
          conn,
          "SELECT * FROM accounts WHERE id = 1",
          [],
          decoder,
        )
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_returns_value_test() {
  with_pool(fn(p) {
    let result =
      pool_connection.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (1, 50)",
            [],
          )

        let decoder = decode.at([1], decode.int)
        let assert Ok(pool_connection.QueryResult(_, [balance])) =
          pool_connection.query_with(
            conn,
            "SELECT * FROM accounts WHERE id = 1",
            [],
            decoder,
          )

        Ok(balance * 2)
      })

    result |> should.be_ok
    let assert Ok(value) = result
    value |> should.equal(100)
  })
}

pub fn transaction_rolls_back_on_error_test() {
  with_pool(fn(p) {
    // First insert some data
    let _ =
      pool_connection.get_connection(p, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (1, 100)",
            [],
          )
      })

    // Transaction that fails
    let result =
      pool_connection.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "UPDATE accounts SET balance = 200 WHERE id = 1",
            [],
          )
        // Return error to trigger rollback
        Error(QueryError("Intentional failure"))
      })

    result |> should.be_error

    // Verify data was rolled back
    pool_connection.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok(pool_connection.QueryResult(_, [balance])) =
        pool_connection.query_with(
          conn,
          "SELECT * FROM accounts WHERE id = 1",
          [],
          decoder,
        )
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_rolls_back_on_query_error_test() {
  with_pool(fn(p) {
    let _ =
      pool_connection.get_connection(p, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (1, 100)",
            [],
          )
      })

    let result =
      pool_connection.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "UPDATE accounts SET balance = 500 WHERE id = 1",
            [],
          )
        // This will fail - table doesn't exist
        pool_connection.exec_with(
          conn,
          "INSERT INTO nonexistent_table_xyz VALUES (1)",
          [],
        )
      })

    result |> should.be_error

    // Verify rollback
    pool_connection.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok(pool_connection.QueryResult(_, [balance])) =
        pool_connection.query_with(
          conn,
          "SELECT * FROM accounts WHERE id = 1",
          [],
          decoder,
        )
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_negative_retries_returns_error_test() {
  with_pool(fn(p) {
    let result = pool_connection.transaction(p, -1, fn(_conn) { Ok(Nil) })

    result |> should.be_error
    let assert Error(ConnectionError(msg)) = result
    msg |> should.equal("Transaction retries cannot be negative")
  })
}

pub fn transaction_multiple_operations_test() {
  with_pool(fn(p) {
    let result =
      pool_connection.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (1, 100)",
            [],
          )
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "INSERT INTO accounts VALUES (2, 200)",
            [],
          )
        let assert Ok(_) =
          pool_connection.exec_with(
            conn,
            "UPDATE accounts SET balance = balance + 50 WHERE id = 1",
            [],
          )
        Ok(Nil)
      })

    result |> should.be_ok

    pool_connection.get_connection(p, fn(conn) {
      let decoder = {
        use id <- decode.field(0, decode.int)
        use balance <- decode.field(1, decode.int)
        decode.success(#(id, balance))
      }
      let assert Ok(pool_connection.QueryResult(_, rows)) =
        pool_connection.query_with(
          conn,
          "SELECT * FROM accounts ORDER BY id",
          [],
          decoder,
        )

      rows |> should.equal([#(1, 150), #(2, 200)])
    })
  })
}
