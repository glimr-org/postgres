import gleam/dynamic/decode
import gleeunit/should
import glimr/db/pool_connection.{ConnectionError, QueryError}
import glimr_postgres/db/db
import glimr_postgres/db/pool
import glimr_postgres/db/query
import test_helper

fn with_pool(f: fn(pool.Pool) -> a) -> a {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)

  // Create test table
  let _ =
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS accounts")
      let assert Ok(_) =
        query.exec(
          conn,
          "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
        )
    })

  let result = f(p)

  // Clean up
  let _ =
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS accounts")
    })

  pool.stop_pool(p)
  result
}

pub fn transaction_commits_on_success_test() {
  with_pool(fn(p) {
    // Insert in transaction
    let result =
      db.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (1, 100)")
        Ok(Nil)
      })

    result |> should.be_ok

    // Verify data persisted
    pool.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok([balance]) =
        query.query(conn, "SELECT * FROM accounts WHERE id = 1", [], decoder)
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_returns_value_test() {
  with_pool(fn(p) {
    let result =
      db.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (1, 50)")

        let decoder = decode.at([1], decode.int)
        let assert Ok([balance]) =
          query.query(conn, "SELECT * FROM accounts WHERE id = 1", [], decoder)

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
      pool.get_connection(p, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (1, 100)")
      })

    // Transaction that fails
    let result =
      db.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "UPDATE accounts SET balance = 200 WHERE id = 1")
        // Return error to trigger rollback
        Error(QueryError("Intentional failure"))
      })

    result |> should.be_error

    // Verify data was rolled back
    pool.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok([balance]) =
        query.query(conn, "SELECT * FROM accounts WHERE id = 1", [], decoder)
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_rolls_back_on_query_error_test() {
  with_pool(fn(p) {
    let _ =
      pool.get_connection(p, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (1, 100)")
      })

    let result =
      db.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "UPDATE accounts SET balance = 500 WHERE id = 1")
        // This will fail - table doesn't exist
        query.exec(conn, "INSERT INTO nonexistent_table_xyz VALUES (1)")
      })

    result |> should.be_error

    // Verify rollback
    pool.get_connection(p, fn(conn) {
      let decoder = decode.at([1], decode.int)
      let assert Ok([balance]) =
        query.query(conn, "SELECT * FROM accounts WHERE id = 1", [], decoder)
      balance |> should.equal(100)
    })
  })
}

pub fn transaction_negative_retries_returns_error_test() {
  with_pool(fn(p) {
    let result = db.transaction(p, -1, fn(_conn) { Ok(Nil) })

    result |> should.be_error
    let assert Error(ConnectionError(msg)) = result
    msg |> should.equal("Transaction retries cannot be negative")
  })
}

pub fn transaction_multiple_operations_test() {
  with_pool(fn(p) {
    let result =
      db.transaction(p, 0, fn(conn) {
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (1, 100)")
        let assert Ok(_) =
          query.exec(conn, "INSERT INTO accounts VALUES (2, 200)")
        let assert Ok(_) =
          query.exec(
            conn,
            "UPDATE accounts SET balance = balance + 50 WHERE id = 1",
          )
        Ok(Nil)
      })

    result |> should.be_ok

    pool.get_connection(p, fn(conn) {
      let decoder = {
        use id <- decode.field(0, decode.int)
        use balance <- decode.field(1, decode.int)
        decode.success(#(id, balance))
      }
      let assert Ok(rows) =
        query.query(conn, "SELECT * FROM accounts ORDER BY id", [], decoder)

      rows |> should.equal([#(1, 150), #(2, 200)])
    })
  })
}
