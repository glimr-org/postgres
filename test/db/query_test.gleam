import gleam/dynamic/decode
import gleeunit/should
import glimr/db/db.{QueryError}
import glimr_postgres/postgres
import pog
import test_helper

fn with_pool(f: fn(postgres.Pool) -> a) -> a {
  let config = test_helper.test_config()
  let assert Ok(p) = postgres.start_pool(config)

  let result = f(p)

  postgres.stop_pool(p)
  result
}

fn with_clean_table(
  p: postgres.Pool,
  table_name: String,
  ddl: String,
  f: fn() -> a,
) -> a {
  // Drop and recreate table
  let _ =
    postgres.get_connection(p, fn(conn) {
      let _ = postgres.exec(conn, "DROP TABLE IF EXISTS " <> table_name)
      let assert Ok(_) = postgres.exec(conn, ddl)
    })

  let result = f()

  // Clean up
  let _ =
    postgres.get_connection(p, fn(conn) {
      let _ = postgres.exec(conn, "DROP TABLE IF EXISTS " <> table_name)
    })

  result
}

pub fn query_select_with_decoder_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "users",
      "CREATE TABLE users (id INTEGER, name TEXT)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          // Insert data
          let assert Ok(_) =
            postgres.query(
              conn,
              "INSERT INTO users (id, name) VALUES ($1, $2)",
              [pog.int(1), pog.text("Alice")],
              decode.dynamic,
            )

          // Query with decoder
          let decoder = {
            use id <- decode.field(0, decode.int)
            use name <- decode.field(1, decode.string)
            decode.success(#(id, name))
          }

          let result =
            postgres.query(conn, "SELECT id, name FROM users", [], decoder)

          result |> should.be_ok
          let assert Ok(rows) = result
          rows |> should.equal([#(1, "Alice")])
        })
      },
    )
  })
}

pub fn query_with_parameters_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "items",
      "CREATE TABLE items (id INTEGER, value TEXT)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let assert Ok(_) =
            postgres.query(
              conn,
              "INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three')",
              [],
              decode.dynamic,
            )

          let decoder = decode.at([1], decode.string)
          let result =
            postgres.query(
              conn,
              "SELECT * FROM items WHERE id > $1 ORDER BY id",
              [pog.int(1)],
              decoder,
            )

          result |> should.be_ok
          let assert Ok(rows) = result
          rows |> should.equal(["two", "three"])
        })
      },
    )
  })
}

pub fn query_empty_result_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "empty_table",
      "CREATE TABLE empty_table (id INTEGER)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let decoder = decode.at([0], decode.int)
          let result =
            postgres.query(conn, "SELECT * FROM empty_table", [], decoder)

          result |> should.be_ok
          let assert Ok(rows) = result
          rows |> should.equal([])
        })
      },
    )
  })
}

pub fn query_invalid_sql_test() {
  with_pool(fn(p) {
    postgres.get_connection(p, fn(conn) {
      let decoder = decode.at([0], decode.int)
      let result =
        postgres.query(conn, "SELECT * FROM nonexistent_table_xyz", [], decoder)

      result |> should.be_error
      let assert Error(QueryError(msg)) = result
      msg |> should.not_equal("")
    })
  })
}

pub fn exec_create_table_test() {
  with_pool(fn(p) {
    postgres.get_connection(p, fn(conn) {
      // Clean up first
      let _ = postgres.exec(conn, "DROP TABLE IF EXISTS test_table")

      let result =
        postgres.exec(conn, "CREATE TABLE test_table (id INTEGER PRIMARY KEY)")

      result |> should.be_ok

      // Clean up
      let _ = postgres.exec(conn, "DROP TABLE IF EXISTS test_table")
    })
  })
}

pub fn exec_insert_test() {
  with_pool(fn(p) {
    with_clean_table(p, "numbers", "CREATE TABLE numbers (n INTEGER)", fn() {
      postgres.get_connection(p, fn(conn) {
        let result = postgres.exec(conn, "INSERT INTO numbers VALUES (42)")

        result |> should.be_ok

        // Verify insertion
        let decoder = decode.at([0], decode.int)
        let assert Ok([n]) =
          postgres.query(conn, "SELECT n FROM numbers", [], decoder)
        n |> should.equal(42)
      })
    })
  })
}

pub fn exec_update_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "data",
      "CREATE TABLE data (id INTEGER, val INTEGER)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let assert Ok(_) =
            postgres.exec(conn, "INSERT INTO data VALUES (1, 10)")

          let result =
            postgres.exec(conn, "UPDATE data SET val = 20 WHERE id = 1")
          result |> should.be_ok

          let decoder = decode.at([1], decode.int)
          let assert Ok([val]) =
            postgres.query(conn, "SELECT * FROM data", [], decoder)
          val |> should.equal(20)
        })
      },
    )
  })
}

pub fn exec_delete_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "to_delete",
      "CREATE TABLE to_delete (id INTEGER)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let assert Ok(_) =
            postgres.exec(conn, "INSERT INTO to_delete VALUES (1), (2), (3)")

          let result = postgres.exec(conn, "DELETE FROM to_delete WHERE id = 2")
          result |> should.be_ok

          let decoder = decode.at([0], decode.int)
          let assert Ok(rows) =
            postgres.query(
              conn,
              "SELECT * FROM to_delete ORDER BY id",
              [],
              decoder,
            )
          rows |> should.equal([1, 3])
        })
      },
    )
  })
}

pub fn exec_invalid_sql_test() {
  with_pool(fn(p) {
    postgres.get_connection(p, fn(conn) {
      let result = postgres.exec(conn, "INVALID SQL STATEMENT")

      result |> should.be_error
    })
  })
}

pub fn exec_constraint_violation_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "unique_test",
      "CREATE TABLE unique_test (id INTEGER PRIMARY KEY)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let assert Ok(_) =
            postgres.exec(conn, "INSERT INTO unique_test VALUES (1)")

          // Try to insert duplicate
          let result = postgres.exec(conn, "INSERT INTO unique_test VALUES (1)")

          result |> should.be_error
        })
      },
    )
  })
}

pub fn query_returns_row_count_test() {
  with_pool(fn(p) {
    with_clean_table(
      p,
      "count_test",
      "CREATE TABLE count_test (id INTEGER)",
      fn() {
        postgres.get_connection(p, fn(conn) {
          let assert Ok(_) =
            postgres.exec(conn, "INSERT INTO count_test VALUES (1), (2), (3)")

          // exec should return affected row count
          let result = postgres.exec(conn, "DELETE FROM count_test")
          result |> should.be_ok
          let assert Ok(count) = result
          count |> should.equal(3)
        })
      },
    )
  })
}
