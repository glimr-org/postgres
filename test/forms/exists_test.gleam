import gleeunit/should
import glimr/db/db
import glimr/forms/validator
import glimr_postgres/postgres
import test_helper

fn with_pool(f: fn(db.DbPool) -> a) -> a {
  let pool = postgres.start_from_config(test_helper.test_config())

  let _ =
    db.get_connection(pool, fn(conn) {
      let _ = db.exec_with(conn, "DROP TABLE IF EXISTS exists_users", [])
      let assert Ok(_) =
        db.exec_with(
          conn,
          "CREATE TABLE exists_users (id SERIAL PRIMARY KEY, email TEXT, api_key TEXT)",
          [],
        )
      let assert Ok(_) =
        db.exec_with(
          conn,
          "INSERT INTO exists_users (email, api_key) VALUES ('admin@example.com', 'secret-KEY-123')",
          [],
        )
    })

  let result = f(pool)

  let _ =
    db.get_connection(pool, fn(conn) {
      let _ = db.exec_with(conn, "DROP TABLE IF EXISTS exists_users", [])
    })

  db.stop_pool(pool)
  result
}

fn form(field: String, value: String) -> validator.FormData {
  validator.FormData(
    get: fn(key) {
      case key == field {
        True -> value
        False -> ""
      }
    },
    get_file: fn(_) { panic as "not used" },
    get_file_result: fn(_) { panic as "not used" },
  )
}

const ctx = Nil

// ------------------------------------------------------------- Exists (case-insensitive)

pub fn exists_passes_when_exact_match_found_test() {
  with_pool(fn(pool) {
    let data = form("email", "admin@example.com")

    validator.start(
      [validator.for("email", [validator.Exists(pool, "exists_users")])],
      data,
      ctx,
    )
    |> should.be_ok
  })
}

pub fn exists_passes_when_different_case_found_test() {
  with_pool(fn(pool) {
    let data = form("email", "Admin@Example.COM")

    validator.start(
      [validator.for("email", [validator.Exists(pool, "exists_users")])],
      data,
      ctx,
    )
    |> should.be_ok
  })
}

pub fn exists_passes_when_uppercase_found_test() {
  with_pool(fn(pool) {
    let data = form("email", "ADMIN@EXAMPLE.COM")

    validator.start(
      [validator.for("email", [validator.Exists(pool, "exists_users")])],
      data,
      ctx,
    )
    |> should.be_ok
  })
}

pub fn exists_fails_when_value_not_in_db_test() {
  with_pool(fn(pool) {
    let data = form("email", "nobody@example.com")

    validator.start(
      [validator.for("email", [validator.Exists(pool, "exists_users")])],
      data,
      ctx,
    )
    |> should.be_error
  })
}

pub fn exists_passes_for_empty_value_test() {
  with_pool(fn(pool) {
    let data = form("email", "")

    validator.start(
      [validator.for("email", [validator.Exists(pool, "exists_users")])],
      data,
      ctx,
    )
    |> should.be_ok
  })
}

// ------------------------------------------------------------- ExistsSensitive (case-sensitive)

pub fn exists_sensitive_passes_when_exact_match_found_test() {
  with_pool(fn(pool) {
    let data = form("api_key", "secret-KEY-123")

    validator.start(
      [
        validator.for("api_key", [
          validator.ExistsSensitive(pool, "exists_users"),
        ]),
      ],
      data,
      ctx,
    )
    |> should.be_ok
  })
}

pub fn exists_sensitive_fails_when_different_case_test() {
  with_pool(fn(pool) {
    let data = form("api_key", "SECRET-key-123")

    validator.start(
      [
        validator.for("api_key", [
          validator.ExistsSensitive(pool, "exists_users"),
        ]),
      ],
      data,
      ctx,
    )
    |> should.be_error
  })
}

pub fn exists_sensitive_fails_when_value_not_in_db_test() {
  with_pool(fn(pool) {
    let data = form("api_key", "nonexistent-key")

    validator.start(
      [
        validator.for("api_key", [
          validator.ExistsSensitive(pool, "exists_users"),
        ]),
      ],
      data,
      ctx,
    )
    |> should.be_error
  })
}

pub fn exists_sensitive_passes_for_empty_value_test() {
  with_pool(fn(pool) {
    let data = form("api_key", "")

    validator.start(
      [
        validator.for("api_key", [
          validator.ExistsSensitive(pool, "exists_users"),
        ]),
      ],
      data,
      ctx,
    )
    |> should.be_ok
  })
}
