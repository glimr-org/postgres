import gleam/dict
import gleeunit/should
import glimr/config
import glimr/db/db
import glimr/session
import glimr_postgres/postgres
import simplifile
import test_helper

const config_dir = "config"

const session_config_file = "config/session.toml"

fn setup_session_config() -> Nil {
  let _ = simplifile.create_directory_all(config_dir)
  let _ =
    simplifile.write(
      session_config_file,
      "table = \"sessions_test\"
cookie = \"test_session\"
lifetime = 120
expire_on_close = false
",
    )
  clear_config_cache()
  config.load()
  Nil
}

fn cleanup_session_config() -> Nil {
  let _ = simplifile.delete(session_config_file)
  clear_config_cache()
  clear_session_store()
  Nil
}

fn with_clean_session(f: fn() -> a) -> a {
  setup_session_config()

  let db = postgres.start_from_config(test_helper.test_config())

  // Create sessions table
  use conn <- db.get_connection(db)
  let _ =
    db.exec_with(
      conn,
      "CREATE TABLE IF NOT EXISTS sessions_test (
        id TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        last_activity INTEGER NOT NULL
      )",
      [],
    )
  let _ = db.exec_with(conn, "TRUNCATE sessions_test", [])

  // Create and cache the session store
  let store = postgres.session_store(db)
  session.setup(store)

  let result = f()

  db.stop_pool(db)
  cleanup_session_config()
  result
}

// ------------------------------------------------------------- Load Tests

pub fn load_nonexistent_session_returns_empty_test() {
  with_clean_session(fn() {
    let #(data, flash) = session.load("nonexistent-id")

    data |> should.equal(dict.new())
    flash |> should.equal(dict.new())
  })
}

// ------------------------------------------------------------- Save and Load Tests

pub fn save_and_load_data_test() {
  with_clean_session(fn() {
    let data =
      dict.new()
      |> dict.insert("user_id", "42")
      |> dict.insert("role", "admin")

    session.save("sess-1", data, dict.new())

    let #(loaded_data, loaded_flash) = session.load("sess-1")

    dict.get(loaded_data, "user_id") |> should.equal(Ok("42"))
    dict.get(loaded_data, "role") |> should.equal(Ok("admin"))
    loaded_flash |> should.equal(dict.new())
  })
}

pub fn save_and_load_flash_test() {
  with_clean_session(fn() {
    let flash =
      dict.new()
      |> dict.insert("success", "Saved!")
      |> dict.insert("info", "Note this")

    session.save("sess-2", dict.new(), flash)

    let #(loaded_data, loaded_flash) = session.load("sess-2")

    loaded_data |> should.equal(dict.new())
    dict.get(loaded_flash, "success") |> should.equal(Ok("Saved!"))
    dict.get(loaded_flash, "info") |> should.equal(Ok("Note this"))
  })
}

pub fn save_and_load_data_and_flash_test() {
  with_clean_session(fn() {
    let data =
      dict.new()
      |> dict.insert("user_id", "99")

    let flash =
      dict.new()
      |> dict.insert("warning", "Check your email")

    session.save("sess-3", data, flash)

    let #(loaded_data, loaded_flash) = session.load("sess-3")

    dict.get(loaded_data, "user_id") |> should.equal(Ok("99"))
    dict.get(loaded_flash, "warning") |> should.equal(Ok("Check your email"))
  })
}

pub fn save_overwrites_existing_session_test() {
  with_clean_session(fn() {
    let data1 =
      dict.new()
      |> dict.insert("key", "first")

    session.save("sess-4", data1, dict.new())

    let data2 =
      dict.new()
      |> dict.insert("key", "second")

    session.save("sess-4", data2, dict.new())

    let #(loaded_data, _) = session.load("sess-4")
    dict.get(loaded_data, "key") |> should.equal(Ok("second"))
  })
}

// ------------------------------------------------------------- Destroy Tests

pub fn destroy_removes_session_test() {
  with_clean_session(fn() {
    let data =
      dict.new()
      |> dict.insert("key", "value")

    session.save("sess-5", data, dict.new())

    // Verify it exists
    let #(loaded, _) = session.load("sess-5")
    dict.get(loaded, "key") |> should.equal(Ok("value"))

    // Destroy it
    session.destroy("sess-5")

    // Should be gone
    let #(loaded_after, _) = session.load("sess-5")
    loaded_after |> should.equal(dict.new())
  })
}

pub fn destroy_nonexistent_does_not_crash_test() {
  with_clean_session(fn() { session.destroy("nonexistent") })
}

// ------------------------------------------------------------- GC Tests

pub fn gc_does_not_crash_test() {
  with_clean_session(fn() { session.gc() })
}

// ------------------------------------------------------------- FFI Helpers

@external(erlang, "glimr_session_test_ffi", "clear_config_cache")
fn clear_config_cache() -> Nil

@external(erlang, "glimr_session_test_ffi", "clear_session_store")
fn clear_session_store() -> Nil
