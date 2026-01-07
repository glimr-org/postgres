import gleam/dynamic/decode
import gleeunit/should
import glimr/db/migrate as framework_migrate
import glimr_postgres/db/migrate
import glimr_postgres/db/pool
import glimr_postgres/db/query
import pog
import test_helper

fn with_pool(f: fn(pool.Pool) -> a) -> a {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)

  // Clean up migrations table before each test
  let _ =
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS _glimr_migrations")
    })

  let result = f(p)

  // Clean up after test
  let _ =
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS _glimr_migrations")
    })

  pool.stop_pool(p)
  result
}

pub fn ensure_table_creates_migrations_table_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let result = migrate.ensure_table(conn)
      result |> should.be_ok

      // Verify table exists by querying it
      let decoder = decode.at([0], decode.string)
      let assert Ok(_) =
        query.query(conn, "SELECT version FROM _glimr_migrations", [], decoder)
    })
  })
}

pub fn ensure_table_is_idempotent_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      // Call twice - should not error
      let assert Ok(_) = migrate.ensure_table(conn)
      let result = migrate.ensure_table(conn)
      result |> should.be_ok
    })
  })
}

pub fn get_applied_returns_empty_for_new_table_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let result = migrate.get_applied(conn)
      result |> should.be_ok
      let assert Ok(applied) = result
      applied |> should.equal([])
    })
  })
}

pub fn get_applied_returns_applied_versions_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      // Insert some versions manually
      let assert Ok(_) =
        pog.query("INSERT INTO _glimr_migrations (version) VALUES ($1)")
        |> pog.parameter(pog.text("20240101000000"))
        |> pog.execute(conn)

      let assert Ok(_) =
        pog.query("INSERT INTO _glimr_migrations (version) VALUES ($1)")
        |> pog.parameter(pog.text("20240102000000"))
        |> pog.execute(conn)

      let result = migrate.get_applied(conn)
      result |> should.be_ok
      let assert Ok(applied) = result
      applied |> should.equal(["20240101000000", "20240102000000"])
    })
  })
}

pub fn apply_pending_single_migration_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let migration =
        framework_migrate.Migration(
          version: "20240101000000",
          name: "create_test_table",
          sql: "CREATE TABLE test_migrate (id INTEGER)",
        )

      let result = migrate.apply_pending(conn, [migration])
      result |> should.be_ok
      let assert Ok(applied) = result
      applied |> should.equal(["20240101000000"])

      // Verify table was created
      let decoder = decode.at([0], decode.string)
      let assert Ok(tables) =
        query.query(
          conn,
          "SELECT tablename FROM pg_tables WHERE tablename = 'test_migrate'",
          [],
          decoder,
        )
      tables |> should.equal(["test_migrate"])

      // Verify migration was recorded
      let assert Ok(recorded) = migrate.get_applied(conn)
      recorded |> should.equal(["20240101000000"])

      // Clean up
      let _ = query.exec(conn, "DROP TABLE IF EXISTS test_migrate")
    })
  })
}

pub fn apply_pending_multiple_migrations_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let migrations = [
        framework_migrate.Migration(
          version: "20240101000000",
          name: "create_table_a",
          sql: "CREATE TABLE migrate_a (id INTEGER)",
        ),
        framework_migrate.Migration(
          version: "20240102000000",
          name: "create_table_b",
          sql: "CREATE TABLE migrate_b (id INTEGER)",
        ),
      ]

      let result = migrate.apply_pending(conn, migrations)
      result |> should.be_ok
      let assert Ok(applied) = result
      applied |> should.equal(["20240101000000", "20240102000000"])

      // Verify both tables exist
      let decoder = decode.at([0], decode.string)
      let assert Ok(tables) =
        query.query(
          conn,
          "SELECT tablename FROM pg_tables WHERE tablename IN ('migrate_a', 'migrate_b') ORDER BY tablename",
          [],
          decoder,
        )
      tables |> should.equal(["migrate_a", "migrate_b"])

      // Clean up
      let _ = query.exec(conn, "DROP TABLE IF EXISTS migrate_a")
      let _ = query.exec(conn, "DROP TABLE IF EXISTS migrate_b")
    })
  })
}

pub fn apply_pending_empty_list_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let result = migrate.apply_pending(conn, [])
      result |> should.be_ok
      let assert Ok(applied) = result
      applied |> should.equal([])
    })
  })
}

pub fn apply_pending_stops_on_error_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let migrations = [
        framework_migrate.Migration(
          version: "20240101000000",
          name: "valid_migration",
          sql: "CREATE TABLE migrate_valid (id INTEGER)",
        ),
        framework_migrate.Migration(
          version: "20240102000000",
          name: "invalid_migration",
          sql: "INVALID SQL STATEMENT",
        ),
        framework_migrate.Migration(
          version: "20240103000000",
          name: "should_not_run",
          sql: "CREATE TABLE should_not_exist (id INTEGER)",
        ),
      ]

      let result = migrate.apply_pending(conn, migrations)
      result |> should.be_error

      // First migration should have been applied
      let assert Ok(applied) = migrate.get_applied(conn)
      applied |> should.equal(["20240101000000"])

      // Third migration table should not exist
      let decoder = decode.at([0], decode.string)
      let assert Ok(tables) =
        query.query(
          conn,
          "SELECT tablename FROM pg_tables WHERE tablename = 'should_not_exist'",
          [],
          decoder,
        )
      tables |> should.equal([])

      // Clean up
      let _ = query.exec(conn, "DROP TABLE IF EXISTS migrate_valid")
    })
  })
}

pub fn apply_pending_with_comments_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let migration =
        framework_migrate.Migration(
          version: "20240101000000",
          name: "with_comments",
          sql: "-- This is a comment\nCREATE TABLE comment_test (id INTEGER)\n-- Another comment",
        )

      let result = migrate.apply_pending(conn, [migration])
      result |> should.be_ok

      // Verify table was created
      let decoder = decode.at([0], decode.string)
      let assert Ok(tables) =
        query.query(
          conn,
          "SELECT tablename FROM pg_tables WHERE tablename = 'comment_test'",
          [],
          decoder,
        )
      tables |> should.equal(["comment_test"])

      // Clean up
      let _ = query.exec(conn, "DROP TABLE IF EXISTS comment_test")
    })
  })
}

pub fn apply_pending_with_multiple_statements_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let assert Ok(_) = migrate.ensure_table(conn)

      let migration =
        framework_migrate.Migration(
          version: "20240101000000",
          name: "multi_statement",
          sql: "CREATE TABLE multi_a (id INTEGER); CREATE TABLE multi_b (name TEXT)",
        )

      let result = migrate.apply_pending(conn, [migration])
      result |> should.be_ok

      // Verify both tables were created
      let decoder = decode.at([0], decode.string)
      let assert Ok(tables) =
        query.query(
          conn,
          "SELECT tablename FROM pg_tables WHERE tablename IN ('multi_a', 'multi_b') ORDER BY tablename",
          [],
          decoder,
        )
      tables |> should.equal(["multi_a", "multi_b"])

      // Clean up
      let _ = query.exec(conn, "DROP TABLE IF EXISTS multi_a")
      let _ = query.exec(conn, "DROP TABLE IF EXISTS multi_b")
    })
  })
}
