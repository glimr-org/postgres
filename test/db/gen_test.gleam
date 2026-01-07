import gleam/dynamic/decode
import gleam/option
import gleeunit/should
import glimr/db/pool_connection.{
  BlobValue, BoolValue, FloatValue, IntValue, NullValue, StringValue,
}
import glimr_postgres/db/gen
import glimr_postgres/db/pool
import glimr_postgres/db/query
import pog
import test_helper

fn with_pool(f: fn(pool.Pool) -> a) -> a {
  let config = test_helper.test_config()
  let assert Ok(p) = pool.start_pool(config)
  let result = f(p)
  pool.stop_pool(p)
  result
}

// ------------------------------------------------------------- Value Constructor Tests

pub fn int_creates_int_value_test() {
  let value = gen.int(42)
  value |> should.equal(IntValue(42))
}

pub fn int_negative_value_test() {
  let value = gen.int(-123)
  value |> should.equal(IntValue(-123))
}

pub fn int_zero_value_test() {
  let value = gen.int(0)
  value |> should.equal(IntValue(0))
}

pub fn float_creates_float_value_test() {
  let value = gen.float(3.14)
  value |> should.equal(FloatValue(3.14))
}

pub fn float_negative_value_test() {
  let value = gen.float(-2.5)
  value |> should.equal(FloatValue(-2.5))
}

pub fn float_zero_value_test() {
  let value = gen.float(0.0)
  value |> should.equal(FloatValue(0.0))
}

pub fn string_creates_string_value_test() {
  let value = gen.string("hello")
  value |> should.equal(StringValue("hello"))
}

pub fn string_empty_value_test() {
  let value = gen.string("")
  value |> should.equal(StringValue(""))
}

pub fn string_unicode_value_test() {
  let value = gen.string("hello world!")
  value |> should.equal(StringValue("hello world!"))
}

pub fn bool_true_value_test() {
  let value = gen.bool(True)
  value |> should.equal(BoolValue(True))
}

pub fn bool_false_value_test() {
  let value = gen.bool(False)
  value |> should.equal(BoolValue(False))
}

pub fn null_creates_null_value_test() {
  let value = gen.null()
  value |> should.equal(NullValue)
}

pub fn blob_creates_blob_value_test() {
  let data = <<1, 2, 3, 4, 5>>
  let value = gen.blob(data)
  value |> should.equal(BlobValue(<<1, 2, 3, 4, 5>>))
}

pub fn blob_empty_value_test() {
  let value = gen.blob(<<>>)
  value |> should.equal(BlobValue(<<>>))
}

pub fn nullable_with_some_value_test() {
  let value = gen.nullable(gen.int, option.Some(42))
  value |> should.equal(IntValue(42))
}

pub fn nullable_with_none_value_test() {
  let value = gen.nullable(gen.int, option.None)
  value |> should.equal(NullValue)
}

pub fn nullable_with_string_some_test() {
  let value = gen.nullable(gen.string, option.Some("hello"))
  value |> should.equal(StringValue("hello"))
}

pub fn nullable_with_string_none_test() {
  let value = gen.nullable(gen.string, option.None)
  value |> should.equal(NullValue)
}

// ------------------------------------------------------------- to_pog_value Tests

pub fn to_pog_value_int_test() {
  let pog_value = gen.to_pog_value(IntValue(42))
  pog_value |> should.equal(pog.int(42))
}

pub fn to_pog_value_float_test() {
  let pog_value = gen.to_pog_value(FloatValue(3.14))
  pog_value |> should.equal(pog.float(3.14))
}

pub fn to_pog_value_string_test() {
  let pog_value = gen.to_pog_value(StringValue("hello"))
  pog_value |> should.equal(pog.text("hello"))
}

pub fn to_pog_value_bool_test() {
  let pog_value = gen.to_pog_value(BoolValue(True))
  pog_value |> should.equal(pog.bool(True))
}

pub fn to_pog_value_null_test() {
  let pog_value = gen.to_pog_value(NullValue)
  pog_value |> should.equal(pog.null())
}

pub fn to_pog_value_blob_test() {
  let pog_value = gen.to_pog_value(BlobValue(<<1, 2, 3>>))
  pog_value |> should.equal(pog.bytea(<<1, 2, 3>>))
}

// ------------------------------------------------------------- Integration Tests

pub fn int_value_roundtrip_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
      let assert Ok(_) = query.exec(conn, "CREATE TABLE gen_test (val INTEGER)")

      let pog_val = gen.to_pog_value(gen.int(42))
      let assert Ok(_) =
        query.query(
          conn,
          "INSERT INTO gen_test VALUES ($1)",
          [pog_val],
          decode.dynamic,
        )

      let decoder = decode.at([0], decode.int)
      let assert Ok([result]) =
        query.query(conn, "SELECT val FROM gen_test", [], decoder)

      result |> should.equal(42)

      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
    })
  })
}

pub fn string_value_roundtrip_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
      let assert Ok(_) = query.exec(conn, "CREATE TABLE gen_test (val TEXT)")

      let pog_val = gen.to_pog_value(gen.string("hello world"))
      let assert Ok(_) =
        query.query(
          conn,
          "INSERT INTO gen_test VALUES ($1)",
          [pog_val],
          decode.dynamic,
        )

      let decoder = decode.at([0], decode.string)
      let assert Ok([result]) =
        query.query(conn, "SELECT val FROM gen_test", [], decoder)

      result |> should.equal("hello world")

      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
    })
  })
}

pub fn bool_value_roundtrip_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
      let assert Ok(_) = query.exec(conn, "CREATE TABLE gen_test (val BOOLEAN)")

      let pog_val = gen.to_pog_value(gen.bool(True))
      let assert Ok(_) =
        query.query(
          conn,
          "INSERT INTO gen_test VALUES ($1)",
          [pog_val],
          decode.dynamic,
        )

      let decoder = decode.at([0], decode.bool)
      let assert Ok([result]) =
        query.query(conn, "SELECT val FROM gen_test", [], decoder)

      result |> should.equal(True)

      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
    })
  })
}

pub fn null_value_roundtrip_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
      let assert Ok(_) = query.exec(conn, "CREATE TABLE gen_test (val TEXT)")

      let pog_val = gen.to_pog_value(gen.null())
      let assert Ok(_) =
        query.query(
          conn,
          "INSERT INTO gen_test VALUES ($1)",
          [pog_val],
          decode.dynamic,
        )

      let decoder = decode.at([0], decode.optional(decode.string))
      let assert Ok([result]) =
        query.query(conn, "SELECT val FROM gen_test", [], decoder)

      result |> should.equal(option.None)

      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
    })
  })
}

pub fn blob_value_roundtrip_test() {
  with_pool(fn(p) {
    pool.get_connection(p, fn(conn) {
      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
      let assert Ok(_) = query.exec(conn, "CREATE TABLE gen_test (val BYTEA)")

      let data = <<1, 2, 3, 4, 5>>
      let pog_val = gen.to_pog_value(gen.blob(data))
      let assert Ok(_) =
        query.query(
          conn,
          "INSERT INTO gen_test VALUES ($1)",
          [pog_val],
          decode.dynamic,
        )

      let decoder = decode.at([0], decode.bit_array)
      let assert Ok([result]) =
        query.query(conn, "SELECT val FROM gen_test", [], decoder)

      result |> should.equal(<<1, 2, 3, 4, 5>>)

      let _ = query.exec(conn, "DROP TABLE IF EXISTS gen_test")
    })
  })
}
