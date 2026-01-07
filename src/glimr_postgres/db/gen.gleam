//// Value constructors for generated repository code.
////
//// Provides type-safe parameter value construction and
//// conversion to native pog values. Used internally
//// by code generated from database schemas.

import gleam/option.{type Option}
import glimr/db/pool_connection.{
  type Value, BlobValue, BoolValue, FloatValue, IntValue, NullValue, StringValue,
}
import pog

// ------------------------------------------------------------- Public Functions

/// Creates an integer parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn int(v: Int) -> Value {
  pool_connection.int(v)
}

/// Creates a float parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn float(v: Float) -> Value {
  pool_connection.float(v)
}

/// Creates a string parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn string(v: String) -> Value {
  pool_connection.string(v)
}

/// Creates a boolean parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn bool(v: Bool) -> Value {
  pool_connection.bool(v)
}

/// Creates a NULL parameter value for use in queries.
/// Use this when you need to explicitly pass NULL as
/// a parameter value in prepared statements.
///
pub fn null() -> Value {
  pool_connection.null()
}

/// Creates a blob parameter value for use in queries.
/// Wraps the binary data in the generic Value type for
/// parameter binding in prepared statements.
///
pub fn blob(v: BitArray) -> Value {
  pool_connection.blob(v)
}

/// Creates an optional parameter value for use in queries.
/// Converts Some values using the inner function and None
/// to NULL for parameter binding in prepared statements.
///
pub fn nullable(inner: fn(a) -> Value, v: Option(a)) -> Value {
  pool_connection.nullable(inner, v)
}

// ------------------------------------------------------------- Internal Public Functions

/// Converts a generic Value to a native pog.Value. Used
/// internally by generated repository code to translate
/// parameter values for the underlying driver.
///
@internal
pub fn to_pog_value(value: Value) -> pog.Value {
  case value {
    IntValue(v) -> pog.int(v)
    FloatValue(v) -> pog.float(v)
    StringValue(v) -> pog.text(v)
    BoolValue(v) -> pog.bool(v)
    NullValue -> pog.null()
    BlobValue(v) -> pog.bytea(v)
  }
}
