//// Value constructors for generated repository code.
////
//// Provides type-safe parameter value construction and
//// conversion to native pog values. Used internally
//// by code generated from database schemas.

import gleam/option.{type Option}
import glimr/db/db.{
  type Value, BlobValue, BoolValue, FloatValue, IntValue, NullValue, StringValue,
}
import pog

// ------------------------------------------------------------- Public Functions

/// Creates an integer parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn int(v: Int) -> Value {
  db.int(v)
}

/// Creates a float parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn float(v: Float) -> Value {
  db.float(v)
}

/// Creates a string parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn string(v: String) -> Value {
  db.string(v)
}

/// Creates a boolean parameter value for use in queries.
/// Wraps the value in the generic Value type for parameter
/// binding in prepared statements.
///
pub fn bool(v: Bool) -> Value {
  db.bool(v)
}

/// Creates a NULL parameter value for use in queries.
/// Use this when you need to explicitly pass NULL as
/// a parameter value in prepared statements.
///
pub fn null() -> Value {
  db.null()
}

/// Creates a blob parameter value for use in queries.
/// Wraps the binary data in the generic Value type for
/// parameter binding in prepared statements.
///
pub fn blob(v: BitArray) -> Value {
  db.blob(v)
}

/// Creates an optional parameter value for use in queries.
/// Converts Some values using the inner function and None
/// to NULL for parameter binding in prepared statements.
///
pub fn nullable(inner: fn(a) -> Value, v: Option(a)) -> Value {
  db.nullable(inner, v)
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
