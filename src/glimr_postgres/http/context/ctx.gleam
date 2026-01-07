//// PostgreSQL HTTP Context
////
//// Provides a context for managing PostgreSQL connection pools
//// in HTTP applications. Use `ctx.postgres.pool` for the 
//// default pool and `ctx.postgres.pool_for("name")` for named 
//// connections.

import gleam/dict.{type Dict}
import gleam/list
import gleam/string
import glimr/db/driver.{
  type Connection, PostgresConnection, PostgresUriConnection,
}
import glimr_postgres/db/pool.{type Pool}

// ------------------------------------------------------------- Public Types

/// PostgreSQL context containing the default pool and a 
/// function to access named connection pools. Use pool for the 
/// default connection and pool_for to access named connections.
///
pub type PostgresContext {
  PostgresContext(pool: Pool, pool_for: fn(String) -> Pool)
}

// ------------------------------------------------------------- Public Functions

/// Loads PostgreSQL pools from the given connections. Filters 
/// to only PostgreSQL connections and starts a pool for each.
///
/// Use `ctx.postgres.pool` to get the default pool (the one 
/// with `is_default: True`). Use `ctx.postgres.pool_for("name")` 
/// to access other connections by name.
///
pub fn load(connections: List(Connection)) -> PostgresContext {
  let connections =
    list.filter(connections, fn(conn) {
      case conn {
        PostgresConnection(..) -> True
        PostgresUriConnection(..) -> True
        _ -> False
      }
    })

  case connections {
    [] -> panic as "No PostgreSQL connections found in database configuration."
    _ -> {
      let pools =
        connections
        |> list.map(fn(conn) {
          let config = driver.to_config(conn)
          let assert Ok(started_pool) = pool.start_pool(config)
          #(driver.connection_name(conn), started_pool)
        })
        |> dict.from_list

      let default_pool = find_default_pool(connections, pools)

      PostgresContext(pool: default_pool, pool_for: fn(name) {
        get_pool(pools, name)
      })
    }
  }
}

// ------------------------------------------------------------- Private Functions

/// Finds the default pool from the list of connections. Looks
/// for the connection with is_default set to true and returns 
/// its pool. Panics if no default is found or multiple exist.
///
fn find_default_pool(
  connections: List(Connection),
  pools: Dict(String, Pool),
) -> Pool {
  let defaults = list.filter(connections, driver.is_default)

  case defaults {
    [] ->
      panic as "No default PostgreSQL connection found. Set is_default: True on one connection."
    [default] -> {
      let assert Ok(p) = dict.get(pools, driver.connection_name(default))
      p
    }
    _ ->
      panic as "Multiple PostgreSQL connections have is_default: True. Only one is allowed."
  }
}

/// Gets a pool by name, panicking with a helpful message if not
/// found. The error message includes a list of available
/// connection names to help with debugging.
///
fn get_pool(pools: Dict(String, Pool), name: String) -> Pool {
  case dict.get(pools, name) {
    Ok(p) -> p
    Error(_) ->
      panic as {
        "PostgreSQL connection '"
        <> name
        <> "' not found. "
        <> "Available connections: "
        <> dict_keys_string(pools)
      }
  }
}

/// Converts dict keys to a comma-separated string for error
/// messages. Keys are sorted alphabetically for consistent
/// output in error messages.
///
fn dict_keys_string(d: Dict(String, Pool)) -> String {
  d
  |> dict.keys
  |> list.sort(string.compare)
  |> string.join(", ")
}
