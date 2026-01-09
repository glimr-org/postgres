//// PostgreSQL Connection Management
////
//// This module provides postgres database connection management 
//// for Glimr. It handles the initialization and configuration 
//// of postgres connection pools, enabling applications to 
//// efficiently manage database connections. 

import glimr/db/driver.{type Connection}
import glimr_postgres/db/pool.{type Pool}

// ------------------------------------------------------------- Public Functions

/// Starts a PostgreSQL connection pool with the specified 
/// configuration. Searches through the provided connections 
/// list to find a matching connection by name, then initializes 
/// and returns a database pool using that configuration.
///
pub fn start(name: String, connections: List(Connection)) -> Pool {
  let conn = driver.find_by_name(name, connections)
  let config = driver.to_config(conn)

  let assert Ok(database) = pool.start_pool(config)
  database
}
