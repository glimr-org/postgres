import glimr/db/pool_connection

pub const test_url = "postgresql://test:test@localhost:5433/glimr_test"

pub fn test_config() -> pool_connection.Config {
  pool_connection.PostgresConfig(test_url, 2)
}
