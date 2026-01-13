import glimr/db/pool_connection

pub const test_url = "postgresql://test:test@localhost:5433/glimr_test"

pub fn test_config() -> pool_connection.Config {
  pool_connection.PostgresConfig(test_url, 2)
}

/// Suppress supervisor shutdown reports (expected during pool cleanup in tests)
@external(erlang, "logger_ffi", "suppress_supervisor_reports")
pub fn suppress_supervisor_reports() -> Nil
