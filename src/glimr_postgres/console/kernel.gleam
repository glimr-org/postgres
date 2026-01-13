//// glimr_postgres Console Kernel
////
//// Provides PostgreSQL-specific console commands for migrations
//// and code generation. Add these commands to your application's
//// command provider.
////
//// ## Usage
////
//// ```gleam
//// import glimr_postgres/console/kernel as postgres_kernel
////
//// pub fn register() -> List(Command) {
////   list.flatten([
////     kernel.commands(),
////     postgres_kernel.commands(),
////   ])
//// }
//// ```

import glimr/console/command.{type Command}
import glimr_postgres/internal/console/commands/cache_table
import glimr_postgres/internal/console/commands/gen
import glimr_postgres/internal/console/commands/migrate

// ------------------------------------------------------------- Public Functions

/// Returns the list of PostgreSQL console commands. Users
/// add these to their command_provider to enable postgres
/// specific CLI operations.
///
pub fn commands() -> List(Command) {
  [
    migrate.command(),
    gen.command(),
    cache_table.command(),
  ]
}
