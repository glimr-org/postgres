# Glimr PostgreSQL Driver ✨

The official PostgreSQL driver for the Glimr web framework, providing connection pooling, query execution, and migration support. This package is meant to be used alongside the `lpil/pog` and `glimr-org/framework` packages and the `glimr-org/glimr` starter repository.

If you'd like to stay updated on Glimr's development, Follow [@migueljarias](https://x.com/migueljarias) on X (that's me) for updates.

## About

> **Note:** This repository contains the PostgreSQL driver for Glimr. If you want to build an application using Glimr, visit the main [Glimr repository](https://github.com/glimr-org/glimr).

## Features

- **Connection Pooling** - Efficient connection management with automatic checkout/checkin
- **Query Builder** - Type-safe query execution with parameter binding
- **Transaction Support** - Atomic operations with automatic retry on deadlock
- **Migration Runner** - Apply database migrations with version tracking
- **HTTP Context** - Easy pool access in web request handlers

## Installation

Add the PostgreSQL driver to your Gleam project:

```sh
gleam add glimr_postgres
```

## Learn More

- [Glimr](https://github.com/glimr-org/glimr) - Main Glimr repository
- [Glimr Framework](https://github.com/glimr-org/framework) - Core framework
- [pog](https://hexdocs.pm/pog/) - PostgreSQL client for Gleam

### Built With

- [**pog**](https://hexdocs.pm/pog/) - PostgreSQL client library for Gleam
- [**gleam_otp**](https://hexdocs.pm/gleam_otp/) - OTP support for connection pooling

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

The Glimr PostgreSQL driver is open-sourced software licensed under the [MIT](https://opensource.org/license/MIT) license.
