# RisingWave

`risingwave://user:password@host:port/dbname?query`

| URL Query  | WithInstance Config | Description                                                                                                                                                                                                           |
|------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table                                                                                                                                                                                          |
| `x-migrations-table-quoted` | `MigrationsTableQuoted` | By default, migrate quotes the migration table for SQL injection safety reasons. This option disable quoting and naively checks that you have quoted the migration table name. e.g. `"my_schema"."schema_migrations"` |
| `x-statement-timeout` | `StatementTimeout` | Abort any statement that takes more than the specified number of milliseconds                                                                                                                                         |
| `dbname` | `DatabaseName` | The name of the database to connect to                                                                                                                                                                                |
| `user` | | The user to sign in as                                                                                                                                                                                                |
| `password` | | The user's password                                                                                                                                                                                                   | 
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost)                                                                                                                  |
| `port` | | The port to bind to. (default is 4566)                                                                                                                                                                                |
| `connect_timeout` | | Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely.                                                                                                                               |
| `sslcert` | | Cert file location. The file must contain PEM encoded data.                                                                                                                                                           |
| `sslkey` | | Key file location. The file must contain PEM encoded data.                                                                                                                                                            |
| `sslrootcert` | | The location of the root certificate file. The file must contain PEM encoded data.                                                                                                                                    | 
| `sslmode` | | Whether or not to use SSL (disable\|require\|verify-ca\|verify-full)                                                                                                                                                  |

RisingWave is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.

## Notes

* RisingWave does not support transaction yet. When running multiple SQL statements in one migration, the queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
