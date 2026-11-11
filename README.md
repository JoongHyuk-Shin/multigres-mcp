# multigres-mcp

An MCP server for [Multigres](https://github.com/multigres/multigres) clusters. It gives an MCP client two groups of tools:

- Postgres diagnostics through the Multigres gateway: schemas and objects, read-only SQL, query plans with hypothetical indexes, top queries, index advice and a health check. These follow the tools of [postgres-mcp](https://github.com/crystaldba/postgres-mcp).
- Cluster tools through multiadmin: cluster status, backups, node logs, and in the unrestricted mode switching the primary and starting or expiring backups.

The server is read-only by default. Write tools are registered only in the unrestricted mode.

It has been tested against a local Multigres Docker cluster built from commit `5fa0f2955ede` (2026-11-10, PostgreSQL 17.7, two cells) on Linux with Docker Engine. Other setups have not been tested.

## Install

Each [release](https://github.com/JoongHyuk-Shin/multigres-mcp/releases) has a `multigres-mcp-<version>.mcpb` file. It is an [MCP Bundle](https://github.com/modelcontextprotocol/mcpb): clients that install bundles ask for the settings under "Configuration" and run the server. It is also a zip file whose `server/` directory holds binaries for Linux and macOS on amd64 and arm64 and for Windows on amd64, which any other client can run directly.

## Build

Requires Go 1.26 or later.

```sh
git clone https://github.com/JoongHyuk-Shin/multigres-mcp
cd multigres-mcp
go build -o multigres-mcp ./cmd/multigres-mcp
```

The server talks MCP over standard input and output. `./multigres-mcp --help` prints the options.

`packaging/build-mcpb.sh` builds the release bundle into `dist/`. It also needs jq, Node.js and openssl.

## Configuration

Every option can be given as a flag or as an environment variable. A flag wins over the variable.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--database-url` | `DATABASE_URI` | none, required | Connection string of the Multigres gateway. |
| `--admin-url` | `MULTIGRES_ADMIN_URL` | none | multiadmin HTTP address. The cluster tools are registered only when it is set. |
| `--admin-token` | `MULTIGRES_ADMIN_TOKEN` | none | Bearer token for multiadmin started with `--enable-auth`. |
| `--node-host-map` | `MULTIGRES_NODE_HOST_MAP` | none | `reported=reachable` host pairs, comma separated, for the direct node connections of `read_node_log` and of the `node` option of `diagnose` and `get_top_queries`. multiadmin reports each node's host as the cluster sees it; this replaces a host that the server cannot reach. Ports are not changed. |
| `--access-mode` | `MULTIGRES_MCP_ACCESS_MODE` | `restricted` | `restricted` (read-only) or `unrestricted` (adds the write tools). |
| `--sql-timeout` | `MULTIGRES_MCP_SQL_TIMEOUT` | `25s` | Statement timeout for the SQL every tool sends. See "Things to know". |

Direct node connections reuse the user, password and database of `--database-url` and take the host and port from multiadmin.

In the local Docker cluster both nodes report the host `localhost`, while their Postgres ports are reachable only at the container's address. Find that address in the Multigres checkout with:

```sh
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(docker compose ps -q multigres)"
```

and pass it as `--node-host-map localhost=<address>`.

## Adding it to an MCP client

With Claude Code, for the local Docker cluster (replace `172.18.0.2` with the address found above):

```sh
claude mcp add multigres -e DATABASE_URI=postgres://postgres:postgres@localhost:15432/postgres -- /path/to/multigres-mcp --admin-url http://localhost:15000 --node-host-map localhost=172.18.0.2
```

Other clients take the same command, arguments and environment in their server configuration, for example:

```json
{
  "mcpServers": {
    "multigres": {
      "type": "stdio",
      "command": "/path/to/multigres-mcp",
      "args": ["--admin-url", "http://localhost:15000", "--node-host-map", "localhost=172.18.0.2"],
      "env": {
        "DATABASE_URI": "postgres://postgres:postgres@localhost:15432/postgres"
      }
    }
  }
}
```

## Tools

| Tool | Mode | What it does |
|---|---|---|
| `list_schemas` | both | List the schemas. The `multigres` schema is internal to Multigres. |
| `list_objects` | both | List the tables, views, sequences or extensions of a schema. |
| `get_object_details` | both | Columns, constraints and indexes of a table or view, or details of a sequence or extension. |
| `execute_sql` | both | Run one read-only statement (`SELECT`, `EXPLAIN` of a `SELECT`, or `SHOW`). In both modes it only reads. |
| `explain_query` | both | The plan of a query, optionally with `ANALYZE` or with hypothetical indexes. A query with `$1` placeholders, as stored in query statistics, gets a generic plan. |
| `get_top_queries` | both | The slowest or most expensive queries, from `pg_stat_statements` and from the query statistics each gateway keeps, shown separately. |
| `analyze_workload_indexes` | both | Index advice for the workload in `pg_stat_statements`, by comparing planner costs with hypothetical indexes. Nothing is created. |
| `analyze_query_indexes` | both | The same for up to 10 given queries. |
| `diagnose` | both | Health checks: bloat, vacuum, locks, activity, waits, slow queries, missing indexes, cache, log, index health, sequences, replication and constraints. It starts with the statistics period of the node that answered. |
| `cluster_status` | both, needs `--admin-url` | Cells, databases, the primary and replicas of each shard with LSN and lag, gateways, orchestrators, recent primary changes, and whether the connected user is a superuser. |
| `list_backups` | both, needs `--admin-url` | Backups of a shard. |
| `verify_backups` | both, needs `--admin-url` | Run pgBackRest `verify` on a shard's backups, which checks that the backups and archived WAL in the repository are valid. It does not run a restore. |
| `get_backup_job_status` | both, needs `--admin-url` | Status of a backup job. |
| `read_node_log` | both, needs `--admin-url` | The end of a node's Postgres log, read over a direct connection to that node. |
| `execute_write_sql` | unrestricted | Run one statement that may change data or schema, committed if it succeeds. |
| `switch_primary` | unrestricted, needs `--admin-url` | Ask Multigres to switch a shard's primary, then report the old and new primary and terms. Multigres may elect the same node again; the tool says so and does not retry. |
| `create_backup` | unrestricted, needs `--admin-url` | Start a backup and return its job id. |
| `expire_backups` | unrestricted, needs `--admin-url` | Expire backups with the retention rules configured in the cluster. |
| `set_postgres_restarts` | unrestricted, needs `--admin-url` | Turn automatic Postgres restarts on a node on or off. |

That is 14 tools in the restricted mode and 19 in the unrestricted mode, or 9 and 10 without `--admin-url`. `diagnose` and `get_top_queries` take an optional `node` to read one node directly instead of through the gateway, for example the old primary after a failover.

## Safety

The restricted mode does not register any tool that can write. Writing SQL is a tool of its own, `execute_write_sql`, so a client permission given to `execute_sql` never covers a write, even after the server is restarted in the unrestricted mode.

Every statement that `execute_sql`, `explain_query` and `analyze_query_indexes` receive passes these checks. They do not rely on each other: checks 1, 3 and 4 each stop a write on their own.

1. It is parsed with the Multigres parser. It must be exactly one statement, the top statement must be `SELECT`, `EXPLAIN` of a `SELECT`, or `SHOW`, and the tree must not contain data-changing statements, `SELECT INTO` or locking clauses. Functions must be on an allow list (the postgres-mcp list with functions that have side effects removed), and a schema-qualified function must be in `pg_catalog`.
2. It is sent with the extended query protocol, which carries one statement only.
3. The read-only connection is opened with `default_transaction_read_only=on`.
4. It runs in `BEGIN TRANSACTION READ ONLY`, which is always rolled back.
5. `search_path` is pinned, and a statement that calls an unqualified function whose name also exists outside `pg_catalog` is not sent.
6. A statement timeout is set.

This covers the statements the model sends and the functions they call directly. The bodies of objects already in the database (views, functions, operators, policies) are trusted and not checked. Against a production database, connect as a read-only role as well. `cluster_status` shows whether the connected user is a superuser.

Multigres hands pooled Postgres connections from one client to the next. Within the same transaction, the server removes the hypothetical indexes and session advisory locks a tool call may have left on its connection. If that cleanup fails, it ends its own Postgres session rather than hand the state on. `execute_write_sql` rejects transaction control, session-level `SET` and `RESET`, `multigres.*` settings, hypopg functions, and statements that leave session state behind (temporary objects, `PREPARE`, `LISTEN`, `DECLARE ... WITH HOLD`, `LOAD`, `DISCARD`, `DO`). Session settings (`set_config(..., false)`) and the random seed (`setseed`) changed inside the body of a function or trigger are not removed and can reach the next client of that connection; like the other object bodies, they are trusted.

Direct node connections run fixed queries only, never SQL from the model.

The multiadmin address (in full and as `host:port`), the admin token and the database password are removed from tool results and error messages. The removal replaces text, so a password that is a common word is masked wherever that word appears. A password equal to the user or database name is not masked, because results show those names; the server prints a warning when it starts.

## Things to know

- `--sql-timeout` defaults to 25s because the multipooler ends a transaction's connection when one statement runs past `--connpool-user-reserved-inactivity-timeout` (default 30s). To use a longer timeout, raise that pooler setting too.
- The results of `execute_sql` and `execute_write_sql` end with a line naming the settings the server set for that call only (`statement_timeout`, `lock_timeout`, and for reads `search_path`), and the `statement_timeout` its own session had before. The gateway enforces `statement_timeout` itself without setting it on the node, so the node's `pg_settings` shows 0 for it while `SHOW` through the gateway answers with the gateway's value.
- Cumulative statistics are kept per node. After a failover the new primary's statistics do not include what happened on the old primary. The tools that use statistics start with the period they cover; the `node` option of `diagnose` and `get_top_queries` reads another node.
- Index sizes in the advice are hypopg estimates and ignore btree deduplication. On a column with many repeated values the estimate can be several times the real size (on 200,000 rows with 1,000 distinct values: 4,644,864 bytes estimated, 1,384,448 bytes real).
- The gateway truncates the query text it keeps to `--query-metrics-sql-max-bytes` (default 4096) without a marker. Plans and index advice use the full `pg_stat_statements` text or the full statement you give.
- `get_top_queries` shows the first 2,000 characters of each `pg_stat_statements` text and `diagnose` the first 500, with the full length and the `queryid`, so one very long statement cannot fill the 50,000-character result. The result says how to read a longer text with `execute_sql` in pieces of 20,000 characters. The lock check shows the first 500 characters of the waiting and blocking statements. The index advisors analyze the full text but list each skipped or costed query by its first 200 characters and full length, plus the `queryid` for workload queries, and show the first 500 characters of a skip reason.
- `VACUUM` and `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so `execute_write_sql` cannot run them.
- Calls to multiadmin time out after 30 seconds, `verify_backups` after 10 minutes.
- If `execute_write_sql` gets no reply to its `COMMIT`, it says it does not know whether the statement was committed. Check the data before sending it again.

## Tests

```sh
go test ./...
```

runs the unit tests. The integration and end-to-end tests have the build tag `cluster` and need a local Multigres Docker cluster with `pg_stat_statements` preloaded and `hypopg` installed. They switch the primary, drop and recreate the two extensions, and end sessions, so do not point them at a cluster you care about. They fail rather than skip when the cluster cannot be reached.

Start the cluster from a Multigres checkout:

```sh
git clone https://github.com/multigres/multigres
cd multigres
git checkout 5fa0f2955ede
MULTIGRES_PG_EXTRA_CONF="shared_preload_libraries = 'pg_stat_statements'" docker compose up -d --build
```

Wait until `docker compose ps` shows the container as healthy, then install `hypopg` and create both extensions:

```sh
docker compose exec -u root multigres sh -c 'apt-get update -q && apt-get install -y -q --no-install-recommends postgresql-17-hypopg'
docker compose exec -e PGPASSWORD=postgres multigres psql -h localhost -p 15432 -U postgres -d postgres -c 'CREATE EXTENSION pg_stat_statements' -c 'CREATE EXTENSION hypopg'
```

Then, in this repository, with `MULTIGRES_MCP_TEST_NODE_HOST` set to the container address from the `docker inspect` command above:

```sh
MULTIGRES_MCP_TEST_NODE_HOST=<address> go test -tags cluster -p 1 -count=1 -timeout 25m ./...
```

`-p 1` runs one package at a time, because the packages share the cluster's primary, extensions and connection pools. The tests use the gateway at `localhost:15432`, multiadmin at `localhost:15000` and the node ports 25432 and 25433 of the Docker cluster; `MULTIGRES_MCP_TEST_GATEWAY_URL` and `MULTIGRES_MCP_TEST_ADMIN_URL` change the first two. [test/facts-map.md](test/facts-map.md) maps the Multigres behaviour the design relies on to the test that checks it.

## License

Apache License 2.0, see [LICENSE](LICENSE). Parts adapted from postgres-mcp are under the MIT License, see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
