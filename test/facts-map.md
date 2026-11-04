# What the tests pin down about Multigres

Before writing this server, its behaviour was measured on a local Multigres Docker cluster (commit 49e7a2f7, two cells, `pg_stat_statements` preloaded and `hypopg` installed). The design leans on those observations. This table maps each one to the test that would turn red if Multigres changed it, or says why no test can reach it.

Tests tagged `cluster` need that cluster; see the README.

## Postgres tools through the gateway

| Observed | Test |
|---|---|
| Catalog reads pass the gateway, and the `multigres` schema is listed with the others. | `test/e2e` `TestEveryToolAnswersThroughTheBinary`: `list_schemas` shows "Multigres Internal Schema", `list_objects` lists tables and extensions, `get_object_details` shows the column, primary key and foreign key. |
| `BEGIN TRANSACTION READ ONLY`, `ROLLBACK` and the startup option `default_transaction_read_only=on` reach Postgres through the gateway. | `internal/db` `TestReadOnlyStartupOptionSurvivesBatchRollback`, `TestReadTransactionRejectsWritesWithoutStartupOption`. |
| `EXPLAIN`, `EXPLAIN ANALYZE` and plans with a hypothetical index pass the gateway. | `TestEveryToolAnswersThroughTheBinary`: `explain_query` with `analyze` shows "actual time=", with a hypothetical index the plan names it. |
| `pg_stat_statements` reads pass. The stored text is the gateway's rewrite (`AS o INNER JOIN`) and keeps `$1` placeholders. | `TestEveryToolAnswersThroughTheBinary` (`get_top_queries`); `internal/advisor` `TestCollectColumnsFindsEveryConditionClause` (the rewritten join and the placeholder query as measured); `internal/sqlguard` `TestGenericPlanRoundTripKeepsMeaning`. |
| Index advice on `customer_id` comes back through the gateway. | `TestEveryToolAnswersThroughTheBinary` (`analyze_query_indexes`). |
| A hypothetical index created in one autocommit statement and explained in the next can vanish under load, because the gateway may use another pooled connection. | Every tool call runs in one transaction on one connection; `internal/tools` `TestNoHypotheticalIndexReachesOtherClients` runs the tools with busy sessions around them. |
| A hypothetical index left by a client stays on the pooled connection and shows up in other clients' plans, whether it was made in autocommit, in a rolled-back transaction or in one multi-statement string. Only a counter of the same client kind, in autocommit and in a transaction, sees it. | `TestNoHypotheticalIndexReachesOtherClients` (counts 40 new connections in each mode, with a control that leaves one on purpose and must be seen 40 of 40), `TestExplainQueryClearsItsHypotheticalIndexes`, `TestExecuteSQLClearsHypopgStateFromViews`; `internal/db` `TestHypopgStateCreatedInsideViewsIsCleared`. |

## Diagnostic checks

| Observed | Test |
|---|---|
| Bloat, vacuum, lock, activity, wait, slow query, missing index and cache queries return through the gateway what a direct connection returns. The client's `application_name` reaches Postgres. | `internal/tools` `TestDiagnoseShowsInducedState` (each check against a state the test creates; the lock check names the waiting and blocking sessions by `application_name`). |
| `client_addr` is empty for every session that came through the gateway. | Not asserted. The activity check prints this as a note instead of relying on it. |
| The admin API's per-gateway query statistics work without `pg_stat_statements`. | `TestEveryToolAnswersThroughTheBinary` (`get_top_queries` with `source: gateway`); `internal/tools` `TestToolsNameTheMissingExtension` (falls back to them with the extension dropped). |
| The gateway refuses `pg_read_file`; the log is readable over a direct node connection. | `read_node_log` and the `log` check read over a direct connection only: `TestEveryToolAnswersThroughTheBinary`, `TestDiagnoseShowsInducedState` (finds an error the test logged). |
| Statistics are per node. After a primary switch the new primary's counters start from zero. | `internal/tools` `TestClusterWritesKeepSecretsAndReportTheirOutcome`, subtest "switch, then the statistics period and the index advisor": the period shows the promotion term, the new primary reports restarted tables, and index advice still comes back. |

## Direct node access

| Observed | Test |
|---|---|
| The poolers report host `localhost`; from the host the nodes are reachable at the container address. | `internal/config` `TestNodeHostMapIsExactMatch`; `internal/db` `TestReadNodeConnectsToThatNode`; `internal/tools` `TestNodeDirectFollowsTheHostMap`. |
| Diagnosis on a replica was not tried in the first measurement. | `TestDiagnoseShowsInducedState` runs the replication check on the replica and expects "this node is a replica: true". |

## Admin API

| Observed | Test |
|---|---|
| All 28 operations answer without a token. This server calls 16 of them. | `internal/tools` `TestReadOnlyToolsCallOnlyReadProcedures` (restricted mode reaches only read procedures); `TestEveryToolAnswersThroughTheBinary` calls all 19 tools. |
| With `--enable-auth` a missing token, a token signed by another key and a token from another issuer all get HTTP 401 `{"code":"unauthenticated","message":"authentication failed"}`; a valid token passes. | Checked by hand on 2026-11-03 against multiadmin started with `--enable-auth` and a test JWKS server (it also needs `--service-map grpc-multiadmin` to serve the Connect API). `TestFailuresAreReportedAndTheServerStaysUp`, subtest "admin server refuses the token", replays that response; `internal/admin` `TestReaderSendsTheBearerTokenOnlyWhenSet`. |
| A backup returns a job id at once; job status, verification and expiry answer. | `TestEveryToolAnswersThroughTheBinary` (`create_backup`, `get_backup_job_status`, `verify_backups`, `expire_backups`). |
| A primary switch only makes the current primary step down; the same node can be elected again. | `test/e2e` `TestSwitchPrimaryTwice`; `TestClusterWritesKeepSecretsAndReportTheirOutcome`. |
| Errors cannot be sorted by HTTP status (some input errors come as code 2 with HTTP 500). | The server passes the code and message through: `TestFailuresAreReportedAndTheServerStaysUp` expects "admin API error unauthenticated: authentication failed". |
| Connection, migration and consensus rule operations answer. | Not used by this server, so no test. |

## Extensions

| Observed | Test |
|---|---|
| The default image does not preload `pg_stat_statements` and has no `hypopg`. `CREATE EXTENSION` passes the gateway. | `internal/tools` `TestToolsNameTheMissingExtension` drops both extensions through the gateway, expects each tool to name the missing one, and creates them again. |

## Not measured

Gateway reads routed to replicas, three cells, a killed node, a real migration, a real consensus rule change, and Docker setups (such as Docker Desktop) where the container address is not reachable from the host.
