# Existing MongoDB sink against a real Amazon DocumentDB TLS endpoint

Evidence for the "Amazon DocumentDB sink" row of apache/seatunnel#10443.

## Environment

| Item | Value |
|---|---|
| Target | Amazon DocumentDB 5.0.2, instance-based, 1 x `db.r5.large`, us-west-2, TLS required |
| CA | RDS global bundle `global-bundle.pem` (108 certificates) |
| Sink | `connector-mongodb-3.0.0.jar` from Maven Central (SHA-1 verified), Mongo Java driver **4.7.1** |
| Engine | SeaTunnel 3.0.0 (Zeta), `-m local`, `parallelism = 1`, `job.retry.times = 0` |
| Client | EC2 in the same VPC, OpenJDK 17 (Corretto) |
| Source | `FakeSource` (fixed CDC rows for semantics, random rows for throughput) |

The sink connection and write path is unchanged between `v3.0.0` and current `dev`: the only
commits touching `connector-mongodb` since the release are #12687 / #12399 (dry-run validators)
and #12600 (null session guard in committer `close`). `dev` still calls
`MongoClients.create(connectionString)` with driver 4.7.1.

Script: [run-matrix.sh](run-matrix.sh) (credentials via environment, nothing stored).
Log excerpts for every case (sanitized): [evidence-excerpts.md](evidence-excerpts.md).

## 1. Trust construction

| # | Configuration | Result | Root cause |
|---|---|---|---|
| T01 | `tls=true&tlsCAFile=<bundle>` in URI, default JVM trust | FAILED | `SSLHandshakeException: PKIX path building failed`. The Java driver ignores `tlsCAFile`; the JDK `cacerts` contains Amazon Root CA 1-4 but no RDS CA |
| T02 | `tls=true&tlsAllowInvalidCertificates=true` | FAILED | Same PKIX error: the option is ignored as well, so there is no URI-level trust-all path |
| T03 | JVM-wide `-Djavax.net.ssl.trustStore=<pkcs12 from bundle>` | OK | Works, but the truststore is process-global: it applies to every job and connector in the JVM |
| T06 | `tls=false` | FAILED | `MongoSocketReadTimeoutException` / server selection timeout (cluster requires TLS) |

**The only way to connect today is a JVM-global truststore.** There is no per-job trust option.

## 2. Endpoint validation

| # | Configuration | Result | Root cause |
|---|---|---|---|
| T03 | Cluster endpoint DNS name, `replicaSet=rs0` | OK | Certificate SANs cover the cluster and discovered instance endpoints |
| T04 | Instance IP, `directConnection=true` | FAILED | `CertificateException: No subject alternative names matching IP address ...` |
| T05 | Instance IP + `tlsAllowInvalidHostnames=true` | OK | Hostname check disabled (honoured by the driver, unlike `tlsAllowInvalidCertificates`) |

## 3. Authentication

| # | Configuration | Result | Notes |
|---|---|---|---|
| T07 | Wrong password | FAILED | Error 18 `Authentication failed`. Driver negotiated **SCRAM-SHA-256** by default |
| T08 | `authMechanism=SCRAM-SHA-1` | OK | |
| T09 | `authMechanism=SCRAM-SHA-256` | OK | |

## 4. Write semantics

| # | Configuration | Result | Target state / notes |
|---|---|---|---|
| T10 | `retryWrites` not set (driver default) | OK | See note below |
| T11 | CDC rows (insert 1,2,3; update 1; delete 2; insert 4), `upsert-enable=true`, `primary-key=[id]` | OK | `{1:a2, 3:c, 4:d}` - correct. Types preserved: Long, Decimal128, Date, Boolean, Array, embedded document |
| T12 | Same rows, `upsert-enable=false`, `primary-key=[id]` | OK | `{1:a2, 3:c, 4:d}` - correct |
| T13 | Same rows, `transaction=true`, upsert | FAILED | `NullPointerException: ... "document" is null` in `CommittableUpsertTransaction` (DELETE rows carry no `$set`). **Reproduced identically on a MongoDB 7 replica set** -> generic sink bug, not DocumentDB |
| T14 | `data_save_mode=DROP_DATA` on pre-populated collection | OK | Old documents removed, new rows written |
| T15 | `data_save_mode=ERROR_WHEN_DATA_EXISTS` on pre-populated collection | Failed as designed | `API-10 The target data source already has data` |

**retryWrites note.** AWS documents that engine versions before 8.0.2 reject retryable writes
(error 301). Observed on this 5.0.2 cluster:

| Client | `retryWrites` | Result |
|---|---|---|
| mongosh (Node.js driver) | `true` | error 301 `Retryable writes are not supported` |
| SeaTunnel sink (Java driver 4.7.1) | not set / `true` | **no error**, writes applied |

The server advertises `logicalSessionTimeoutMinutes: 30`. Why the Java 4.7.1 path does not hit
301 was not determined; recommend documenting `retryWrites=false` as the DocumentDB source
already enforces it.

## 5. Throughput (20,000 rows, `db.r5.large`, same VPC)

Wall-clock time per job, including about 4 s of job start-up.

| # | Configuration | DocumentDB | Local MongoDB 7 (control) |
|---|---|---|---|
| P01 | insert, default buffer options | **59.4 s** (~360 rows/s) | 7.9 s |
| P01b | insert, `buffer-flush.max-rows = 1000` set explicitly | **6.1 s** | - |
| P02 | upsert by `id` (indexed), default buffer options | 66.2 s | 9.5 s |
| P02b | upsert by `id` (indexed), `buffer-flush.max-rows = 1000` | 51.9 s | - |
| P03 | upsert by `id`, **no index** on `id` (3,000 rows) | 18.1 s | - |
| P04 | `transaction=true`, upsert (30,000 rows) | 462.9 s (~65 rows/s), 30 transactions of at most 1,024 docs, none aborted | - |

Findings behind these numbers:

1. **Defaults of `buffer-flush.max-rows` / `buffer-flush.interval` are not applied (bug, also on
   `dev`).** `MongodbSinkFactory` only calls `withFlushSize` / `withBatchIntervalMs` when the
   option is explicitly present, so `MongodbWriterOptions.Builder` keeps `flushSize = 0` and
   `MongodbWriter.isOverMaxBatchSizeLimit()` (`size >= 0`) is always true: **every row is a
   separate `bulkWrite` round trip**. Invisible against a local server, ~10x slower against a
   managed endpoint. Setting the option explicitly fixes inserts (P01 -> P01b).
2. **Per-document upserts are server-bound on DocumentDB.** Batching only helps ~20% (P02 ->
   P02b). Measured directly with mongosh on the same cluster, 1,000 operations per call:
   `replaceOne(upsert)` ~370-480 ops/s, `updateOne(upsert)` ~415, `deleteOne` ~452, while
   `insertMany` (ordered or unordered) reaches ~17k-35k docs/s and
   `deleteMany({_id: {$in}})` + `insertMany` ~17k-18k docs/s. Ordered vs unordered makes no
   material difference for inserts.
3. **Transactions**: the sink commits one transaction per `DocumentBulk` (max 1,024 documents).
   DocumentDB limits a transaction to 1 minute and a 32 MB log, so aborts only occur when 1,024
   operations exceed 60 s (e.g. upserts on an unindexed key over a large collection).
   Transaction mode was ~5x slower than non-transactional upserts here.

## 6. What the DocumentDB source does differently

`AmazonDocumentDBConfig.createMongoClientSettings()` on `dev`:

- `MongoClientSettings.builder().applyConnectionString(uri).retryWrites(false)` - retryable
  writes forced off, and `retryWrites=true` in the URI is rejected with a clear error;
- `applyToSslSettings(enabled(tls).context(SSLContext))` where the `SSLContext` is built from
  the job's `tls_ca_file` PEM (`CertificateFactory` -> `KeyStore` -> `TrustManagerFactory`) -
  per job, without touching JVM-global trust.

The MongoDB sink uses `MongoClients.create(connectionString)` and has no hook for either.

## 7. Where the gaps sit

| Gap | Category | DocumentDB-specific? |
|---|---|---|
| No per-job CA / SSLContext (T01-T03) | Trust construction | Yes in practice (RDS CA not in JDK trust) |
| `retryWrites` not forced off | Write semantics / config | Yes (documented; not triggered by Java 4.7.1 here) |
| Buffer defaults not applied -> one round trip per row | Performance bug | No - generic, amplified by network latency |
| Per-document upsert/delete ~400 ops/s | Write semantics / performance | Yes - server-side cost |
| `transaction=true` + DELETE rows -> NPE | Bug | No - reproduced on MongoDB 7 |
| `upsert-enable=true` without `primary-key` -> NPE at write time instead of a config error | Validation | No |
| Update/delete rows without `primary-key` -> NPE (`MongoKeyExtractor`) | Validation | No |
| Insert mode + Zeta pipeline restart (`job.retry.times`, default 3) -> duplicated documents | Semantics | No |

Other observation: the SeaTunnel client prints the full job config at start-up, including the
`uri` with the password in clear text (only keys named like `password` are masked).

## 8. Suggested scope for a MongoDB sink extension

Smallest backward-compatible slice, matching the evidence above:

1. Optional `tls_ca_file` (and explicit hostname-verification semantics) building a per-job
   `SSLContext`, same approach as the DocumentDB source; no trust-all, no new dependency.
2. Apply the documented defaults for `buffer-flush.max-rows` / `buffer-flush.interval`
   (separate bug-fix PR).

Possible follow-up, to be discussed separately: an opt-in batched write mode for full-document
rows (per batch `deleteMany({key: {$in}})` + `insertMany`), which measured ~40x faster than
per-document upserts on DocumentDB, with the trade-off that documents of a batch are briefly
absent between the two statements.
