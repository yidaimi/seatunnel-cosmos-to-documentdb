# Amazon DocumentDB source connector: real-server validation

Requested in [apache/seatunnel#10443](https://github.com/apache/seatunnel/issues/10443): run the
merged Amazon DocumentDB source connector (`connector-amazondocumentdb`,
[apache/seatunnel#12046](https://github.com/apache/seatunnel/pull/12046)) against a live
Amazon DocumentDB endpoint (read path, `tls_ca_file`, `retryWrites(false)`) and publish the
config and log excerpts. Credentials, the cluster endpoint and IP addresses are replaced by
placeholders.

## Environment

| Item | Value |
|---|---|
| Target | Amazon DocumentDB 5.0.2, instance-based cluster (db.r5.large), TLS enabled, us-west-2 |
| Client | EC2 in the same VPC, Amazon Linux 2023 (aarch64), OpenJDK 17.0.16, time zone UTC |
| SeaTunnel | built from apache/seatunnel `dev` at `928334411`, Zeta local mode (`-m local`); last connector change `36401ab4b` |
| CA | Amazon RDS `global-bundle.pem`, configured only through the connector's `tls_ca_file` |
| JVM trust | no `javax.net.ssl.*` option in `JAVA_OPTS`, `config/jvm_*` or `bin/` (the JDK trust store does not contain the RDS CA) |
| Data | `st_source_compat.orders`, 10 documents: ObjectId `_id`, Int64, Int32, Double, Decimal128, Date, Boolean, array, embedded document, one explicit `null`, one document without `price`, one document with an extra field not in the schema |
| Sinks | `Assert` (exact row count, `NOT_NULL` rules) and `Console` |

Reproduction: [`run-source-matrix.sh`](run-source-matrix.sh) (credentials from environment variables).

## Results

| Case | Configuration | Expected | Result | Evidence |
|---|---|---|---|---|
| S01 | `tls_ca_file` = RDS bundle, `retryWrites` not in the URI (driver default would be `true`) | read all rows | **FINISHED**, 10 read, 0 failed; all types converted correctly | job state, counts, rows |
| S02 | S01 + `match.query = {"status": "OPEN"}` + `match.projection` | 4 rows, only projected fields | **FINISHED**, 4 read; non-projected fields `null` | rows |
| S03 | `tls_ca_file` = an unrelated CA | TLS failure | **FAILED**: `SSLHandshakeException: PKIX path building failed` | exception |
| S04 | `tls = true` (default) without `tls_ca_file` | configuration error | **Rejected before job submission**: `option 'tls_ca_file' is required when TLS is enabled` | exception |
| S05 | URI `retryWrites=true` | configuration error | **Rejected before job submission**: `AmazonDocumentDB does not support retryable writes` | exception |
| S06 | option `tls = false`, URI `tls=true` | option wins; plain connection to a TLS-only cluster fails | **FAILED**: `SocketTimeoutException: Read timed out` -> `MongoTimeoutException` | exception |
| S07 | endpoint given as instance IP, `directConnection=true` | hostname verification failure | **FAILED**: `SSLHandshakeException: No subject alternative names matching IP address <instance-ip> found` | exception |
| S08 | S07 + `tlsAllowInvalidHostnames=true` | explicit opt-out is honored | **FINISHED**, 10 read | job state, counts |

Conclusions:

1. **Read path works against a real cluster.** A bounded scan of the whole collection, and with a
   BSON filter and projection, returns the expected rows. Int64, Int32, Double, Decimal128 (scale
   kept, e.g. `41.00`), Date (`TIMESTAMP`; the host runs in UTC, so time zone handling was not exercised), Boolean, arrays and embedded documents
   (`MAP<STRING,STRING>`) are converted correctly. An explicit `null` and a missing field both
   become `null`, and a field not in the schema is ignored.
2. **`tls_ca_file` is applied per job.** S01 succeeds without any JVM-wide trust setting, so the
   RDS CA came only from `tls_ca_file`. S03 shows that only the configured CA is trusted, and S07
   shows that hostname verification stays enabled. S08 shows that it can still be turned off
   explicitly with `tlsAllowInvalidHostnames=true` in the URI.
3. **`retryWrites(false)` is enforced.** S01 passes without `retryWrites` in the URI, and S05 shows
   that an explicit `retryWrites=true` is rejected at configuration time. (The source only reads,
   so this mainly protects against copied URIs.)
4. **Configuration errors fail fast.** S04 and S05 are reported by the client before the job is
   submitted, without network access. The messages name the option and do not contain the URI.
5. The `tls` option overrides the URI (S06), as documented.

Not covered: stream mode and change streams (out of scope for V1 of the connector), parallel
splits (V1 uses one split) and large collections.

The `java.net.BindException: Address already in use` lines in the logs come from the local-mode
HTTP port, which was already in use on the test host. They do not affect the jobs.

## Config and log excerpts

Each excerpt shows the `source` block of the job config, followed by lines copied from the
SeaTunnel job log. All jobs use `env { parallelism = 1, job.mode = "BATCH", job.retry.times = 0 }`
and the sinks described above.

#### S01

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?replicaSet=rs0&readPreference=secondaryPreferred"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/global-bundle.pem"
    schema = { fields {
      _id = string, order_no = bigint, status = string, qty = int, price = double,
      amount = "decimal(10, 2)", created_at = timestamp, paid = boolean,
      tags = "array<string>", address = "map<string, string>", note = string } }
  }
```

```text
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Job SeaTunnel_Job (1173523781763727361), Pipeline: [(1/1)] will end with state FINISHED
INFO  [o.a.s.e.c.j.ClientJobProxy    ] [main] - Job (1173523781763727361) end with state FINISHED
Total Read Count          :                  10
Total Failed Count        :                   0
```

#### S02

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/global-bundle.pem"
    match.query = "{\"status\": \"OPEN\"}"
    match.projection = "{\"order_no\": 1, \"status\": 1, \"amount\": 1}"
    schema = { ... }  # see S01
  }
```

```text
INFO  [.a.s.c.s.c.s.ConsoleSinkWriter] [st-multi-table-sink-writer-1] - subtaskIndex=0  rowIndex=1:  SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : 6af6b9b7f994d34bf6300589, 1001, OPEN, , , 10.25, , , , ,
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Job SeaTunnel_Job (1173523799266557953), Pipeline: [(1/1)] will end with state FINISHED
INFO  [o.a.s.e.c.j.ClientJobProxy    ] [main] - Job (1173523799266557953) end with state FINISHED
Total Read Count          :                   4
```

#### S03

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?replicaSet=rs0&serverSelectionTimeoutMS=15000"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/untrusted-ca.pem"
    schema = { ... }  # see S01
  }
```

```text
Caused by: javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
INFO  [o.m.d.cluster                 ] [BlockingWorker-TaskGroupLocation{jobId=1173523816270266369, pipelineId=1, taskGroupId=2}] - No server chosen by com.mongodb.client.internal.MongoClientDelegate$1@… from cluster description ClusterDescription{type=REPLICA_SET, connectionMode=MULTIPLE, serverDescriptions=[ServerDescription{address=<cluster-endpoint>:27017, type=UNKNOWN, state=CONNECTING, exception={com.mongodb.Mo
Caused by: com.mongodb.MongoTimeoutException: Timed out after 15000 ms while waiting for a server that matches com.mongodb.client.internal.MongoClientDelegate$1@…. Client view of cluster state is {type=REPLICA_SET, servers=[{address=<cluster-endpoint>:27017, type=UNKNOWN, state=CONNECTING, exception={com.mongodb.MongoSocketWriteException: Exception sending message}, caused by {javax.net.ssl.SSLHandshakeException: PKI
ERROR [o.a.s.e.s.d.p.PhysicalVertex  ] [hz.main.seaTunnel.task.thread-2] - Job (1173523816270266369), Pipeline: [(1/1)], task: [pipeline-1 [Source[0]-AmazonDocumentDB]-SourceTask (1/1)], taskGroupLocation: [TaskGroupLocation{jobId=1173523816270266369, pipelineId=1, taskGroupId=2}] end with state FAILED and Exception: java.lang.IllegalStateException: Failed to read AmazonDocumentDB data from database [st_source_compat
```

#### S04

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?replicaSet=rs0"
    database = "st_source_compat"
    collection = "orders"
    
    schema = { ... }  # see S01
  }
```

```text
Caused by: java.lang.IllegalArgumentException: AmazonDocumentDB option 'tls_ca_file' is required when TLS is enabled
```

#### S05

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?replicaSet=rs0&retryWrites=true"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/global-bundle.pem"
    schema = { ... }  # see S01
  }
```

```text
Caused by: java.lang.IllegalArgumentException: AmazonDocumentDB does not support retryable writes; remove 'retryWrites=true' from option 'uri' or set it to false
```

#### S06

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<cluster-endpoint>:27017/?tls=true&replicaSet=rs0&serverSelectionTimeoutMS=15000"
    database = "st_source_compat"
    collection = "orders"
    tls = false
    schema = { ... }  # see S01
  }
```

```text
Caused by: java.net.SocketTimeoutException: Read timed out
Caused by: com.mongodb.MongoTimeoutException: Timed out after 15000 ms while waiting for a server that matches com.mongodb.client.internal.MongoClientDelegate$1@…. Client view of cluster state is {type=REPLICA_SET, servers=[{address=<cluster-endpoint>:27017, type=UNKNOWN, state=CONNECTING, exception={com.mongodb.MongoSocketReadTimeoutException: Timeout while receiving message}, caused by {java.net.SocketTimeoutExcept
ERROR [o.a.s.e.s.d.p.PhysicalVertex  ] [hz.main.seaTunnel.task.thread-2] - Job (1173523920427417601), Pipeline: [(1/1)], task: [pipeline-1 [Source[0]-AmazonDocumentDB]-SourceTask (1/1)], taskGroupLocation: [TaskGroupLocation{jobId=1173523920427417601, pipelineId=1, taskGroupId=2}] end with state FAILED and Exception: java.lang.IllegalStateException: Failed to read AmazonDocumentDB data from database [st_source_compat
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Job SeaTunnel_Job (1173523920427417601), Pipeline: [(1/1)] will end with state FAILED
```

#### S07

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<instance-ip>:27017/?directConnection=true&serverSelectionTimeoutMS=15000"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/global-bundle.pem"
    schema = { ... }  # see S01
  }
```

```text
Caused by: javax.net.ssl.SSLHandshakeException: No subject alternative names matching IP address <instance-ip> found
Caused by: com.mongodb.MongoTimeoutException: Timed out after 15000 ms while waiting to connect. Client view of cluster state is {type=UNKNOWN, servers=[{address=<instance-ip>:27017, type=UNKNOWN, state=CONNECTING, exception={com.mongodb.MongoSocketWriteException: Exception sending message}, caused by {javax.net.ssl.SSLHandshakeException: No subject alternative names matching IP address <instance-ip> found}, caused b
ERROR [o.a.s.e.s.d.p.PhysicalVertex  ] [hz.main.seaTunnel.task.thread-2] - Job (1173524000400211969), Pipeline: [(1/1)], task: [pipeline-1 [Source[0]-AmazonDocumentDB]-SourceTask (1/1)], taskGroupLocation: [TaskGroupLocation{jobId=1173524000400211969, pipelineId=1, taskGroupId=2}] end with state FAILED and Exception: java.lang.IllegalStateException: Failed to read AmazonDocumentDB data from database [st_source_compat
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-9] - Job SeaTunnel_Job (1173524000400211969), Pipeline: [(1/1)] will end with state FAILED
```

#### S08

```hocon
AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://<user>:<password>@<instance-ip>:27017/?directConnection=true&tlsAllowInvalidHostnames=true&serverSelectionTimeoutMS=15000"
    database = "st_source_compat"
    collection = "orders"
    tls_ca_file = "/path/to/global-bundle.pem"
    schema = { ... }  # see S01
  }
```

```text
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-9] - Job SeaTunnel_Job (1173524079156658177), Pipeline: [(1/1)] will end with state FINISHED
INFO  [o.a.s.e.c.j.ClientJobProxy    ] [main] - Job (1173524079156658177) end with state FINISHED
Total Read Count          :                  10
```

#### S01 rows (Console sink)

```text
subtaskIndex=0  rowIndex=1:  SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : 6af6b9b7f994d34bf6300589, 1001, OPEN, 1, 1.5, 10.25, 2026-01-01T08:30, false, [t1, all], {"zip":"91","city":"city1"}, n1
subtaskIndex=0  rowIndex=5:  SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : 6af6b9b7f994d34bf630058d, 1005, CLOSED, 5, 7.5, 51.25, 2026-01-05T08:30, false, [t5, all], {"zip":"95","city":"city5"},
subtaskIndex=0  rowIndex=9:  SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : 6af6b9b7f994d34bf6300591, 1009, CLOSED, 9, , 92.25, 2026-01-09T08:30, false, [t9, all], {"zip":"99","city":"city9"}, n9
subtaskIndex=0  rowIndex=10:  SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : 6af6b9b7f994d34bf6300592, 1010, OPEN, 10, 15.0, 102.50, 2026-01-10T08:30, true, [t10, all], {"zip":"910","city":"city10"}, n10
```
