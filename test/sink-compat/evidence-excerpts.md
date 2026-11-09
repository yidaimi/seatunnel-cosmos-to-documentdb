# Evidence excerpts: MongoDB sink against Amazon DocumentDB

Raw log excerpts behind [compatibility-matrix.md](compatibility-matrix.md). Every block below
is copied from the SeaTunnel job log of that test case; timestamps, thread names and stack
frames are removed, and credentials, user name, cluster/instance endpoints, cluster id and
private IPs are replaced by placeholders. The test inputs are defined in
[run-matrix.sh](run-matrix.sh).

- Engine time = SeaTunnel `Total Time(s)` (job execution only, excludes ~4 s start-up).
- All jobs: SeaTunnel 3.0.0 `-m local`, `parallelism = 1`, `job.retry.times = 0`,
  `connector-mongodb-3.0.0.jar` (Mongo Java driver 4.7.1), Amazon DocumentDB 5.0.2.
- `JVM trust` = `-Djavax.net.ssl.trustStore=<pkcs12 built from global-bundle.pem>`.

## 1. Trust construction

### T01 - TLS, CA only via URI tlsCAFile, default JVM trust

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=~/global-bundle.pem&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t01"
}
```

**JVM trust:** no

**Result:** `FAILED`

```text
com.mongodb.MongoSocketWriteException: Exception sending message
javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
```

### T02 - TLS, URI tlsAllowInvalidCertificates=true (trust-all attempt)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t02"
}
```

**JVM trust:** no

**Result:** `FAILED`

```text
com.mongodb.MongoSocketWriteException: Exception sending message
javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
```

### T03 - TLS, JVM-wide truststore (-Djavax.net.ssl.trustStore)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t03"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

### T06 - No TLS (tls=false) against TLS-enabled cluster

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=false&replicaSet=rs0&retryWrites=false&serverSelectionTimeoutMS=15000"
  database = "st_sink_compat"
  collection = "t06"
}
```

**JVM trust:** no

**Result:** `FAILED`

```text
com.mongodb.MongoSocketReadTimeoutException: Timeout while receiving message
com.mongodb.MongoTimeoutException: Timed out after 15000 ms while waiting for a server that matches com.mongodb.client.internal.MongoClientDelegate$1@7436c736. Client view of cluster state is {type=REPLICA_SET, servers=[{address=<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:2701
com.mongodb.MongoSocketReadTimeoutException: Timeout while receiving message}, caused by {java.net.SocketTimeoutException: Read timed out}}]
```

## 2. Endpoint validation

### T04 - Endpoint by IP, directConnection (hostname check)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<instance-private-ip>:27017/?tls=true&directConnection=true&retryWrites=false"
  database = "st_sink_compat"
  collection = "t04"
}
```

**JVM trust:** yes

**Result:** `FAILED`

```text
com.mongodb.MongoSocketWriteException: Exception sending message
javax.net.ssl.SSLHandshakeException: No subject alternative names matching IP address <instance-private-ip> found
java.security.cert.CertificateException: No subject alternative names matching IP address <instance-private-ip> found
```

### T05 - Endpoint by IP + tlsAllowInvalidHostnames=true

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<instance-private-ip>:27017/?tls=true&directConnection=true&tlsAllowInvalidHostnames=true&retryWrites=false"
  database = "st_sink_compat"
  collection = "t05"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

## 3. Authentication

### T07 - Wrong password

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t07"
}
```

**JVM trust:** yes

**Result:** `FAILED`

```text
com.mongodb.MongoSecurityException: Exception authenticating MongoCredential{mechanism=SCRAM-SHA-256, userName='<user>', source='admin', password=<hidden>, mechanismProperties=<hidden>}
com.mongodb.MongoCommandException: Command failed with error 18: 'Authentication failed.' on server <instance>.<id>.us-west-2.docdb.amazonaws.com:27017. The full response is {"ok": 0, "errmsg": "Authentication failed.", "code": 18, "operationTime": {"$timestamp": {"t": 1793671156, "i":
com.mongodb.MongoInterruptedException: Interrupted while waiting for a server that matches com.mongodb.client.internal.MongoClientDelegate$1@684e882f
```

### T08 - authMechanism=SCRAM-SHA-1

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false&authMechanism=SCRAM-SHA-1"
  database = "st_sink_compat"
  collection = "t08"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

### T09 - authMechanism=SCRAM-SHA-256

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false&authMechanism=SCRAM-SHA-256"
  database = "st_sink_compat"
  collection = "t09"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

## 4. Write semantics

### T10 - retryWrites not set (driver default true)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0"
  database = "st_sink_compat"
  collection = "t10"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

### X5j - Java driver 4.7.1, `retryWrites=true` explicit in URI

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=true"
  database = "st_sink_compat"
  collection = "x5j"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

### X5k - Java driver 4.7.1, `retryWrites` not set

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0"
  database = "st_sink_compat"
  collection = "x5k"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

### T11 - CDC rows, upsert-enable=true, primary-key=[id]

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t11"
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 7 / written 7, engine time 2 s

**Target state:** `{1:a2, 3:c, 4:d}` (expected). Field types of id=1: `id=Long, amount=Decimal128, ts=Date, flag=Boolean, tags=Array, attrs=Object, name=String`

### T12 - CDC rows, upsert-enable=false (insert/update/delete)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t12"
  upsert-enable = false
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 7 / written 7, engine time 2 s

**Target state:** `{1:a2, 3:c, 4:d}` (expected)

### T13 - transaction=true, upsert, CDC rows

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t13"
  transaction = true
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FAILED`

```text
java.lang.NullPointerException: Cannot invoke "org.bson.BsonDocument.get(Object)" because "document" is null
```

### X2 - Control: local MongoDB 7 replica set, `transaction=true` + CDC rows (same as T13)

```hocon
MongoDB {
  uri = "mongodb://127.0.0.1:27117/?directConnection=true"
  database = "st_sink_compat"
  collection = "x2"
  transaction = true
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** no

**Result:** `FAILED`

```text
java.lang.NullPointerException: Cannot invoke "org.bson.BsonDocument.get(Object)" because "document" is null
```

### T14 - data_save_mode=DROP_DATA on pre-populated collection

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t14"
  data_save_mode = "DROP_DATA"
}
```

**JVM trust:** yes

**Result:** `FAILED`

**Target state:** old documents removed; rows 1,2,3 written; job then failed on the update row because no `primary-key` was configured (test design), see X4

```text
java.lang.NullPointerException: Cannot read the array length because "array" is null
java.lang.NullPointerException: Cannot read the array length because "array" is null]
```

### X4 - T14 redo: `data_save_mode=DROP_DATA`, insert-only rows

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t14b"
  data_save_mode = "DROP_DATA"
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3 / written 3, engine time 2 s

**Target state:** old documents removed; `{1:a, 2:b, 3:c}` written

### T15 - data_save_mode=ERROR_WHEN_DATA_EXISTS on pre-populated collection

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "t15"
  data_save_mode = "ERROR_WHEN_DATA_EXISTS"
}
```

**JVM trust:** yes

**Result:** `not submitted (client-side error)`

**Target state:** pre-existing document untouched (`old1`)

```text
org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: ErrorCode:[API-09], ErrorDescription:[Handle save mode failed]
org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: ErrorCode:[API-10], ErrorDescription:[The target data source already has data] - The target data source already has data
```

## 5. Throughput

### P01 - 20000 rows, insert (upsert-enable=false)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p01"
  upsert-enable = false
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 20000 / written 20000, engine time 56 s

### P01b - 20000 rows, insert, `buffer-flush.max-rows = 1000` set explicitly

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p01b"
  buffer-flush.max-rows = 1000
  buffer-flush.interval = 1000
  upsert-enable = false
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 20000 / written 20000, engine time 3 s

### X1a - Control: local MongoDB 7 standalone, 20000 inserts, default buffer options

```hocon
MongoDB {
  uri = "mongodb://127.0.0.1:27118/"
  database = "st_sink_compat"
  collection = "x1a"
  upsert-enable = false
}
```

**JVM trust:** no

**Result:** `FINISHED`, read 20000 / written 20000, engine time 5 s

### P02 - 20000 rows, upsert by id, target index on id

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p02"
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 20000 / written 20000, engine time 63 s

### P02b - 20000 rows, upsert by id (indexed), `buffer-flush.max-rows = 1000` set explicitly

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p02b"
  buffer-flush.max-rows = 1000
  buffer-flush.interval = 1000
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 20000 / written 20000, engine time 49 s

### X1b - Control: local MongoDB 7 standalone, 20000 upserts (index on id), default buffer options

```hocon
MongoDB {
  uri = "mongodb://127.0.0.1:27118/"
  database = "st_sink_compat"
  collection = "x1b"
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** no

**Result:** `FINISHED`, read 20000 / written 20000, engine time 6 s

### P03 - 3000 rows, upsert by id, NO index on id

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p03"
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 3000 / written 3000, engine time 15 s

### P04 - 30000 rows, `transaction=true`, upsert (one transaction per 1,024-document bulk)

```hocon
MongoDB {
  uri = "mongodb://<user>:<password>@<cluster>.cluster-<id>.us-west-2.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0&retryWrites=false"
  database = "st_sink_compat"
  collection = "p04"
  transaction = true
  upsert-enable = true
  primary-key = ["id"]
}
```

**JVM trust:** yes

**Result:** `FINISHED`, read 30000 / written 30000, engine time 460 s

**Target state:** 30 transactions committed, at most 1,024 documents each (27 x 1024, 2 x 1023, 1 x 304 newly upserted; the two 1023 bulks each contained one random id that already existed); 29,998 distinct ids

```text
INFO  [MongodbSinkAggregatedCommitter] [hz.main.generic-operation.thread-41] - Inserted 1024 documents into collection st_sink_compat.p04.
INFO  [MongodbSinkAggregatedCommitter] [hz.main.generic-operation.thread-41] - Inserted 1024 documents into collection st_sink_compat.p04.
...
```

## 6. Pipeline restart duplicates inserts (insert mode)

First run of T03 with CDC rows, no `primary-key` and the default `job.retry.times` (3).
The update row fails with an NPE (`MongoKeyExtractor`), Zeta restores the pipeline and
the already written inserts are applied again:

```text
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Restore time 1, pipeline Job SeaTunnel_Job (1169816945067819009), Pipeline: [(1/1)]
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Wait 3s and then restore the pipeline Job SeaTunnel_Job (1169816945067819009), Pipeline: [(1/1)]
INFO  [o.a.s.e.s.d.p.SubPlan         ] [seatunnel-coordinator-service-8] - Restore time 2, pipeline Job SeaTunnel_Job (1169816945067819009), Pipeline: [(1/1)]
```

Target collection afterwards (one entry per document): `[1:a, 1:a, 1:a, 1:a, 2:b, 2:b, 2:b, 2:b, 3:c, 3:c, 3:c, 3:c]` - each insert applied 4 times.

## 7. Direct measurements with mongosh (same cluster)

Retryable writes with the Node.js driver (mongosh 2.x, `retryWrites=true`):

```text
ERROR 301: Retryable writes are not supported
```

`hello` on the cluster: `logicalSessionTimeoutMinutes: 30, maxWireVersion: 13, setName: 'rs0'`.

Bulk insert, 1,000 documents per call, three rounds:

```text
insertMany ordered:true                    57 ms ->  17544 docs/s
insertMany ordered:false                   35 ms ->  28571 docs/s
bulkWrite insertOne x1000 ordered:true     53 ms ->  18868 docs/s
insertMany ordered:true                    49 ms ->  20408 docs/s
insertMany ordered:false                   44 ms ->  22727 docs/s
bulkWrite insertOne x1000 ordered:true     30 ms ->  33333 docs/s
insertMany ordered:true                    29 ms ->  34483 docs/s
insertMany ordered:false                   28 ms ->  35714 docs/s
bulkWrite insertOne x1000 ordered:true     28 ms ->  35714 docs/s
```

Single-document statements, 1,000 operations per call:

```text
replaceOne(upsert), new documents       27236 ms / 10000 ops ->  367 ops/s
replaceOne(upsert), existing documents  20641 ms / 10000 ops ->  484 ops/s
updateOne(upsert), new documents         2408 ms /  1000 ops ->  415 ops/s
replaceOne (no upsert), existing         1975 ms /  1000 ops ->  506 ops/s
deleteOne, existing                      2211 ms /  1000 ops ->  452 ops/s
deleteMany({_id:{$in:1000}}) + insertMany 1000     58 ms   -> 17241 docs/s
```

## 8. Source references for the buffer default bug (unchanged on `dev`)

`MongodbSinkFactory` applies the buffer options only when they are present:

```java
if (readonlyConfig.getOptional(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS).isPresent()) {
    builder.withFlushSize(readonlyConfig.get(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS));
}
```

`MongodbWriterOptions.Builder` declares `protected int flushSize;` (Java default `0`), while
`MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS` documents `defaultValue(1000)`. With `flushSize = 0`,
`MongodbWriter.isOverMaxBatchSizeLimit()` returns `bulkActions != -1 && bulkRequests.size() >= 0`,
which is always true, so `doBulkWrite()` runs after every row.

