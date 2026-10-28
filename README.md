# connector-cosmos-mongodb

Apache SeaTunnel 3.0.0 插件：把 **Azure Cosmos DB for MongoDB** 同步到 **Amazon DocumentDB**，支持全量 + 基于 change stream 的增量同步。

| 插件 | 类型 | 作用 |
|---|---|---|
| `CosmosMongo` | Source | 全量快照（按 `_id` 排序，可断点续扫）+ change stream 增量 |
| `DocumentDB`  | Sink   | 默认按批次执行 `deleteMany($in)` + `insertMany`（可选逐条 `replaceOne(upsert)` / `deleteOne`），幂等写入 |

## 工作原理

- **一个集合 = 一个 split**，一个集合始终只由一个 reader 读取，所以同一文档的变更顺序在整条链路上都能保持。
- **initial 模式**：先打开 change stream 拿到当前位点（resume token），再做全量；全量结束后从该位点追增量。
  全量期间发生的变更会在之后回放。sink 写的是整文档 upsert，回放是幂等的，最终会和源端一致。
- **checkpoint 状态**：每个集合保存 `阶段 / 最后一个全量 _id / resume token`。
  停机或 savepoint 恢复后，全量从断点 `_id` 之后继续扫，增量从 token 之后继续读。
- **增量读取**：每个集合一个后台线程读 change stream 并放进有界队列，空闲集合的阻塞等待不会拖慢其他集合。
- **数据格式**：文档以 canonical Extended JSON 在管道中传递，ObjectId / Decimal128 / Long / Date / Binary 等类型全部无损。
  行结构固定为 `database, collection, id, document` 四列，所以一个作业可以同步整个库，不用为每个集合定义 schema。
- **投递语义**：at-least-once。sink 在每次 checkpoint 前强制 flush，写入都是幂等的，所以结果等价于 exactly-once。

## Cosmos DB 限制（重要）

`change_stream.mode = cosmos_ru`（RU 账号）按微软文档要求，固定使用
`$match(operationType in insert/update/replace) + $project(_id, fullDocument, ns, documentKey)` 和 `fullDocument: updateLookup`：

- **Cosmos RU 的 change stream 不产生 delete 事件**，所以增量阶段源端的删除**不会**同步到 DocumentDB。
  可选做法：业务上改成软删除（设置 `deleted=true` 再配 TTL），或者切换前跑一次 `startup.mode = snapshot` 加对账。
- 因为不返回 `operationType`，所有事件都按 upsert 处理。
- vCore 账号或原生 MongoDB 请用 `change_stream.mode = standard`，delete 会同步。

实测 Cosmos RU（API 7.0）的其他行为，插件已针对这些行为做了处理：

- 同一批次内的事件**共用一个 resume token**，指向整批之后的位置；批次读完后 cursor 返回的 token 反而是批次之前的位置。
  插件只在整批事件都入队之后才推进 checkpoint 位点，并且在 `cosmos_ru` 模式下不使用 cursor 级别的 token。
- 同一文档的多次修改可能合并成一个事件，只保留最新版本，不影响整文档 upsert。
- 跨类型的 `_id` 排序和比较规则和 MongoDB 不同（例如字符串排在 ObjectId 后面，`$gt <数字>` 不会匹配其他类型）。
  全量续扫只依赖同类型之间的比较；混合类型的集合恢复时，其他类型的文档会被重读一遍（写入是幂等的）。

## 编译

```bash
mvn -B package          # 需要 JDK 8+ / Maven 3.6+，产物 target/connector-cosmos-mongodb-3.0.0.jar
```

mongodb-driver 4.11.5 已经 shade 并 relocate 到 `org.apache.seatunnel.connectors.seatunnel.cosmosmongo.shaded`，
不会和其他 connector（例如自带 MongoDB connector 用的 4.7.1）冲突。

## 安装到 SeaTunnel

```bash
ST=/path/to/apache-seatunnel-3.0.0
cp target/connector-cosmos-mongodb-3.0.0.jar $ST/connectors/
cat >> $ST/connectors/plugin-mapping.properties <<'EOF'
seatunnel.source.CosmosMongo = connector-cosmos-mongodb
seatunnel.sink.DocumentDB = connector-cosmos-mongodb
EOF
# 集群模式下更新 jar 后需要重启集群（classloader-cache-mode）
```

## 运行

```bash
cp config/cosmos-to-documentdb.conf.template my-job.conf   # 填写连接信息
# 集群模式（支持 savepoint / 断点恢复）
$ST/bin/seatunnel-cluster.sh -d
$ST/bin/seatunnel.sh --config my-job.conf --async          # 返回 job id
$ST/bin/seatunnel.sh -s <jobId>                            # savepoint 并停止
$ST/bin/seatunnel.sh --config my-job.conf -r <jobId> --async   # 从 savepoint 恢复
# 只做一次性全量：startup.mode = "snapshot" + job.mode = "BATCH"，可用 -m local 运行
```

## Source 参数（CosmosMongo）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `uri` | 必填 | Cosmos DB 连接串；未显式设置 `retryWrites` 时自动设为 `false` |
| `database` | 必填 | 源库 |
| `collections` | 全部 | 要同步的集合列表，省略时同步所有非 `system.` 集合 |
| `startup.mode` | `initial` | `initial` 全量+增量 / `snapshot` 只全量（BATCH）/ `incremental` 只增量（从当前时间开始） |
| `change_stream.mode` | `cosmos_ru` | `cosmos_ru` 或 `standard` |
| `snapshot.fetch_size` | 1000 | 全量游标 batchSize |
| `poll.batch_size` | 1024 | 每个集合每轮最多发出的条数，两轮之间可以做 checkpoint |
| `change_stream.batch_size` | 1000 | change stream 游标 batchSize |
| `change_stream.max_await_ms` | 500 | getMore 的 maxAwaitTimeMS |
| `change_stream.queue_size` | 8192 | 每个集合预读事件的队列长度 |
| `change_stream.idle_sleep_ms` | 50 | 所有集合都没有新数据时 reader 的休眠时间 |
| `change_stream.token_wait_ms` | 10000 | 全量开始前等待初始 resume token 的最长时间 |
| `max_retries` / `retry_backoff_ms` | 10 / 1000 | 读失败（16500 限流、网络等）重试次数和初始退避时间（指数退避，最长 30s） |
| `tls_ca_file` | - | 可选，PEM 格式 CA 证书 |

## Sink 参数（DocumentDB）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `uri` | 必填 | DocumentDB 连接串（`tls=true&retryWrites=false`） |
| `database` | 源库名 | 目标库，省略时沿用每行的源库名 |
| `tls_ca_file` | - | DocumentDB CA 证书，例如 `global-bundle.pem`（Java driver 会忽略 URI 里的 `tlsCAFile`） |
| `write_mode` | `delete_insert` | `delete_insert`：每批每个 `_id` 只保留最终状态，再执行 `deleteMany($in)` + `insertMany`，在 DocumentDB 上比逐条写快约 40 倍，但同一批文档在两步之间会短暂查不到；`upsert`：逐条 `replaceOne(upsert)` / `deleteOne`，目标端不会出现文档短暂缺失，但在 DocumentDB 上只有约 400 条/秒 |
| `batch_size` | 1000 | 缓存多少条写操作后触发一次 bulkWrite |
| `batch_interval_ms` | 1000 | 缓存写操作的最长停留时间；每次 checkpoint 也会 flush |
| `max_retries` / `retry_backoff_ms` | 5 / 1000 | bulkWrite 失败重试（单文档错误比如超大文档、唯一索引冲突，不重试直接报错） |

## 本地测试

`test/` 目录下是用两个本地 MongoDB 7 容器（源端是副本集）做的端到端测试：

```bash
test/run-cluster-test.sh cosmos_ru false   # Cosmos 兼容 pipeline，无删除
test/run-cluster-test.sh standard  true    # 标准 change stream，包含删除
```

脚本流程：重置数据 → 提交作业 → 并发写入 → 全量期间 savepoint 停止 → 停机期间继续写 → 恢复 → 逐文档比对 → 增量吞吐测试。

## 已知限制

- 单个集合的全量是单线程按 `_id` 顺序扫描，超大集合不会在集合内部再拆分并行。
- 不同步索引、集合选项和 TTL 配置，请提前在 DocumentDB 上建好。
- 增量阶段集合被 drop 或 rename 时作业会报错停止（standard 模式），不会静默跳过。
