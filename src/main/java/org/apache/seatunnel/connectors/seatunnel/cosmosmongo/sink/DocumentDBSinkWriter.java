/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.BsonCodec;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.MongoClientFactory;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.WriteMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.WRITE_FAILED;

/**
 * Applies rows to DocumentDB. INSERT / UPDATE_AFTER carry the full document, DELETE removes it,
 * UPDATE_BEFORE is ignored. Rows are buffered per collection in a {@link WriteBatch} and applied
 * according to {@link WriteMode}:
 *
 * <ul>
 *   <li>{@code DELETE_INSERT} (default): per chunk of ids, {@code deleteMany({_id: {$in}})} then
 *       {@code insertMany} of the final documents. On DocumentDB this is about 40x faster than
 *       one statement per document.
 *   <li>{@code UPSERT}: ordered {@code replaceOne(upsert)} / {@code deleteOne} per row.
 * </ul>
 *
 * <p>Both modes are idempotent, so a failed write is simply retried and events replayed after a
 * restore converge to the same result. Buffered writes are flushed on every checkpoint, which
 * gives at-least-once delivery.
 */
@Slf4j
public class DocumentDBSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private static final long MAX_BACKOFF_MS = 30_000L;
    /** Ids per deleteMany / insertMany pair, keeps every command far below the 16 MB limit. */
    private static final int DELETE_INSERT_CHUNK = 1000;

    private static final int DUPLICATE_KEY = 11000;
    private static final BulkWriteOptions ORDERED = new BulkWriteOptions().ordered(true);
    private static final InsertManyOptions UNORDERED = new InsertManyOptions().ordered(false);

    private final SinkSettings settings;
    private final MongoClient client;
    private final Map<MongoNamespace, WriteBatch> buffer = new LinkedHashMap<>();

    private int buffered;
    private long lastFlushMs = System.currentTimeMillis();
    private long written;

    public DocumentDBSinkWriter(SinkWriter.Context context, SinkSettings settings) {
        this.settings = settings;
        this.client =
                MongoClientFactory.create(
                        settings.getUri(),
                        settings.getTlsCaFile(),
                        "seatunnel-documentdb-writer-" + context.getIndexOfSubtask());
        context.registerFlushAction(this::flush);
        log.info(
                "DocumentDB writer {} uses write_mode {}",
                context.getIndexOfSubtask(),
                settings.getWriteMode());
    }

    @Override
    public synchronized void write(SeaTunnelRow row) {
        RowKind kind = row.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER || kind == RowKind.DELETE) {
            String database =
                    settings.getDatabase() != null
                            ? settings.getDatabase()
                            : (String) row.getField(settings.getDatabaseIndex());
            String collection = (String) row.getField(settings.getCollectionIndex());
            WriteBatch batch =
                    buffer.computeIfAbsent(
                            new MongoNamespace(database, collection), k -> new WriteBatch());
            BsonValue id = BsonCodec.valueFromJson((String) row.getField(settings.getIdIndex()));
            if (kind == RowKind.DELETE) {
                batch.delete(id);
            } else {
                batch.upsert(
                        id,
                        BsonCodec.documentFromJson(
                                (String) row.getField(settings.getDocumentIndex())));
            }
            buffered++;
        }
        if (buffered >= settings.getBatchSize()
                || System.currentTimeMillis() - lastFlushMs >= settings.getBatchIntervalMs()) {
            flush();
        }
    }

    /** Writes all buffered operations; also invoked by the engine flush timer. */
    public synchronized void flush() {
        if (buffered == 0) {
            lastFlushMs = System.currentTimeMillis();
            return;
        }
        for (Map.Entry<MongoNamespace, WriteBatch> entry : buffer.entrySet()) {
            if (settings.getWriteMode() == WriteMode.DELETE_INSERT) {
                deleteInsertWithRetry(entry.getKey(), entry.getValue());
            } else {
                bulkWriteWithRetry(entry.getKey(), entry.getValue().operations());
            }
        }
        written += buffered;
        log.debug("Flushed {} write(s), {} in total", buffered, written);
        buffer.clear();
        buffered = 0;
        lastFlushMs = System.currentTimeMillis();
    }

    /**
     * Applies the final state of every id: {@code deleteMany} of all ids of a chunk, then {@code
     * insertMany} of the documents that still exist. Both statements are retried together, which
     * is idempotent.
     *
     * <p>If the insert reports only duplicate keys, the chunk is replayed with its original
     * ordered operations instead. This covers ids that MongoDB treats as equal while BSON does
     * not (for example {@code 1} and {@code NumberLong(1)} in the same batch). Other per-document
     * errors (unique index violation, document too large) fail the job.
     */
    private void deleteInsertWithRetry(MongoNamespace ns, WriteBatch batch) {
        MongoCollection<BsonDocument> collection = collection(ns);
        List<BsonValue> ids = batch.ids();
        for (int from = 0; from < ids.size(); from += DELETE_INSERT_CHUNK) {
            List<BsonValue> chunk = ids.subList(from, Math.min(ids.size(), from + DELETE_INSERT_CHUNK));
            List<BsonDocument> documents = batch.documents(chunk);
            BsonDocument filter = new BsonDocument("_id", new BsonDocument("$in", new BsonArray(chunk)));
            int attempt = 0;
            while (true) {
                try {
                    collection.deleteMany(filter);
                    if (!documents.isEmpty()) {
                        collection.insertMany(documents, UNORDERED);
                    }
                    break;
                } catch (MongoBulkWriteException e) {
                    if (!e.getWriteErrors().isEmpty() && onlyDuplicateKeys(e)) {
                        log.warn(
                                "insertMany into {} reported {} duplicate key(s), replaying {} id(s)"
                                        + " with ordered upserts",
                                ns.getFullName(),
                                e.getWriteErrors().size(),
                                chunk.size());
                        bulkWriteWithRetry(ns, batch.operations(chunk));
                        break;
                    }
                    if (!e.getWriteErrors().isEmpty()) {
                        throw new CosmosMongoConnectorException(
                                WRITE_FAILED,
                                "Insert into " + ns.getFullName() + " rejected: " + e.getWriteErrors(),
                                e);
                    }
                    attempt = backoffOrFail(ns, attempt, e);
                } catch (MongoException e) {
                    attempt = backoffOrFail(ns, attempt, e);
                }
            }
        }
    }

    private static boolean onlyDuplicateKeys(MongoBulkWriteException e) {
        for (BulkWriteError error : e.getWriteErrors()) {
            if (error.getCode() != DUPLICATE_KEY) {
                return false;
            }
        }
        return true;
    }

    private MongoCollection<BsonDocument> collection(MongoNamespace ns) {
        return client.getDatabase(ns.getDatabaseName())
                .getCollection(ns.getCollectionName(), BsonDocument.class);
    }

    private void bulkWriteWithRetry(MongoNamespace ns, List<WriteModel<BsonDocument>> models) {
        int attempt = 0;
        while (true) {
            try {
                collection(ns).bulkWrite(models, ORDERED);
                return;
            } catch (MongoBulkWriteException e) {
                // Per-document errors (document too large, unique index violation, ...) fail
                // the same way again, so do not retry them.
                if (!e.getWriteErrors().isEmpty()) {
                    throw new CosmosMongoConnectorException(
                            WRITE_FAILED,
                            "Bulk write to " + ns.getFullName() + " rejected: " + e.getWriteErrors(),
                            e);
                }
                attempt = backoffOrFail(ns, attempt, e);
            } catch (MongoException e) {
                attempt = backoffOrFail(ns, attempt, e);
            }
        }
    }

    private int backoffOrFail(MongoNamespace ns, int attempt, MongoException e) {
        attempt++;
        if (attempt > settings.getMaxRetries()) {
            throw new CosmosMongoConnectorException(
                    WRITE_FAILED,
                    "Bulk write to " + ns.getFullName() + " failed after " + attempt + " attempt(s)",
                    e);
        }
        long backoff =
                Math.min(
                        MAX_BACKOFF_MS,
                        settings.getRetryBackoffMs() * (1L << Math.min(attempt - 1, 20)));
        log.warn(
                "Bulk write to {} failed (attempt {}/{}), retrying in {} ms: {}",
                ns.getFullName(),
                attempt,
                settings.getMaxRetries(),
                backoff,
                e.toString());
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new CosmosMongoConnectorException(WRITE_FAILED, "Interrupted while retrying", ie);
        }
        return attempt;
    }

    /** Flushes before the checkpoint completes so checkpointed source positions are durable. */
    @Override
    public Optional<Void> prepareCommit(long checkpointId) {
        flush();
        return Optional.empty();
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        try {
            flush();
        } finally {
            client.close();
            log.info("DocumentDB writer closed, {} write(s) applied", written);
        }
    }
}
