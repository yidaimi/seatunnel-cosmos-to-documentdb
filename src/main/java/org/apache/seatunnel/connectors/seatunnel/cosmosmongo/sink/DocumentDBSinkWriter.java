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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.BsonCodec;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.MongoClientFactory;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.WRITE_FAILED;

/**
 * Applies rows to DocumentDB: INSERT / UPDATE_AFTER become {@code replaceOne(_id, doc,
 * upsert)}, DELETE becomes {@code deleteOne(_id)}, UPDATE_BEFORE is ignored.
 *
 * <p>Every operation is idempotent, so a failed bulk write is simply retried as a whole and
 * events replayed after a restore converge to the same result. Buffered writes are flushed on
 * every checkpoint, which gives at-least-once delivery.
 */
@Slf4j
public class DocumentDBSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private static final long MAX_BACKOFF_MS = 30_000L;
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);
    private static final BulkWriteOptions ORDERED = new BulkWriteOptions().ordered(true);

    private final SinkSettings settings;
    private final MongoClient client;
    /** Insertion ordered so writes of one collection keep their order. */
    private final Map<MongoNamespace, List<WriteModel<BsonDocument>>> buffer =
            new LinkedHashMap<>();

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
    }

    @Override
    public synchronized void write(SeaTunnelRow row) {
        WriteModel<BsonDocument> model = toWriteModel(row);
        if (model != null) {
            String database =
                    settings.getDatabase() != null
                            ? settings.getDatabase()
                            : (String) row.getField(settings.getDatabaseIndex());
            String collection = (String) row.getField(settings.getCollectionIndex());
            buffer.computeIfAbsent(new MongoNamespace(database, collection), k -> new ArrayList<>())
                    .add(model);
            buffered++;
        }
        if (buffered >= settings.getBatchSize()
                || System.currentTimeMillis() - lastFlushMs >= settings.getBatchIntervalMs()) {
            flush();
        }
    }

    private WriteModel<BsonDocument> toWriteModel(SeaTunnelRow row) {
        BsonValue id = BsonCodec.valueFromJson((String) row.getField(settings.getIdIndex()));
        BsonDocument filter = new BsonDocument("_id", id);
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                BsonDocument document =
                        BsonCodec.documentFromJson((String) row.getField(settings.getDocumentIndex()));
                return new ReplaceOneModel<>(filter, document, UPSERT);
            case DELETE:
                return new DeleteOneModel<>(filter);
            default:
                return null;
        }
    }

    /** Writes all buffered operations; also invoked by the engine flush timer. */
    public synchronized void flush() {
        if (buffered == 0) {
            lastFlushMs = System.currentTimeMillis();
            return;
        }
        for (Map.Entry<MongoNamespace, List<WriteModel<BsonDocument>>> entry : buffer.entrySet()) {
            bulkWriteWithRetry(entry.getKey(), entry.getValue());
        }
        written += buffered;
        log.debug("Flushed {} write(s), {} in total", buffered, written);
        buffer.clear();
        buffered = 0;
        lastFlushMs = System.currentTimeMillis();
    }

    private void bulkWriteWithRetry(MongoNamespace ns, List<WriteModel<BsonDocument>> models) {
        int attempt = 0;
        while (true) {
            try {
                client.getDatabase(ns.getDatabaseName())
                        .getCollection(ns.getCollectionName(), BsonDocument.class)
                        .bulkWrite(models, ORDERED);
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
