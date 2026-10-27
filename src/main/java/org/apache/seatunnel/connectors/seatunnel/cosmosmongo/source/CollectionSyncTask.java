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

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.BsonCodec;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.DocumentRowSchema;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.ChangeStreamMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.CHANGE_STREAM_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.SNAPSHOT_READ_FAILED;

/**
 * Synchronizes one collection: optional full snapshot followed by the change stream.
 *
 * <p>The change stream itself is read by a {@link ChangeStreamFetcher} thread. Reading happens in {@link #fetch()} without the checkpoint lock (it may block on the network
 * or sleep for a retry back-off). The result is a {@link Batch} that {@link #apply} emits and
 * folds into the split state while the caller holds the checkpoint lock, so a checkpoint always
 * sees emitted rows and split progress move together.
 *
 * <p>For {@code startup.mode = initial} the change stream position is captured <i>before</i> the
 * snapshot starts. Changes made while the snapshot runs are replayed afterwards; because the sink
 * applies full-document upserts / deletes by {@code _id}, replaying is idempotent and the target
 * converges to the source state.
 */
@Slf4j
class CollectionSyncTask implements AutoCloseable {

    private static final long MAX_BACKOFF_MS = 30_000L;

    private final CosmosMongoSplit split;
    private final SourceSettings settings;
    private final MongoCollection<BsonDocument> collection;
    private final String tableId;

    private MongoCursor<BsonDocument> snapshotCursor;
    /** Cursor opened before the snapshot when the server gave no token; handed to the fetcher. */
    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> streamCursor;

    private ChangeStreamFetcher fetcher;
    private int consecutiveFailures;
    private long snapshotStartMs;

    CollectionSyncTask(
            CosmosMongoSplit split, SourceSettings settings, MongoClient client, String tableId) {
        this.split = split;
        this.settings = settings;
        this.tableId = tableId;
        this.collection =
                client.getDatabase(split.getDatabase())
                        .getCollection(split.getCollection(), BsonDocument.class);
    }

    CosmosMongoSplit.Phase phase() {
        return split.getPhase();
    }

    CosmosMongoSplit snapshotState() {
        return split.copy();
    }

    String splitId() {
        return split.splitId();
    }

    /** Reads the next batch for the current phase. Must not be called with the checkpoint lock. */
    Batch fetch() {
        switch (split.getPhase()) {
            case SNAPSHOT:
                if (needsTokenBeforeSnapshot()) {
                    return captureStartToken();
                }
                return fetchSnapshot();
            case STREAM:
                return fetchChanges();
            default:
                return Batch.EMPTY;
        }
    }

    /** Emits the rows of a batch and advances the split state. Caller holds the checkpoint lock. */
    void apply(Batch batch, Collector<SeaTunnelRow> output) {
        for (SeaTunnelRow row : batch.rows) {
            output.collect(row);
        }
        if (batch.lastSnapshotId != null) {
            split.setLastSnapshotIdJson(BsonCodec.valueToJson(batch.lastSnapshotId));
        }
        if (batch.resumeToken != null) {
            split.setResumeTokenJson(BsonCodec.documentToJson(batch.resumeToken));
        }
        if (split.getPhase() == CosmosMongoSplit.Phase.SNAPSHOT) {
            split.setSnapshotRows(split.getSnapshotRows() + batch.rows.size());
        } else {
            split.setStreamEvents(split.getStreamEvents() + batch.rows.size());
        }
        if (batch.snapshotDone) {
            CosmosMongoSplit.Phase next =
                    settings.getStartupMode() == StartupMode.SNAPSHOT
                            ? CosmosMongoSplit.Phase.FINISHED
                            : CosmosMongoSplit.Phase.STREAM;
            split.setPhase(next);
            log.info(
                    "Snapshot of {} finished: {} document(s), {} ms since this reader started it,"
                            + " next phase {}",
                    split.splitId(),
                    split.getSnapshotRows(),
                    System.currentTimeMillis() - snapshotStartMs,
                    next);
            if (next == CosmosMongoSplit.Phase.FINISHED) {
                closeQuietly();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Snapshot
    // ------------------------------------------------------------------------------------------

    private boolean needsTokenBeforeSnapshot() {
        return settings.getStartupMode() == StartupMode.INITIAL
                && split.getResumeTokenJson() == null
                && streamCursor == null;
    }

    /**
     * Opens the change stream before the snapshot and records its current position.
     *
     * <p>MongoDB 4.0.7+ returns a {@code postBatchResumeToken} even when there are no events. If
     * an event arrives first, its token is used and the event itself can be dropped: the snapshot
     * runs afterwards and therefore already contains that change. If the server hands out no
     * token at all, the opened cursor is kept and consumed after the snapshot.
     */
    private Batch captureStartToken() {
        try {
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = openChangeStream(null);
            long deadline = System.currentTimeMillis() + settings.getChangeStreamTokenWaitMs();
            BsonDocument token = cursor.getResumeToken();
            while (token == null && System.currentTimeMillis() < deadline) {
                ChangeStreamDocument<BsonDocument> event = cursor.tryNext();
                token = event != null ? event.getResumeToken() : cursor.getResumeToken();
            }
            consecutiveFailures = 0;
            if (token == null) {
                log.warn(
                        "Server returned no resume token for {} within {} ms; keeping the change"
                                + " stream cursor open during the snapshot",
                        split.splitId(),
                        settings.getChangeStreamTokenWaitMs());
                streamCursor = cursor;
                return Batch.EMPTY;
            }
            cursor.close();
            log.info("Captured change stream start position for {}", split.splitId());
            return new Batch(Collections.emptyList(), null, token, false);
        } catch (MongoException e) {
            onFailure(CHANGE_STREAM_FAILED, "open change stream", e);
            return Batch.EMPTY;
        }
    }

    private Batch fetchSnapshot() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        BsonValue lastId = null;
        try {
            if (snapshotCursor == null) {
                snapshotCursor = openSnapshotCursor();
            }
            while (rows.size() < settings.getPollBatchSize() && snapshotCursor.hasNext()) {
                BsonDocument doc = snapshotCursor.next();
                BsonValue id = doc.get("_id");
                rows.add(upsertRow(id, doc, RowKind.INSERT));
                lastId = id;
            }
            consecutiveFailures = 0;
            boolean done = rows.size() < settings.getPollBatchSize();
            if (done) {
                snapshotCursor.close();
                snapshotCursor = null;
            }
            return new Batch(rows, lastId, null, done);
        } catch (MongoException e) {
            // Keep what was read; the cursor is reopened after the last emitted _id.
            closeSnapshotCursor();
            onFailure(SNAPSHOT_READ_FAILED, "snapshot read", e);
            return new Batch(rows, lastId, null, false);
        }
    }

    private MongoCursor<BsonDocument> openSnapshotCursor() {
        Bson filter = new BsonDocument();
        if (snapshotStartMs == 0) {
            snapshotStartMs = System.currentTimeMillis();
        }
        if (split.getLastSnapshotIdJson() != null) {
            BsonValue lastId = BsonCodec.valueFromJson(split.getLastSnapshotIdJson());
            boolean singleIdType = hasSingleIdType();
            filter = BsonCodec.idAfterFilter(lastId, singleIdType);
            log.info(
                    "Resuming snapshot of {} after _id {} ({})",
                    split.splitId(),
                    split.getLastSnapshotIdJson(),
                    singleIdType
                            ? "single _id type"
                            : "mixed _id types, documents of other types are read again");
        } else {
            log.info("Starting snapshot of {}", split.splitId());
        }
        return collection
                .find(filter)
                .sort(new BsonDocument("_id", new BsonInt32(1)))
                .batchSize(settings.getSnapshotFetchSize())
                .iterator();
    }

    /**
     * Whether all {@code _id} values share one type group. A sort groups values by type, so it is
     * enough to compare the smallest and the largest {@code _id} (two index lookups). Documents
     * inserted later are covered by the change stream anyway.
     */
    private boolean hasSingleIdType() {
        BsonDocument projection = new BsonDocument("_id", new BsonInt32(1));
        BsonDocument min =
                collection.find().projection(projection).sort(new BsonDocument("_id", new BsonInt32(1))).limit(1).first();
        BsonDocument max =
                collection.find().projection(projection).sort(new BsonDocument("_id", new BsonInt32(-1))).limit(1).first();
        return min == null || max == null || BsonCodec.sameTypeGroup(min.get("_id"), max.get("_id"));
    }

    // ------------------------------------------------------------------------------------------
    // Change stream
    // ------------------------------------------------------------------------------------------

    private Batch fetchChanges() {
        if (fetcher == null) {
            BsonDocument startToken =
                    split.getResumeTokenJson() == null
                            ? null
                            : BsonCodec.documentFromJson(split.getResumeTokenJson());
            fetcher =
                    new ChangeStreamFetcher(
                            split.splitId(),
                            settings,
                            startToken,
                            streamCursor,
                            settings.getStartupMode() == StartupMode.INITIAL,
                            this::openChangeStream,
                            this::eventToRow);
            // A cursor held since before the snapshot is now owned by the fetcher.
            streamCursor = null;
            fetcher.start();
        }
        List<ChangeStreamFetcher.Item> items = new ArrayList<>();
        fetcher.drainTo(items, settings.getPollBatchSize());
        if (items.isEmpty()) {
            return Batch.EMPTY;
        }
        BsonDocument token = null;
        for (ChangeStreamFetcher.Item item : items) {
            if (item.token != null) {
                token = item.token;
            }
        }
        return new Batch(ChangeStreamFetcher.rows(items), null, token, false);
    }

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openChangeStream(BsonDocument resumeAfter) {
        List<Bson> pipeline;
        if (settings.getChangeStreamMode() == ChangeStreamMode.COSMOS_RU) {
            // The exact pipeline required by Cosmos DB for MongoDB (RU).
            pipeline =
                    Arrays.asList(
                            Aggregates.match(
                                    Filters.in(
                                            "operationType",
                                            Arrays.asList("insert", "update", "replace"))),
                            Aggregates.project(
                                    Projections.include(
                                            "_id", "fullDocument", "ns", "documentKey")));
        } else {
            pipeline = Collections.emptyList();
        }
        ChangeStreamIterable<BsonDocument> iterable =
                collection
                        .watch(pipeline, BsonDocument.class)
                        .fullDocument(FullDocument.UPDATE_LOOKUP)
                        .batchSize(settings.getChangeStreamBatchSize())
                        .maxAwaitTime(settings.getChangeStreamMaxAwaitMs(), TimeUnit.MILLISECONDS);
        if (resumeAfter != null) {
            iterable = iterable.resumeAfter(resumeAfter);
        }
        return iterable.cursor();
    }

    private SeaTunnelRow eventToRow(ChangeStreamDocument<BsonDocument> event) {
        BsonDocument fullDocument = event.getFullDocument();
        if (settings.getChangeStreamMode() == ChangeStreamMode.COSMOS_RU) {
            // operationType is never returned by Cosmos RU: every event is an upsert.
            if (fullDocument == null) {
                log.debug("Skip event without fullDocument for {}", split.splitId());
                return null;
            }
            return upsertRow(fullDocument.get("_id"), fullDocument, RowKind.UPDATE_AFTER);
        }
        String op = event.getOperationTypeString();
        if (op == null) {
            throw new CosmosMongoConnectorException(
                    CHANGE_STREAM_FAILED,
                    "Change event of " + split.splitId() + " has no operationType; use"
                            + " change_stream.mode = cosmos_ru for Cosmos DB for MongoDB (RU)");
        }
        switch (op) {
            case "insert":
            case "update":
            case "replace":
                if (fullDocument == null) {
                    // Deleted before the lookup; the following delete event removes it.
                    return null;
                }
                return upsertRow(
                        fullDocument.get("_id"),
                        fullDocument,
                        "insert".equals(op) ? RowKind.INSERT : RowKind.UPDATE_AFTER);
            case "delete":
                return deleteRow(event.getDocumentKey().get("_id"));
            case "drop":
            case "rename":
            case "dropDatabase":
            case "invalidate":
                throw new CosmosMongoConnectorException(
                        CHANGE_STREAM_FAILED,
                        "Change stream of " + split.splitId() + " received '" + op
                                + "', the collection no longer exists under this name");
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Rows, failures, cleanup
    // ------------------------------------------------------------------------------------------

    private SeaTunnelRow upsertRow(BsonValue id, BsonDocument document, RowKind kind) {
        SeaTunnelRow row = newRow(id);
        row.setField(DocumentRowSchema.INDEX_DOCUMENT, BsonCodec.documentToJson(document));
        row.setRowKind(kind);
        return row;
    }

    private SeaTunnelRow deleteRow(BsonValue id) {
        SeaTunnelRow row = newRow(id);
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    private SeaTunnelRow newRow(BsonValue id) {
        SeaTunnelRow row = new SeaTunnelRow(DocumentRowSchema.ARITY);
        row.setField(DocumentRowSchema.INDEX_DATABASE, split.getDatabase());
        row.setField(DocumentRowSchema.INDEX_COLLECTION, split.getCollection());
        row.setField(DocumentRowSchema.INDEX_ID, BsonCodec.valueToJson(id));
        row.setTableId(tableId);
        return row;
    }

    /** Fails fast on fatal errors, otherwise sleeps with exponential back-off. */
    private void onFailure(
            CosmosMongoConnectorException.ErrorCode code, String action, MongoException e) {
        consecutiveFailures++;
        boolean fatal =
                e instanceof MongoCommandException
                        && ChangeStreamFetcher.FATAL_CODES.contains(e.getCode());
        if (fatal || consecutiveFailures > settings.getMaxRetries()) {
            throw new CosmosMongoConnectorException(
                    code,
                    String.format(
                            "%s of %s failed after %d attempt(s)",
                            action, split.splitId(), consecutiveFailures),
                    e);
        }
        long backoff =
                Math.min(
                        MAX_BACKOFF_MS,
                        settings.getRetryBackoffMs() * (1L << Math.min(consecutiveFailures - 1, 20)));
        log.warn(
                "{} of {} failed (attempt {}/{}), retrying in {} ms: {}",
                action,
                split.splitId(),
                consecutiveFailures,
                settings.getMaxRetries(),
                backoff,
                e.toString());
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new CosmosMongoConnectorException(code, "Interrupted while retrying", ie);
        }
    }

    private void closeSnapshotCursor() {
        if (snapshotCursor != null) {
            try {
                snapshotCursor.close();
            } catch (Exception ignored) {
                // cursor already broken
            }
            snapshotCursor = null;
        }
    }

    private void closeStreamCursor() {
        if (streamCursor != null) {
            try {
                streamCursor.close();
            } catch (Exception ignored) {
                // cursor already broken
            }
            streamCursor = null;
        }
    }

    private void closeQuietly() {
        closeSnapshotCursor();
        closeStreamCursor();
        if (fetcher != null) {
            fetcher.close();
            fetcher = null;
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }

    /** Result of one {@link #fetch()} call. */
    static final class Batch {
        static final Batch EMPTY = new Batch(Collections.emptyList(), null, null, false);

        final List<SeaTunnelRow> rows;
        final BsonValue lastSnapshotId;
        final BsonDocument resumeToken;
        final boolean snapshotDone;

        Batch(
                List<SeaTunnelRow> rows,
                BsonValue lastSnapshotId,
                BsonDocument resumeToken,
                boolean snapshotDone) {
            this.rows = rows;
            this.lastSnapshotId = lastSnapshotId;
            this.resumeToken = resumeToken;
            this.snapshotDone = snapshotDone;
        }
    }
}
