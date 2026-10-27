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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.ChangeStreamMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import org.bson.BsonDocument;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.CHANGE_STREAM_FAILED;

/**
 * Reads the change stream of one collection on a dedicated thread into a bounded queue.
 *
 * <p>A blocking {@code getMore} on a quiet collection therefore never delays other collections:
 * the reader only drains queues.
 *
 * <p>Positions are queued as separate <i>marker</i> items, only once every event they cover is
 * queued. This matters for Cosmos DB (RU): all events of one server batch carry the same token,
 * which points <i>after</i> the whole batch, so storing it after emitting only part of the batch
 * would skip the rest on restore. A marker is therefore queued when the next event has a
 * different token or when the batch is exhausted. The reader checkpoints the last emitted marker;
 * this thread reconnects after the last queued marker, so nothing is skipped (events after that
 * marker may be read twice, which the idempotent sink absorbs).
 *
 * <p>The cursor-level token returned after a batch ({@code postBatchResumeToken}) is only used to
 * advance a quiet stream in {@code standard} mode: Cosmos DB (RU) returns the position from
 * <i>before</i> the batch there.
 */
@Slf4j
class ChangeStreamFetcher implements AutoCloseable {

    /** Errors that retrying cannot fix. */
    static final Set<Integer> FATAL_CODES =
            new HashSet<>(
                    Arrays.asList(
                            13, // Unauthorized
                            18, // AuthenticationFailed
                            26, // NamespaceNotFound (Cosmos: collection dropped)
                            260, // InvalidResumeToken
                            280, // ChangeStreamFatalError
                            286 // ChangeStreamHistoryLost
                            ));

    private static final long MAX_BACKOFF_MS = 30_000L;

    private final String splitId;
    private final SourceSettings settings;
    private final Function<BsonDocument, MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>
            opener;
    private final Function<ChangeStreamDocument<BsonDocument>, SeaTunnelRow> converter;
    /** When true, losing the position (no token yet) is fatal instead of restarting from now. */
    private final boolean positionRequired;

    private final BlockingQueue<Item> queue;
    private final Thread thread;

    private volatile boolean running = true;
    private volatile RuntimeException failure;

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
    /** Token of the last queued marker; the reconnect position. Only used by the fetch thread. */
    private BsonDocument queuedToken;
    /** Token shared by events queued since the last marker; null when nothing is pending. */
    private BsonDocument pendingToken;

    private final boolean usePostBatchToken;

    ChangeStreamFetcher(
            String splitId,
            SourceSettings settings,
            BsonDocument startToken,
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openCursor,
            boolean positionRequired,
            Function<BsonDocument, MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>
                    opener,
            Function<ChangeStreamDocument<BsonDocument>, SeaTunnelRow> converter) {
        this.splitId = splitId;
        this.settings = settings;
        this.queuedToken = startToken;
        this.cursor = openCursor;
        this.positionRequired = positionRequired;
        this.opener = opener;
        this.converter = converter;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, settings.getChangeStreamQueueSize()));
        this.thread = new Thread(this::run, "cosmos-change-stream-" + splitId);
        this.thread.setDaemon(true);
        this.usePostBatchToken = settings.getChangeStreamMode() == ChangeStreamMode.STANDARD;
    }

    void start() {
        thread.start();
    }

    /**
     * Moves up to {@code max} queued items into {@code out} without blocking.
     *
     * @throws CosmosMongoConnectorException if the fetch thread failed and the queue is drained
     */
    void drainTo(List<Item> out, int max) {
        queue.drainTo(out, max);
        if (out.isEmpty() && failure != null) {
            throw failure;
        }
    }

    private void run() {
        int failures = 0;
        while (running) {
            try {
                if (cursor == null) {
                    cursor = opener.apply(queuedToken);
                    log.info(
                            "Change stream opened for {} ({})",
                            splitId,
                            queuedToken == null ? "from now" : "after stored resume token");
                }
                ChangeStreamDocument<BsonDocument> event = cursor.tryNext();
                if (event != null) {
                    BsonDocument token = event.getResumeToken();
                    if (pendingToken != null && !pendingToken.equals(token)) {
                        // Every event of the previous batch is queued.
                        putMarker(pendingToken);
                    }
                    SeaTunnelRow row = converter.apply(event);
                    if (row != null) {
                        put(new Item(row, null));
                    }
                    pendingToken = token;
                } else if (pendingToken != null) {
                    // Batch exhausted: its position is complete.
                    putMarker(pendingToken);
                } else if (usePostBatchToken) {
                    BsonDocument token = cursor.getResumeToken();
                    if (token != null && !token.equals(queuedToken)) {
                        // Quiet stream: the position advanced without events.
                        putMarker(token);
                    }
                }
                failures = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (MongoException e) {
                if (!running) {
                    break;
                }
                closeCursor();
                // Events after the last marker are read again from queuedToken.
                pendingToken = null;
                failures++;
                boolean fatal =
                        e instanceof MongoCommandException && FATAL_CODES.contains(e.getCode());
                if (queuedToken == null && positionRequired) {
                    fail(
                            new CosmosMongoConnectorException(
                                    CHANGE_STREAM_FAILED,
                                    "Change stream of "
                                            + splitId
                                            + " failed before any resume token was received;"
                                            + " reopening it would skip changes made during the"
                                            + " snapshot",
                                    e));
                    break;
                }
                if (fatal || failures > settings.getMaxRetries()) {
                    fail(
                            new CosmosMongoConnectorException(
                                    CHANGE_STREAM_FAILED,
                                    String.format(
                                            "Change stream of %s failed after %d attempt(s)",
                                            splitId, failures),
                                    e));
                    break;
                }
                long backoff =
                        Math.min(
                                MAX_BACKOFF_MS,
                                settings.getRetryBackoffMs() * (1L << Math.min(failures - 1, 20)));
                log.warn(
                        "Change stream of {} failed (attempt {}/{}), reconnecting in {} ms: {}",
                        splitId,
                        failures,
                        settings.getMaxRetries(),
                        backoff,
                        e.toString());
                if (!sleep(backoff)) {
                    break;
                }
            } catch (RuntimeException e) {
                fail(e);
                break;
            }
        }
        closeCursor();
    }

    private void putMarker(BsonDocument token) throws InterruptedException {
        put(new Item(null, token));
        pendingToken = null;
    }

    private void put(Item item) throws InterruptedException {
        // Bounded queue: when the pipeline is slow this blocks and the server keeps the events.
        while (running && !queue.offer(item, 100, TimeUnit.MILLISECONDS)) {
            // retry until there is room or the fetcher is closed
        }
        if (item.token != null) {
            queuedToken = item.token;
        }
    }

    private void fail(RuntimeException e) {
        log.error("Change stream fetcher of {} stopped", splitId, e);
        failure = e;
    }

    private boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void closeCursor() {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ignored) {
                // cursor already broken
            }
            cursor = null;
        }
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Either a converted change event (token null) or a position marker (row null). */
    static final class Item {
        final SeaTunnelRow row;
        final BsonDocument token;

        Item(SeaTunnelRow row, BsonDocument token) {
            this.row = row;
            this.token = token;
        }
    }

    static List<SeaTunnelRow> rows(List<Item> items) {
        List<SeaTunnelRow> rows = new ArrayList<>(items.size());
        for (Item item : items) {
            if (item.row != null) {
                rows.add(item.row);
            }
        }
        return rows;
    }
}
