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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.bson.BsonDocument;
import org.bson.BsonString;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ChangeStreamFetcherTest {

    private static BsonDocument token(String position) {
        return new BsonDocument("_data", new BsonString(position));
    }

    private static ChangeStreamDocument<BsonDocument> event(String id, BsonDocument token) {
        return new ChangeStreamDocument<>(
                (String) null,
                token,
                null,
                null,
                new BsonDocument("_id", new BsonString(id)),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static SourceSettings settings(String mode) {
        Map<String, Object> map = new HashMap<>();
        map.put("uri", "mongodb://localhost");
        map.put("database", "db");
        map.put("change_stream.mode", mode);
        return new SourceSettings(ReadonlyConfig.fromMap(map));
    }

    /**
     * Replays a scripted change stream. A null entry ends a server batch (tryNext returns null);
     * getResumeToken always returns {@code cursorToken}, like Cosmos DB returning the position
     * from before the batch.
     */
    private static final class ScriptedCursor
            implements MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> {
        private final Deque<ChangeStreamDocument<BsonDocument>> script = new ArrayDeque<>();
        private final List<Boolean> batchEnds = new ArrayList<>();
        private final BsonDocument cursorToken;

        ScriptedCursor(BsonDocument cursorToken, List<ChangeStreamDocument<BsonDocument>> events) {
            this.cursorToken = cursorToken;
            for (ChangeStreamDocument<BsonDocument> e : events) {
                script.add(e == null ? event("__end__", null) : e);
            }
        }

        @Override
        public ChangeStreamDocument<BsonDocument> tryNext() {
            ChangeStreamDocument<BsonDocument> next = script.poll();
            if (next == null || "__end__".equals(next.getFullDocument().getString("_id").getValue())) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }
            return next;
        }

        @Override
        public BsonDocument getResumeToken() {
            return cursorToken;
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChangeStreamDocument<BsonDocument> next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int available() {
            return script.size();
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }
    }

    private static List<ChangeStreamFetcher.Item> drain(
            ChangeStreamFetcher fetcher, int expected) throws InterruptedException {
        List<ChangeStreamFetcher.Item> items = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 5000;
        while (items.size() < expected && System.currentTimeMillis() < deadline) {
            fetcher.drainTo(items, 100);
            Thread.sleep(5);
        }
        Thread.sleep(50);
        fetcher.drainTo(items, 100);
        return items;
    }

    private static String describe(List<ChangeStreamFetcher.Item> items) {
        StringBuilder sb = new StringBuilder();
        for (ChangeStreamFetcher.Item item : items) {
            sb.append(item.row != null ? (String) item.row.getField(0) : "@" + item.token.getString("_data").getValue())
                    .append(' ');
        }
        return sb.toString().trim();
    }

    private static ChangeStreamFetcher fetcher(String mode, ScriptedCursor cursor) {
        return new ChangeStreamFetcher(
                "db.c",
                settings(mode),
                token("start"),
                cursor,
                true,
                t -> {
                    throw new AssertionError("no reconnect expected");
                },
                e -> new SeaTunnelRow(new Object[] {e.getFullDocument().getString("_id").getValue()}));
    }

    /** Cosmos RU: every event of a batch shares the token pointing after the batch. */
    @Test
    void cosmosBatchTokenIsQueuedOnlyAfterTheWholeBatch() throws Exception {
        BsonDocument b1 = token("8");
        BsonDocument b2 = token("11");
        ScriptedCursor cursor =
                new ScriptedCursor(
                        token("1"), // stale cursor-level token, must never be used
                        Arrays.asList(
                                event("a", b1), event("b", b1), event("c", b1), null,
                                event("d", b2), event("e", b2)));
        try (ChangeStreamFetcher fetcher = fetcher("cosmos_ru", cursor)) {
            fetcher.start();
            Assertions.assertEquals("a b c @8 d e @11", describe(drain(fetcher, 7)));
        }
    }

    /** Back-to-back batches without an empty getMore in between. */
    @Test
    void markerIsQueuedWhenTheTokenChanges() throws Exception {
        BsonDocument b1 = token("8");
        BsonDocument b2 = token("11");
        ScriptedCursor cursor =
                new ScriptedCursor(
                        token("1"), Arrays.asList(event("a", b1), event("b", b1), event("c", b2)));
        try (ChangeStreamFetcher fetcher = fetcher("cosmos_ru", cursor)) {
            fetcher.start();
            Assertions.assertEquals("a b @8 c @11", describe(drain(fetcher, 5)));
        }
    }

    /** Standard MongoDB: a quiet stream advances through the post batch resume token. */
    @Test
    void standardModeAdvancesQuietStreamWithCursorToken() throws Exception {
        ScriptedCursor cursor =
                new ScriptedCursor(
                        token("pbrt"), Arrays.asList(event("a", token("t1")), event("b", token("t2"))));
        try (ChangeStreamFetcher fetcher = fetcher("standard", cursor)) {
            fetcher.start();
            Assertions.assertEquals("a @t1 b @t2 @pbrt", describe(drain(fetcher, 5)));
        }
    }
}
