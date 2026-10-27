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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.MongoClientFactory;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import com.mongodb.client.MongoClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.CONNECT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.ILLEGAL_ARGUMENT;

/**
 * Creates one split per collection and spreads them over the readers. A collection is always
 * owned by a single reader, which keeps per-document event order intact end to end.
 */
@Slf4j
public class CosmosMongoSplitEnumerator
        implements SourceSplitEnumerator<CosmosMongoSplit, CosmosMongoEnumeratorState> {

    private final Context<CosmosMongoSplit> context;
    private final SourceSettings settings;
    /** Keyed by split id: the engine may hand the same split back more than once on restore. */
    private final Map<String, CosmosMongoSplit> pendingSplits = new LinkedHashMap<>();
    private boolean discovered;

    public CosmosMongoSplitEnumerator(
            Context<CosmosMongoSplit> context,
            SourceSettings settings,
            CosmosMongoEnumeratorState restoredState) {
        this.context = context;
        this.settings = settings;
        if (restoredState != null) {
            this.discovered = restoredState.isDiscovered();
            restoredState.getPendingSplits().forEach(this::addPending);
        }
    }

    @Override
    public void open() {}

    /** Lists the collections once (skipped after restore) and assigns splits. */
    @Override
    public void run() {
        synchronized (this) {
            if (!discovered) {
                List<String> collections = discoverCollections();
                CosmosMongoSplit.Phase initialPhase =
                        settings.getStartupMode() == StartupMode.INCREMENTAL
                                ? CosmosMongoSplit.Phase.STREAM
                                : CosmosMongoSplit.Phase.SNAPSHOT;
                for (String collection : collections) {
                    addPending(
                            new CosmosMongoSplit(
                                    settings.getDatabase(), collection, initialPhase));
                }
                discovered = true;
                log.info(
                        "Discovered {} collection(s) in database {}: {}",
                        collections.size(),
                        settings.getDatabase(),
                        collections);
            }
            assignPending(context.registeredReaders());
        }
    }

    private List<String> discoverCollections() {
        try (MongoClient client =
                MongoClientFactory.create(
                        settings.getUri(), settings.getTlsCaFile(), "seatunnel-cosmos-enumerator")) {
            // Sorted for a deterministic split -> reader assignment.
            TreeSet<String> existing = new TreeSet<>();
            for (String name : client.getDatabase(settings.getDatabase()).listCollectionNames()) {
                if (!name.startsWith("system.")) {
                    existing.add(name);
                }
            }
            if (settings.getCollections().isEmpty()) {
                return new ArrayList<>(existing);
            }
            List<String> missing = new ArrayList<>();
            for (String name : settings.getCollections()) {
                if (!existing.contains(name)) {
                    missing.add(name);
                }
            }
            if (!missing.isEmpty()) {
                throw new CosmosMongoConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Collections " + missing + " do not exist in " + settings.getDatabase());
            }
            return new ArrayList<>(new TreeSet<>(settings.getCollections()));
        } catch (CosmosMongoConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new CosmosMongoConnectorException(
                    CONNECT_FAILED,
                    "Failed to list collections of database " + settings.getDatabase(),
                    e);
        }
    }

    private void assignPending(Collection<Integer> readers) {
        if (!discovered) {
            return;
        }
        int parallelism = Math.max(1, context.currentParallelism());
        for (Integer reader : readers) {
            List<CosmosMongoSplit> assigned = new ArrayList<>();
            Iterator<CosmosMongoSplit> it = pendingSplits.values().iterator();
            while (it.hasNext()) {
                CosmosMongoSplit split = it.next();
                if (owner(split, parallelism) == reader) {
                    assigned.add(split);
                    it.remove();
                }
            }
            if (!assigned.isEmpty()) {
                context.assignSplit(reader, assigned);
                log.info("Assigned {} split(s) to reader {}: {}", assigned.size(), reader, assigned);
            }
            context.signalNoMoreSplits(reader);
        }
    }

    private static int owner(CosmosMongoSplit split, int parallelism) {
        return Math.floorMod(split.splitId().hashCode(), parallelism);
    }

    @Override
    public void close() {}

    @Override
    public synchronized void addSplitsBack(List<CosmosMongoSplit> splits, int subtaskId) {
        log.info("Received {} split(s) back from reader {}", splits.size(), subtaskId);
        splits.forEach(this::addPending);
    }

    /** Adds a split, keeping only the most advanced copy when the same split arrives twice. */
    private void addPending(CosmosMongoSplit split) {
        CosmosMongoSplit existing = pendingSplits.get(split.splitId());
        if (existing == null) {
            pendingSplits.put(split.splitId(), split);
            return;
        }
        CosmosMongoSplit keep = moreAdvanced(existing, split);
        log.info("Ignoring duplicate split {}, keeping {}", split.splitId(), keep);
        pendingSplits.put(split.splitId(), keep);
    }

    private static CosmosMongoSplit moreAdvanced(CosmosMongoSplit a, CosmosMongoSplit b) {
        if (a.getPhase() != b.getPhase()) {
            return a.getPhase().ordinal() >= b.getPhase().ordinal() ? a : b;
        }
        if (a.getStreamEvents() != b.getStreamEvents()) {
            return a.getStreamEvents() >= b.getStreamEvents() ? a : b;
        }
        return a.getSnapshotRows() >= b.getSnapshotRows() ? a : b;
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Splits are pushed to readers, nothing to do.
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assignPending(Collections.singletonList(subtaskId));
    }

    @Override
    public synchronized CosmosMongoEnumeratorState snapshotState(long checkpointId) {
        List<CosmosMongoSplit> copy = new ArrayList<>();
        for (CosmosMongoSplit split : pendingSplits.values()) {
            copy.add(split.copy());
        }
        return new CosmosMongoEnumeratorState(discovered, copy);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
