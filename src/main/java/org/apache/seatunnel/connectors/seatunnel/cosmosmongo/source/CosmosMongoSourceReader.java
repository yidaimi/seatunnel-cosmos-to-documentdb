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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.MongoClientFactory;

import com.mongodb.client.MongoClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Drives the {@link CollectionSyncTask}s assigned to this reader.
 *
 * <p>Snapshots are read one collection at a time. Collections already in the streaming phase are
 * drained in every round, also while another collection is still being snapshotted.
 */
@Slf4j
public class CosmosMongoSourceReader implements SourceReader<SeaTunnelRow, CosmosMongoSplit> {

    private static final long PROGRESS_LOG_INTERVAL_MS = 60_000L;

    private final SourceReader.Context context;
    private final SourceSettings settings;
    private final String tableId;
    private final List<CollectionSyncTask> tasks = new CopyOnWriteArrayList<>();

    private MongoClient client;
    private volatile boolean noMoreSplits;
    private boolean endSignaled;
    private long lastProgressLogMs = System.currentTimeMillis();

    public CosmosMongoSourceReader(
            SourceReader.Context context, SourceSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    @Override
    public void open() {
        client();
    }

    private synchronized MongoClient client() {
        if (client == null) {
            client =
                    MongoClientFactory.create(
                            settings.getUri(),
                            settings.getTlsCaFile(),
                            "seatunnel-cosmos-reader-" + context.getIndexOfSubtask());
        }
        return client;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        CollectionSyncTask snapshotTask = null;
        List<CollectionSyncTask> streaming = new ArrayList<>();
        boolean allFinished = true;
        for (CollectionSyncTask task : tasks) {
            switch (task.phase()) {
                case SNAPSHOT:
                    if (snapshotTask == null) {
                        snapshotTask = task;
                    }
                    allFinished = false;
                    break;
                case STREAM:
                    streaming.add(task);
                    allFinished = false;
                    break;
                default:
                    break;
            }
        }

        if (allFinished) {
            if (noMoreSplits && context.getBoundedness() == Boundedness.BOUNDED) {
                if (!endSignaled) {
                    endSignaled = true;
                    log.info(
                            "Reader {} finished all {} collection(s)",
                            context.getIndexOfSubtask(),
                            tasks.size());
                    context.signalNoMoreElement();
                }
            } else {
                Thread.sleep(settings.getChangeStreamIdleSleepMs());
            }
            return;
        }

        boolean busy = false;
        if (snapshotTask != null) {
            CollectionSyncTask.Batch batch = snapshotTask.fetch();
            synchronized (output.getCheckpointLock()) {
                snapshotTask.apply(batch, output);
            }
            busy = true;
        }
        // Never blocks: change events are read ahead by one fetcher thread per collection.
        for (CollectionSyncTask task : streaming) {
            CollectionSyncTask.Batch batch = task.fetch();
            if (batch.rows.isEmpty() && batch.resumeToken == null) {
                continue;
            }
            busy |= !batch.rows.isEmpty();
            synchronized (output.getCheckpointLock()) {
                task.apply(batch, output);
            }
        }
        if (!busy) {
            Thread.sleep(settings.getChangeStreamIdleSleepMs());
        }
        logProgress();
    }

    private void logProgress() {
        long now = System.currentTimeMillis();
        if (now - lastProgressLogMs < PROGRESS_LOG_INTERVAL_MS) {
            return;
        }
        lastProgressLogMs = now;
        for (CollectionSyncTask task : tasks) {
            CosmosMongoSplit s = task.snapshotState();
            log.info(
                    "Progress {}: phase={}, snapshotRows={}, streamEvents={}",
                    s.splitId(),
                    s.getPhase(),
                    s.getSnapshotRows(),
                    s.getStreamEvents());
        }
    }

    /** Returns the progress of every assigned collection; called under the checkpoint lock. */
    @Override
    public List<CosmosMongoSplit> snapshotState(long checkpointId) {
        List<CosmosMongoSplit> state = new ArrayList<>(tasks.size());
        for (CollectionSyncTask task : tasks) {
            state.add(task.snapshotState());
        }
        return state;
    }

    @Override
    public void addSplits(List<CosmosMongoSplit> splits) {
        for (CosmosMongoSplit split : splits) {
            if (tasks.stream().anyMatch(t -> t.splitId().equals(split.splitId()))) {
                log.warn(
                        "Reader {} ignores duplicate split {}",
                        context.getIndexOfSubtask(),
                        split.splitId());
                continue;
            }
            log.info("Reader {} received split {}", context.getIndexOfSubtask(), split);
            tasks.add(new CollectionSyncTask(split, settings, client(), tableId));
        }
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {
        for (CollectionSyncTask task : tasks) {
            task.close();
        }
        synchronized (this) {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
