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
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.ChangeStreamMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.StartupMode;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_IDLE_SLEEP_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_MAX_AWAIT_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_TOKEN_WAIT_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.COLLECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.POLL_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.SNAPSHOT_FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.STARTUP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.TLS_CA_FILE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.URI;

/** Immutable, serializable view of the source options. */
@Getter
public class SourceSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final List<String> collections;
    private final StartupMode startupMode;
    private final ChangeStreamMode changeStreamMode;
    private final int snapshotFetchSize;
    private final int changeStreamBatchSize;
    private final long changeStreamMaxAwaitMs;
    private final long changeStreamIdleSleepMs;
    private final int changeStreamQueueSize;
    private final long changeStreamTokenWaitMs;
    private final int pollBatchSize;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final String tlsCaFile;

    public SourceSettings(ReadonlyConfig config) {
        this.uri = config.get(URI);
        this.database = config.get(DATABASE);
        this.collections = config.getOptional(COLLECTIONS).orElse(Collections.emptyList());
        this.startupMode = config.get(STARTUP_MODE);
        this.changeStreamMode = config.get(CHANGE_STREAM_MODE);
        this.snapshotFetchSize = config.get(SNAPSHOT_FETCH_SIZE);
        this.changeStreamBatchSize = config.get(CHANGE_STREAM_BATCH_SIZE);
        this.changeStreamMaxAwaitMs = config.get(CHANGE_STREAM_MAX_AWAIT_MS);
        this.changeStreamIdleSleepMs = config.get(CHANGE_STREAM_IDLE_SLEEP_MS);
        this.changeStreamQueueSize = config.get(CHANGE_STREAM_QUEUE_SIZE);
        this.changeStreamTokenWaitMs = config.get(CHANGE_STREAM_TOKEN_WAIT_MS);
        this.pollBatchSize = config.get(POLL_BATCH_SIZE);
        this.maxRetries = config.get(MAX_RETRIES);
        this.retryBackoffMs = config.get(RETRY_BACKOFF_MS);
        this.tlsCaFile = config.get(TLS_CA_FILE);
    }
}
