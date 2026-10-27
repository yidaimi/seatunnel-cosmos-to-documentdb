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

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * One collection to synchronize, together with its progress. The split is also the checkpoint
 * state: on restore the reader continues the snapshot after {@link #lastSnapshotIdJson} or resumes
 * the change stream after {@link #resumeTokenJson}.
 */
@Getter
@Setter
@ToString
public class CosmosMongoSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    public enum Phase {
        SNAPSHOT,
        STREAM,
        FINISHED
    }

    private final String database;
    private final String collection;
    private Phase phase;
    /** Canonical Extended JSON of the last emitted {@code _id}; null before the first document. */
    private String lastSnapshotIdJson;
    /** Change stream resume token (Extended JSON); null until the first token is known. */
    private String resumeTokenJson;

    private long snapshotRows;
    private long streamEvents;

    public CosmosMongoSplit(String database, String collection, Phase phase) {
        this.database = database;
        this.collection = collection;
        this.phase = phase;
    }

    @Override
    public String splitId() {
        return database + "." + collection;
    }

    public CosmosMongoSplit copy() {
        CosmosMongoSplit copy = new CosmosMongoSplit(database, collection, phase);
        copy.lastSnapshotIdJson = lastSnapshotIdJson;
        copy.resumeTokenJson = resumeTokenJson;
        copy.snapshotRows = snapshotRows;
        copy.streamEvents = streamEvents;
        return copy;
    }
}
