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

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class CosmosMongoSourceOptions {

    public static final String CONNECTOR_IDENTITY = "CosmosMongo";

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Connection string of the Azure Cosmos DB for MongoDB account, e.g. "
                                    + "mongodb://<account>:<key>@<account>.mongo.cosmos.azure.com:10255/"
                                    + "?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Source database name.");

    public static final Option<List<String>> COLLECTIONS =
            Options.key("collections")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Collections to synchronize. When omitted, every non-system collection"
                                    + " of the database is synchronized.");

    public static final Option<StartupMode> STARTUP_MODE =
            Options.key("startup.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "initial: full snapshot then change stream; snapshot: full snapshot only"
                                    + " (bounded job); incremental: change stream only.");

    public static final Option<ChangeStreamMode> CHANGE_STREAM_MODE =
            Options.key("change_stream.mode")
                    .enumType(ChangeStreamMode.class)
                    .defaultValue(ChangeStreamMode.COSMOS_RU)
                    .withDescription(
                            "cosmos_ru: Cosmos DB for MongoDB (RU) compatible pipeline, no deletes;"
                                    + " standard: regular MongoDB change stream with deletes.");

    public static final Option<Integer> SNAPSHOT_FETCH_SIZE =
            Options.key("snapshot.fetch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Cursor batch size used while reading the full snapshot.");

    public static final Option<Integer> CHANGE_STREAM_BATCH_SIZE =
            Options.key("change_stream.batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Cursor batch size of the change stream.");

    public static final Option<Long> CHANGE_STREAM_MAX_AWAIT_MS =
            Options.key("change_stream.max_await_ms")
                    .longType()
                    .defaultValue(500L)
                    .withDescription(
                            "Max time the server waits for new events before returning an empty"
                                    + " batch (maxAwaitTimeMS).");

    public static final Option<Integer> CHANGE_STREAM_QUEUE_SIZE =
            Options.key("change_stream.queue_size")
                    .intType()
                    .defaultValue(8192)
                    .withDescription(
                            "Per collection buffer of change events read ahead by the background"
                                    + " change stream thread.");

    public static final Option<Long> CHANGE_STREAM_IDLE_SLEEP_MS =
            Options.key("change_stream.idle_sleep_ms")
                    .longType()
                    .defaultValue(50L)
                    .withDescription(
                            "Reader sleep time when no collection had any queued change in a"
                                    + " round.");

    public static final Option<Long> CHANGE_STREAM_TOKEN_WAIT_MS =
            Options.key("change_stream.token_wait_ms")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "How long to wait for the server to hand out an initial resume token"
                                    + " before a snapshot starts.");

    public static final Option<Integer> POLL_BATCH_SIZE =
            Options.key("poll.batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Max number of documents / events emitted per collection in one"
                                    + " poll round. Checkpoints can happen between rounds.");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Consecutive retries for a failing read (throttling 16500, network"
                                    + " errors, ...) before the job fails.");

    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Initial back-off between read retries, doubled on every retry (max 30s).");

    public static final Option<String> TLS_CA_FILE =
            Options.key("tls_ca_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional PEM bundle used to verify the server certificate. Not needed"
                                    + " for Cosmos DB (public CA).");
}
