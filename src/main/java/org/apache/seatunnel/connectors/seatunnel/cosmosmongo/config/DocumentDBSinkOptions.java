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

public class DocumentDBSinkOptions {

    public static final String CONNECTOR_IDENTITY = "DocumentDB";

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Connection string of the Amazon DocumentDB cluster, e.g. "
                                    + "mongodb://<user>:<pwd>@<cluster>.docdb.amazonaws.com:27017/"
                                    + "?tls=true&replicaSet=rs0&readPreference=secondaryPreferred"
                                    + "&retryWrites=false");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Target database. When omitted the source database name of each row"
                                    + " is used.");

    public static final Option<String> TLS_CA_FILE =
            Options.key("tls_ca_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "PEM bundle used to verify the DocumentDB certificate, e.g. the"
                                    + " global-bundle.pem downloaded from AWS.");

    public static final Option<WriteMode> WRITE_MODE =
            Options.key("write_mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.DELETE_INSERT)
                    .withDescription(
                            "delete_insert: per batch, deleteMany by _id then insertMany the final"
                                    + " documents (fast on DocumentDB, documents are briefly absent);"
                                    + " upsert: one replaceOne(upsert)/deleteOne per row (slow on"
                                    + " DocumentDB, no gap).");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of buffered writes that triggers a bulk write.");

    public static final Option<Long> BATCH_INTERVAL_MS =
            Options.key("batch_interval_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Max age of buffered writes before they are flushed. Buffered writes"
                                    + " are also always flushed on every checkpoint.");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Retries of a failed bulk write (all writes are idempotent).");

    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Initial back-off between write retries, doubled each retry.");
}
