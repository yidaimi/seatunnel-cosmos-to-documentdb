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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_IDLE_SLEEP_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_MAX_AWAIT_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CHANGE_STREAM_TOKEN_WAIT_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.COLLECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.CONNECTOR_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.POLL_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.SNAPSHOT_FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.STARTUP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.TLS_CA_FILE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions.URI;

public class CosmosMongoSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URI, DATABASE)
                .optional(
                        COLLECTIONS,
                        STARTUP_MODE,
                        CHANGE_STREAM_MODE,
                        SNAPSHOT_FETCH_SIZE,
                        CHANGE_STREAM_BATCH_SIZE,
                        CHANGE_STREAM_MAX_AWAIT_MS,
                        CHANGE_STREAM_IDLE_SLEEP_MS,
                        CHANGE_STREAM_QUEUE_SIZE,
                        CHANGE_STREAM_TOKEN_WAIT_MS,
                        POLL_BATCH_SIZE,
                        MAX_RETRIES,
                        RETRY_BACKOFF_MS,
                        TLS_CA_FILE)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new CosmosMongoSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CosmosMongoSource.class;
    }
}
