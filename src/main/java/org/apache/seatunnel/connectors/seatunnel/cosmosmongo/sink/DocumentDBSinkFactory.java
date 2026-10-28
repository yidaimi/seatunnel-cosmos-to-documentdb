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

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.CONNECTOR_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.TLS_CA_FILE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.WRITE_MODE;

public class DocumentDBSinkFactory implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URI)
                .optional(
                        DATABASE,
                        TLS_CA_FILE,
                        WRITE_MODE,
                        BATCH_SIZE,
                        BATCH_INTERVAL_MS,
                        MAX_RETRIES,
                        RETRY_BACKOFF_MS)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        return () -> new DocumentDBSink(context.getOptions(), context.getCatalogTable());
    }
}
