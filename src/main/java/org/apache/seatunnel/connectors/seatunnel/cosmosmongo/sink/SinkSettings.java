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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.DocumentRowSchema;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.WriteMode;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.TLS_CA_FILE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.DocumentDBSinkOptions.WRITE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.ILLEGAL_ARGUMENT;

/** Immutable, serializable view of the sink options plus resolved field positions. */
@Getter
public class SinkSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final String tlsCaFile;
    private final WriteMode writeMode;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final long retryBackoffMs;

    private final int databaseIndex;
    private final int collectionIndex;
    private final int idIndex;
    private final int documentIndex;

    public SinkSettings(ReadonlyConfig config, CatalogTable catalogTable) {
        this.uri = config.get(URI);
        this.database = config.get(DATABASE);
        this.tlsCaFile = config.get(TLS_CA_FILE);
        this.writeMode = config.get(WRITE_MODE);
        this.batchSize = config.get(BATCH_SIZE);
        this.batchIntervalMs = config.get(BATCH_INTERVAL_MS);
        this.maxRetries = config.get(MAX_RETRIES);
        this.retryBackoffMs = config.get(RETRY_BACKOFF_MS);

        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        this.databaseIndex = indexOf(rowType, DocumentRowSchema.FIELD_DATABASE);
        this.collectionIndex = indexOf(rowType, DocumentRowSchema.FIELD_COLLECTION);
        this.idIndex = indexOf(rowType, DocumentRowSchema.FIELD_ID);
        this.documentIndex = indexOf(rowType, DocumentRowSchema.FIELD_DOCUMENT);
    }

    private static int indexOf(SeaTunnelRowType rowType, String field) {
        int index = Arrays.asList(rowType.getFieldNames()).indexOf(field);
        if (index < 0) {
            throw new CosmosMongoConnectorException(
                    ILLEGAL_ARGUMENT,
                    "DocumentDB sink requires upstream field '"
                            + field
                            + "' (fields: database, collection, id, document as produced by"
                            + " the CosmosMongo source), got "
                            + Arrays.toString(rowType.getFieldNames()));
        }
        return index;
    }
}
