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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common.DocumentRowSchema;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.CosmosMongoSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.config.StartupMode;

import java.util.Collections;
import java.util.List;

/**
 * Azure Cosmos DB for MongoDB source: full snapshot plus change stream based incremental sync.
 */
public class CosmosMongoSource
        implements SeaTunnelSource<SeaTunnelRow, CosmosMongoSplit, CosmosMongoEnumeratorState>,
                SupportParallelism {

    private static final long serialVersionUID = 1L;

    private final SourceSettings settings;
    private final CatalogTable catalogTable;

    public CosmosMongoSource(ReadonlyConfig config) {
        this.settings = new SourceSettings(config);
        this.catalogTable =
                DocumentRowSchema.catalogTable(
                        CosmosMongoSourceOptions.CONNECTOR_IDENTITY, settings.getDatabase());
    }

    @Override
    public String getPluginName() {
        return CosmosMongoSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return settings.getStartupMode() == StartupMode.SNAPSHOT
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, CosmosMongoSplit> createReader(
            SourceReader.Context readerContext) {
        return new CosmosMongoSourceReader(
                readerContext, settings, catalogTable.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<CosmosMongoSplit, CosmosMongoEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<CosmosMongoSplit> enumeratorContext) {
        return new CosmosMongoSplitEnumerator(enumeratorContext, settings, null);
    }

    @Override
    public SourceSplitEnumerator<CosmosMongoSplit, CosmosMongoEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<CosmosMongoSplit> enumeratorContext,
            CosmosMongoEnumeratorState checkpointState) {
        return new CosmosMongoSplitEnumerator(enumeratorContext, settings, checkpointState);
    }
}
