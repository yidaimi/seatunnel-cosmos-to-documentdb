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

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.common;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import java.util.Arrays;
import java.util.Collections;

/**
 * Fixed, schema-less row layout shared by the CosmosMongo source and the DocumentDB sink.
 *
 * <p>Documents are carried as canonical Extended JSON so every BSON type (ObjectId, Date,
 * Decimal128, Binary, ...) survives the trip unchanged, independent of per-collection schemas.
 */
public final class DocumentRowSchema {

    public static final String FIELD_DATABASE = "database";
    public static final String FIELD_COLLECTION = "collection";
    /** Canonical Extended JSON of the {@code _id} value. */
    public static final String FIELD_ID = "id";
    /** Canonical Extended JSON of the full document; null for deletes. */
    public static final String FIELD_DOCUMENT = "document";

    public static final int INDEX_DATABASE = 0;
    public static final int INDEX_COLLECTION = 1;
    public static final int INDEX_ID = 2;
    public static final int INDEX_DOCUMENT = 3;
    public static final int ARITY = 4;

    public static final String TABLE_NAME = "documents";

    private DocumentRowSchema() {}

    public static CatalogTable catalogTable(String catalogName, String database) {
        TableSchema schema =
                TableSchema.builder()
                        .column(column(FIELD_DATABASE, false, "source database"))
                        .column(column(FIELD_COLLECTION, false, "source collection"))
                        .column(column(FIELD_ID, false, "_id as canonical extended JSON"))
                        .column(column(FIELD_DOCUMENT, true, "document as canonical extended JSON"))
                        .primaryKey(
                                PrimaryKey.of(
                                        "pk",
                                        Arrays.asList(FIELD_DATABASE, FIELD_COLLECTION, FIELD_ID)))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of(catalogName, database, TABLE_NAME),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "MongoDB documents as extended JSON");
    }

    private static PhysicalColumn column(String name, boolean nullable, String comment) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, (Long) null, nullable, null, comment);
    }
}
