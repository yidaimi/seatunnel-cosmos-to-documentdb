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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.EnumMap;
import java.util.Map;

/** Lossless BSON <-> canonical Extended JSON conversion plus snapshot resume helpers. */
public final class BsonCodec {

    private static final JsonWriterSettings CANONICAL =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    private static final String WRAPPER_KEY = "v";

    /** {@code $type} aliases of the BSON types that compare with each other by value. */
    private static final Map<BsonType, String[]> TYPE_GROUP_ALIASES = new EnumMap<>(BsonType.class);

    static {
        String[] numbers = {"int", "long", "double", "decimal"};
        TYPE_GROUP_ALIASES.put(BsonType.INT32, numbers);
        TYPE_GROUP_ALIASES.put(BsonType.INT64, numbers);
        TYPE_GROUP_ALIASES.put(BsonType.DOUBLE, numbers);
        TYPE_GROUP_ALIASES.put(BsonType.DECIMAL128, numbers);
        String[] strings = {"string", "symbol"};
        TYPE_GROUP_ALIASES.put(BsonType.STRING, strings);
        TYPE_GROUP_ALIASES.put(BsonType.SYMBOL, strings);
        TYPE_GROUP_ALIASES.put(BsonType.OBJECT_ID, new String[] {"objectId"});
        TYPE_GROUP_ALIASES.put(BsonType.DOCUMENT, new String[] {"object"});
        TYPE_GROUP_ALIASES.put(BsonType.BINARY, new String[] {"binData"});
        TYPE_GROUP_ALIASES.put(BsonType.BOOLEAN, new String[] {"bool"});
        TYPE_GROUP_ALIASES.put(BsonType.DATE_TIME, new String[] {"date"});
        TYPE_GROUP_ALIASES.put(BsonType.TIMESTAMP, new String[] {"timestamp"});
    }

    private BsonCodec() {}

    public static String documentToJson(BsonDocument document) {
        return document.toJson(CANONICAL);
    }

    public static BsonDocument documentFromJson(String json) {
        return BsonDocument.parse(json);
    }

    /** Serializes a single BSON value (e.g. an {@code _id}) to canonical Extended JSON. */
    public static String valueToJson(BsonValue value) {
        String wrapped = new BsonDocument(WRAPPER_KEY, value).toJson(CANONICAL);
        // {"v": <value>}
        return wrapped.substring(wrapped.indexOf(':') + 1, wrapped.lastIndexOf('}')).trim();
    }

    public static BsonValue valueFromJson(String json) {
        return BsonDocument.parse("{\"" + WRAPPER_KEY + "\": " + json + "}").get(WRAPPER_KEY);
    }

    /** True when both values belong to the same comparison group (e.g. int and long). */
    public static boolean sameTypeGroup(BsonValue a, BsonValue b) {
        String[] ga = TYPE_GROUP_ALIASES.get(a.getBsonType());
        String[] gb = TYPE_GROUP_ALIASES.get(b.getBsonType());
        return ga != null ? ga == gb : a.getBsonType() == b.getBsonType();
    }

    /**
     * Filter selecting every document a {@code sort({_id: 1})} scan has not returned yet after it
     * stopped at {@code lastId}.
     *
     * <p>Only same-type comparison is relied on: servers disagree on how {@code _id} values of
     * different types are ordered and compared (Cosmos DB sorts strings after ObjectIds, while
     * {@code $gt} on a number does not match other types). When the collection holds a single
     * {@code _id} type the filter is a plain {@code $gt}. Otherwise documents of other types are
     * selected as well; some of them are read twice, which the idempotent sink absorbs.
     *
     * @param lastId last {@code _id} emitted by the scan
     * @param singleIdType whether every {@code _id} in the collection has the type of {@code lastId}
     */
    public static BsonDocument idAfterFilter(BsonValue lastId, boolean singleIdType) {
        BsonDocument gt = new BsonDocument("_id", new BsonDocument("$gt", lastId));
        if (singleIdType) {
            return gt;
        }
        String[] aliases = TYPE_GROUP_ALIASES.get(lastId.getBsonType());
        if (aliases == null) {
            // Unusual _id type: scan everything again rather than risk a gap.
            return new BsonDocument();
        }
        BsonArray types = new BsonArray();
        for (String alias : aliases) {
            types.add(new BsonString(alias));
        }
        BsonDocument otherTypes =
                new BsonDocument(
                        "_id",
                        new BsonDocument("$not", new BsonDocument("$type", types)));
        BsonArray or = new BsonArray();
        or.add(gt);
        or.add(otherTypes);
        return new BsonDocument("$or", or);
    }
}
