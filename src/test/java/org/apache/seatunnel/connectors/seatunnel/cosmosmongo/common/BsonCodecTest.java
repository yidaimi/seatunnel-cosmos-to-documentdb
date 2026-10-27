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

import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class BsonCodecTest {

    @Test
    void documentRoundTripKeepsBsonTypes() {
        BsonDocument doc =
                new BsonDocument("_id", new BsonObjectId(new ObjectId()))
                        .append("i32", new BsonInt32(1))
                        .append("i64", new BsonInt64(1L))
                        .append("dec", new BsonDecimal128(new Decimal128(new BigDecimal("1.10"))))
                        .append("ts", new BsonDateTime(1_700_000_000_000L))
                        .append("bin", new BsonBinary(new byte[] {1, 2, 3}))
                        .append("nested", new BsonDocument("a", new BsonString("b")));
        BsonDocument back = BsonCodec.documentFromJson(BsonCodec.documentToJson(doc));
        Assertions.assertEquals(doc, back);
        Assertions.assertTrue(back.get("i64").isInt64());
        Assertions.assertTrue(back.get("i32").isInt32());
    }

    @Test
    void valueRoundTrip() {
        BsonValue[] values = {
            new BsonObjectId(new ObjectId()),
            new BsonString("a:b}c"),
            new BsonInt64(42L),
            new BsonDocument("k", new BsonInt32(1)).append("s", new BsonString("x"))
        };
        for (BsonValue v : values) {
            Assertions.assertEquals(v, BsonCodec.valueFromJson(BsonCodec.valueToJson(v)));
        }
    }

    @Test
    void idAfterFilterWithSingleIdTypeIsPlainGt() {
        BsonDocument filter = BsonCodec.idAfterFilter(new BsonString("abc"), true);
        Assertions.assertEquals(
                BsonDocument.parse("{\"_id\": {\"$gt\": \"abc\"}}"), filter);
    }

    @Test
    void idAfterFilterWithMixedTypesSelectsAllOtherTypes() {
        BsonDocument filter = BsonCodec.idAfterFilter(new BsonInt32(7), false);
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$or\": [{\"_id\": {\"$gt\": 7}}, {\"_id\": {\"$not\": {\"$type\":"
                                + " [\"int\", \"long\", \"double\", \"decimal\"]}}}]}"),
                filter);
    }

    @Test
    void sameTypeGroup() {
        Assertions.assertTrue(BsonCodec.sameTypeGroup(new BsonInt32(1), new BsonInt64(2L)));
        Assertions.assertFalse(
                BsonCodec.sameTypeGroup(new BsonInt32(1), new BsonObjectId(new ObjectId())));
        Assertions.assertFalse(
                BsonCodec.sameTypeGroup(new BsonString("a"), new BsonObjectId(new ObjectId())));
    }
}
