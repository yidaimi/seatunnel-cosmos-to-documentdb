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

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class WriteBatchTest {

    private static BsonValue id(String v) {
        return new BsonString(v);
    }

    private static BsonDocument doc(String id, int version) {
        return new BsonDocument("_id", new BsonString(id)).append("v", new BsonInt32(version));
    }

    @Test
    void lastStatePerIdWins() {
        WriteBatch batch = new WriteBatch();
        batch.upsert(id("a"), doc("a", 1)); // insert
        batch.upsert(id("a"), doc("a", 2)); // update
        batch.upsert(id("b"), doc("b", 1)); // insert ...
        batch.delete(id("b")); // ... then delete
        batch.delete(id("c")); // delete ...
        batch.upsert(id("c"), doc("c", 5)); // ... then re-insert
        batch.delete(id("d")); // plain delete

        Assertions.assertEquals(7, batch.size());
        Assertions.assertEquals(Arrays.asList(id("a"), id("b"), id("c"), id("d")), batch.ids());
        // deleteMany covers every id, insertMany only the documents that still exist
        Assertions.assertEquals(
                Arrays.asList(doc("a", 2), doc("c", 5)), batch.documents(batch.ids()));
    }

    @Test
    void documentsOfAChunkOnly() {
        WriteBatch batch = new WriteBatch();
        batch.upsert(id("a"), doc("a", 1));
        batch.upsert(id("b"), doc("b", 1));
        Assertions.assertEquals(
                Collections.singletonList(doc("b", 1)),
                batch.documents(Collections.singletonList(id("b"))));
    }

    @Test
    void operationsKeepArrivalOrderForFallback() {
        WriteBatch batch = new WriteBatch();
        batch.upsert(id("a"), doc("a", 1));
        batch.upsert(id("b"), doc("b", 1));
        batch.delete(id("a"));
        batch.upsert(id("a"), doc("a", 3));

        List<WriteModel<BsonDocument>> all = batch.operations();
        Assertions.assertEquals(4, all.size());

        List<WriteModel<BsonDocument>> onlyA = batch.operations(Collections.singletonList(id("a")));
        Assertions.assertEquals(3, onlyA.size());
        Assertions.assertTrue(onlyA.get(0) instanceof ReplaceOneModel);
        Assertions.assertTrue(onlyA.get(1) instanceof DeleteOneModel);
        Assertions.assertEquals(
                doc("a", 3), ((ReplaceOneModel<BsonDocument>) onlyA.get(2)).getReplacement());
        Assertions.assertTrue(
                ((ReplaceOneModel<BsonDocument>) onlyA.get(2)).getReplaceOptions().isUpsert());
    }
}
