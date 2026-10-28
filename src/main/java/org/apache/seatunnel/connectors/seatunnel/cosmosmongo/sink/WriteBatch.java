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
import org.bson.BsonValue;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Buffered writes of one target collection.
 *
 * <p>Every row carries the complete state of a document (full document or deleted), so the last
 * row per {@code _id} fully determines the result of the whole batch. The batch therefore keeps:
 *
 * <ul>
 *   <li>the final state per {@code _id}, used by {@code delete_insert} mode;
 *   <li>the original operations in arrival order, used by {@code upsert} mode and as the fallback
 *       when {@code insertMany} reports duplicate keys.
 * </ul>
 *
 * <p>Not thread safe; guarded by the writer.
 */
final class WriteBatch {

    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);

    /** Final state per _id; a null value means the document is deleted. */
    private final Map<BsonValue, BsonDocument> finalState = new LinkedHashMap<>();

    private final List<Op> ops = new ArrayList<>();

    void upsert(BsonValue id, BsonDocument document) {
        finalState.put(id, document);
        ops.add(new Op(id, new ReplaceOneModel<>(new BsonDocument("_id", id), document, UPSERT)));
    }

    void delete(BsonValue id) {
        finalState.put(id, null);
        ops.add(new Op(id, new DeleteOneModel<>(new BsonDocument("_id", id))));
    }

    /** Number of buffered rows (not distinct ids). */
    int size() {
        return ops.size();
    }

    /** Distinct ids in first-seen order. */
    List<BsonValue> ids() {
        return new ArrayList<>(finalState.keySet());
    }

    /** Final documents of the given ids; deleted ids are skipped. */
    List<BsonDocument> documents(Collection<BsonValue> ids) {
        List<BsonDocument> documents = new ArrayList<>(ids.size());
        for (BsonValue id : ids) {
            BsonDocument document = finalState.get(id);
            if (document != null) {
                documents.add(document);
            }
        }
        return documents;
    }

    /** All operations in arrival order. */
    List<WriteModel<BsonDocument>> operations() {
        List<WriteModel<BsonDocument>> models = new ArrayList<>(ops.size());
        for (Op op : ops) {
            models.add(op.model);
        }
        return models;
    }

    /** Operations touching the given ids, in arrival order. */
    List<WriteModel<BsonDocument>> operations(Collection<BsonValue> ids) {
        Set<BsonValue> wanted = new HashSet<>(ids);
        List<WriteModel<BsonDocument>> models = new ArrayList<>();
        for (Op op : ops) {
            if (wanted.contains(op.id)) {
                models.add(op.model);
            }
        }
        return models;
    }

    private static final class Op {
        final BsonValue id;
        final WriteModel<BsonDocument> model;

        Op(BsonValue id, WriteModel<BsonDocument> model) {
            this.id = id;
            this.model = model;
        }
    }
}
