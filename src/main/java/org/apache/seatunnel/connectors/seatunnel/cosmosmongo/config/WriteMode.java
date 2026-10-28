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

/** How the DocumentDB sink applies a flushed batch. */
public enum WriteMode {
    /**
     * Per collection, keep only the final state of every {@code _id} in the batch, then run one
     * {@code deleteMany({_id: {$in: ids}})} and one {@code insertMany(documents)} per chunk.
     * DocumentDB executes single-document write statements at roughly 2 ms each, while these
     * multi-document statements are about 40x faster. Documents of the chunk are briefly absent
     * from the target between the two statements.
     */
    DELETE_INSERT,
    /**
     * One {@code replaceOne(upsert)} / {@code deleteOne} per row in an ordered bulk write. The
     * target never misses a document, but it is much slower on DocumentDB.
     */
    UPSERT
}
