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

package org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class CosmosMongoConnectorException extends SeaTunnelRuntimeException {

    public CosmosMongoConnectorException(ErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    public CosmosMongoConnectorException(
            ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
    }

    public enum ErrorCode implements SeaTunnelErrorCode {
        CONNECT_FAILED("COSMOS-MONGO-01", "Failed to connect to MongoDB endpoint"),
        SNAPSHOT_READ_FAILED("COSMOS-MONGO-02", "Failed to read full snapshot"),
        CHANGE_STREAM_FAILED("COSMOS-MONGO-03", "Failed to read change stream"),
        WRITE_FAILED("COSMOS-MONGO-04", "Failed to write to DocumentDB"),
        ILLEGAL_ARGUMENT("COSMOS-MONGO-05", "Illegal connector argument");

        private final String code;
        private final String description;

        ErrorCode(String code, String description) {
            this.code = code;
            this.description = description;
        }

        @Override
        public String getCode() {
            return code;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
