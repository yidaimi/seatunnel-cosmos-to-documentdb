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

import org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

import static org.apache.seatunnel.connectors.seatunnel.cosmosmongo.exception.CosmosMongoConnectorException.ErrorCode.CONNECT_FAILED;

/** Builds MongoDB clients for Cosmos DB / DocumentDB. */
public final class MongoClientFactory {

    private MongoClientFactory() {}

    /**
     * Creates a client from a connection string.
     *
     * <p>Neither Cosmos DB for MongoDB (RU) nor Amazon DocumentDB supports retryable writes, so
     * {@code retryWrites} is forced to {@code false} unless the URI sets it explicitly. The Java
     * driver ignores {@code tlsCAFile}, therefore a PEM bundle can be passed via {@code caFile}.
     *
     * @param uri MongoDB connection string
     * @param caFile optional PEM bundle with trusted CA certificates, may be null
     * @param appName application name reported to the server
     */
    public static MongoClient create(String uri, String caFile, String appName) {
        try {
            ConnectionString connectionString = new ConnectionString(uri);
            MongoClientSettings.Builder builder =
                    MongoClientSettings.builder()
                            .applyConnectionString(connectionString)
                            .applicationName(appName);
            if (connectionString.getRetryWritesValue() == null) {
                builder.retryWrites(false);
            }
            if (caFile != null && !caFile.trim().isEmpty()) {
                SSLContext sslContext = sslContextFromPem(caFile.trim());
                builder.applyToSslSettings(ssl -> ssl.enabled(true).context(sslContext));
            }
            return MongoClients.create(builder.build());
        } catch (CosmosMongoConnectorException e) {
            throw e;
        } catch (Exception e) {
            // Never put the URI in the message: it usually contains the password.
            throw new CosmosMongoConnectorException(
                    CONNECT_FAILED, "Failed to create MongoDB client for " + appName, e);
        }
    }

    static SSLContext sslContextFromPem(String caFile) {
        try (InputStream in = Files.newInputStream(Paths.get(caFile))) {
            Collection<? extends Certificate> certs =
                    CertificateFactory.getInstance("X.509").generateCertificates(in);
            if (certs.isEmpty()) {
                throw new IllegalArgumentException("No certificate found in " + caFile);
            }
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int i = 0;
            for (Certificate cert : certs) {
                trustStore.setCertificateEntry("ca-" + i++, cert);
            }
            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        } catch (Exception e) {
            throw new CosmosMongoConnectorException(
                    CONNECT_FAILED, "Failed to load TLS CA file " + caFile, e);
        }
    }
}
