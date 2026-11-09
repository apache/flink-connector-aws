/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GlueSchemaRegistrySchemaFetcher} using a mock GlueClient. */
class GlueSchemaRegistrySchemaFetcherTest {

    private static final String REGISTRY_NAME = "test-registry";
    private static final String SCHEMA_NAME = "test-schema";

    @Test
    void testFetchSchemaReturnsValidSchema() throws Exception {
        Schema expected =
                SchemaBuilder.record("TestRecord")
                        .namespace("com.example")
                        .fields()
                        .requiredString("id")
                        .requiredInt("value")
                        .endRecord();

        AtomicReference<GetSchemaVersionRequest> capturedRequest = new AtomicReference<>();

        GlueClient mockClient =
                createMockGlueClient(
                        request -> capturedRequest.set(request),
                        GetSchemaVersionResponse.builder()
                                .schemaDefinition(expected.toString())
                                .build());

        GlueSchemaRegistrySchemaFetcher fetcher = new GlueSchemaRegistrySchemaFetcher(mockClient);

        Schema result = fetcher.fetchSchema(REGISTRY_NAME, SCHEMA_NAME);

        assertThat(result).isNotNull();
        assertThat(result.getNamespace()).isEqualTo("com.example");
        assertThat(result.getName()).isEqualTo("TestRecord");
        assertThat(result.getFields()).hasSize(2);

        // Verify the request was constructed correctly
        GetSchemaVersionRequest req = capturedRequest.get();
        assertThat(req.schemaId().registryName()).isEqualTo(REGISTRY_NAME);
        assertThat(req.schemaId().schemaName()).isEqualTo(SCHEMA_NAME);
        assertThat(req.schemaVersionNumber().latestVersion()).isTrue();
    }

    @Test
    void testFetchSchemaReturnsNullForEmptyDefinition() throws Exception {
        GlueClient mockClient =
                createMockGlueClient(
                        request -> {},
                        GetSchemaVersionResponse.builder().schemaDefinition("").build());

        GlueSchemaRegistrySchemaFetcher fetcher = new GlueSchemaRegistrySchemaFetcher(mockClient);

        Schema result = fetcher.fetchSchema(REGISTRY_NAME, SCHEMA_NAME);

        assertThat(result).isNull();
    }

    @Test
    void testFetchSchemaReturnsNullForNullDefinition() throws Exception {
        GlueClient mockClient =
                createMockGlueClient(request -> {}, GetSchemaVersionResponse.builder().build());

        GlueSchemaRegistrySchemaFetcher fetcher = new GlueSchemaRegistrySchemaFetcher(mockClient);

        Schema result = fetcher.fetchSchema(REGISTRY_NAME, SCHEMA_NAME);

        assertThat(result).isNull();
    }

    @Test
    void testFetchSchemaPropagatesGlueClientException() {
        GlueClient mockClient =
                createThrowingMockGlueClient(new RuntimeException("Service unavailable"));

        GlueSchemaRegistrySchemaFetcher fetcher = new GlueSchemaRegistrySchemaFetcher(mockClient);

        assertThatThrownBy(() -> fetcher.fetchSchema(REGISTRY_NAME, SCHEMA_NAME))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Service unavailable");
    }

    /**
     * Creates a mock GlueClient that captures the request and returns a fixed response.
     *
     * <p>Uses a proxy to avoid depending on Mockito.
     */
    private static GlueClient createMockGlueClient(
            Consumer<GetSchemaVersionRequest> requestCaptor, GetSchemaVersionResponse response) {
        return new GlueClientStub() {
            @Override
            public GetSchemaVersionResponse getSchemaVersion(GetSchemaVersionRequest request) {
                requestCaptor.accept(request);
                return response;
            }
        };
    }

    /** Creates a mock GlueClient that throws on getSchemaVersion. */
    private static GlueClient createThrowingMockGlueClient(RuntimeException exception) {
        return new GlueClientStub() {
            @Override
            public GetSchemaVersionResponse getSchemaVersion(GetSchemaVersionRequest request) {
                throw exception;
            }
        };
    }

    /**
     * Minimal stub of GlueClient. Only getSchemaVersion is used by the fetcher; other methods throw
     * UnsupportedOperationException if called.
     */
    private abstract static class GlueClientStub implements GlueClient {

        @Override
        public String serviceName() {
            return "glue";
        }

        @Override
        public void close() {}
    }
}
