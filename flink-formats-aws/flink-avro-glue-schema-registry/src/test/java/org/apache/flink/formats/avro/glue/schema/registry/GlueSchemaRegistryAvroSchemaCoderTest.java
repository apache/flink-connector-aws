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

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GlueSchemaRegistryAvroSchemaCoder}. */
class GlueSchemaRegistryAvroSchemaCoderTest {
    private static final String testTopic = "Test-Topic";
    private static final String schemaName = "User-Topic";
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    private static final byte[] actualBytes =
            new byte[] {12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116};
    private static final byte[] specificBytes =
            new byte[] {
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99,
                8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116
            };
    private static final Map<String, Object> configs = new HashMap<>();
    private static final Map<String, String> metadata = new HashMap<>();
    private static final AwsCredentialsProvider credentialsProvider =
            DefaultCredentialsProvider.builder().build();
    private static Schema userSchema;
    private static User userDefinedPojo;
    private static GlueSchemaRegistryInputStreamDeserializer mockInputStreamDeserializer;
    private static GlueSchemaRegistryOutputStreamSerializer mockOutputStreamSerializer;

    @BeforeAll
    static void setup() throws IOException {
        metadata.put("test-key", "test-value");
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, testTopic);

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);

        Schema.Parser parser = new Schema.Parser();
        userSchema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        userDefinedPojo =
                User.newBuilder()
                        .setName("test_avro_schema")
                        .setFavoriteColor("violet")
                        .setFavoriteNumber(10)
                        .build();

        mockInputStreamDeserializer = new MockGlueSchemaRegistryInputStreamDeserializer();
        mockOutputStreamSerializer = new MockGlueSchemaRegistryOutputStreamSerializer();
    }

    /** Test whether constructor works. */
    @Test
    void testConstructor_withConfigs_succeeds() {
        assertThat(new GlueSchemaRegistryAvroSchemaCoder(testTopic, configs)).isNotNull();
    }

    /** Test whether readSchema method works. */
    @Test
    void testReadSchema_withValidParams_succeeds() throws IOException {
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(mockInputStreamDeserializer);
        Schema resultSchema =
                glueSchemaRegistryAvroSchemaCoder.readSchema(buildByteArrayInputStream());

        assertThat(resultSchema).isEqualTo(userSchema);
    }

    /** Test whether writeSchema method works. */
    @Test
    void testWriteSchema_withValidParams_succeeds() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(actualBytes);
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(mockOutputStreamSerializer);
        glueSchemaRegistryAvroSchemaCoder.writeSchema(userSchema, outputStream);

        testForSerializedData(outputStream.toByteArray());
    }

    /** Test whether writeSchema method throws exception if auto registration un-enabled. */
    @Test
    void testWriteSchema_withoutAutoRegistration_throwsException() {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, false);
        SchemaByDefinitionFetcher fetcher =
                new SchemaByDefinitionFetcher(
                        new MockAWSSchemaRegistryClient(),
                        new GlueSchemaRegistryConfiguration(configs));

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .schemaByDefinitionFetcher(fetcher)
                        .credentialProvider(credentialsProvider)
                        .glueSchemaRegistryConfiguration(
                                new GlueSchemaRegistryConfiguration(configs))
                        .build();

        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(
                        testTopic, configs, glueSchemaRegistrySerializationFacade);
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(glueSchemaRegistryOutputStreamSerializer);

        assertThatThrownBy(
                        () ->
                                glueSchemaRegistryAvroSchemaCoder.writeSchema(
                                        userSchema, new ByteArrayOutputStream()))
                .isInstanceOf(AWSSchemaRegistryException.class)
                .hasMessage(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG);
    }

    private void testForSerializedData(byte[] serializedData) {
        assertThat(serializedData).isNotNull();

        ByteBuffer buffer = getByteBuffer(serializedData);
        byte headerVersionByte = getByte(buffer);

        assertThat(headerVersionByte).isEqualTo(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
    }

    private ByteArrayInputStream buildByteArrayInputStream() {
        return new ByteArrayInputStream(specificBytes);
    }

    private ByteBuffer getByteBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    private Byte getByte(ByteBuffer buffer) {
        return buffer.get();
    }

    private static class MockGlueSchemaRegistryInputStreamDeserializer
            extends GlueSchemaRegistryInputStreamDeserializer {

        public MockGlueSchemaRegistryInputStreamDeserializer() {
            super((GlueSchemaRegistryDeserializationFacade) null);
        }

        @Override
        public Schema getSchemaAndDeserializedStream(InputStream in) {
            return userSchema;
        }
    }

    private static class MockGlueSchemaRegistryOutputStreamSerializer
            extends GlueSchemaRegistryOutputStreamSerializer {

        public MockGlueSchemaRegistryOutputStreamSerializer() {
            super(testTopic, configs, null);
        }

        @Override
        public void registerSchemaAndSerializeStream(Schema schema, OutputStream out, byte[] data)
                throws IOException {
            out.write(specificBytes);
        }
    }

    private static class MockAWSSchemaRegistryClient extends AWSSchemaRegistryClient {

        public MockAWSSchemaRegistryClient() {
            super(credentialsProvider, new GlueSchemaRegistryConfiguration(configs));
        }

        @Override
        public UUID getSchemaVersionIdByDefinition(
                String schemaDefinition, String schemaName, String dataFormat) {
            EntityNotFoundException entityNotFoundException =
                    EntityNotFoundException.builder()
                            .message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                            .build();
            throw new AWSSchemaRegistryException(entityNotFoundException);
        }
    }
}
