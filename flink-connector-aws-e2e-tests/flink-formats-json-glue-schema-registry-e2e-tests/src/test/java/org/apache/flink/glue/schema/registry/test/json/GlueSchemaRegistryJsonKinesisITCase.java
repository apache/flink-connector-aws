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

package org.apache.flink.glue.schema.registry.test.json;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.formats.json.glue.schema.registry.GlueSchemaRegistryJsonDeserializationSchema;
import org.apache.flink.formats.json.glue.schema.registry.GlueSchemaRegistryJsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test for Glue Schema Registry Json format using Localstack. */
public class GlueSchemaRegistryJsonKinesisITCase extends TestLogger {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(GlueSchemaRegistryJsonKinesisITCase.class);

    private static final String INPUT_STREAM = "gsr_json_input_stream";
    private static final String OUTPUT_STREAM = "gsr_json_output_stream";
    private static final String INPUT_STREAM_ARN =
            "arn:aws:kinesis:ap-southeast-1:000000000000:stream/gsr_json_input_stream";
    private static final String OUTPUT_STREAM_ARN =
            "arn:aws:kinesis:ap-southeast-1:000000000000:stream/gsr_json_output_stream";

    private static final String PARTITION_KEY = "fakePartitionKey";

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_SECRET_KEY");

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

    private StreamExecutionEnvironment env;
    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;
    private GSRKinesisPubsubClient gsrKinesisPubsubClient;

    @ClassRule
    public static LocalstackContainer mockKinesisContainer =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withNetworkAliases("localstack");

    @Before
    public void setup() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        Assume.assumeTrue(
                "Access key not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(ACCESS_KEY));
        Assume.assumeTrue(
                "Secret key not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(SECRET_KEY));

        StaticCredentialsProvider gsrCredentialsProvider =
                StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        mockKinesisContainer.getEndpoint(), httpClient, KinesisClient.builder());
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        gsrKinesisPubsubClient = new GSRKinesisPubsubClient(kinesisClient, gsrCredentialsProvider);

        gsrKinesisPubsubClient.prepareStream(INPUT_STREAM);
        gsrKinesisPubsubClient.prepareStream(OUTPUT_STREAM);

        LOGGER.info("Done setting up the localstack.");
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    public void testGSRJsonGenericFormatWithFlink() throws Exception {

        List<JsonDataWithSchema> messages = getGenericRecords();
        for (JsonDataWithSchema msg : messages) {
            gsrKinesisPubsubClient.sendMessage(msg.getSchema(), INPUT_STREAM, msg);
        }
        log.info("generated records");

        DataStream<JsonDataWithSchema> input =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "source")
                        .returns(TypeInformation.of(JsonDataWithSchema.class));

        input.sinkTo(createSink());
        env.executeAsync();

        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(60));
        List<Object> results =
                gsrKinesisPubsubClient.readAllMessages(OUTPUT_STREAM, OUTPUT_STREAM_ARN);
        while (deadline.hasTimeLeft() && results.size() < messages.size()) {
            log.info("waiting for results..");
            Thread.sleep(1000);
            results = gsrKinesisPubsubClient.readAllMessages(OUTPUT_STREAM, OUTPUT_STREAM_ARN);
        }
        log.info("results: {}", results);

        assertThat(results).containsExactlyInAnyOrderElementsOf(messages);
    }

    private KinesisStreamsSource<JsonDataWithSchema> createSource() {
        Configuration sourceConfig = new Configuration();

        sourceConfig.setString(AWS_ENDPOINT, mockKinesisContainer.getEndpoint());
        sourceConfig.setString(AWS_ACCESS_KEY_ID, "accessKeyId");
        sourceConfig.setString(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        sourceConfig.setString(AWS_REGION, Region.AP_SOUTHEAST_1.toString());
        sourceConfig.setString(TRUST_ALL_CERTIFICATES, "true");
        sourceConfig.setString(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        sourceConfig.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                KinesisSourceConfigOptions.InitialPosition
                        .TRIM_HORIZON); // This is optional, by default connector will read from
        // LATEST

        return KinesisStreamsSource.<JsonDataWithSchema>builder()
                .setStreamArn(INPUT_STREAM_ARN)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                JsonDataWithSchema.class, INPUT_STREAM, getConfigs()))
                .build();
    }

    private KinesisStreamsSink<JsonDataWithSchema> createSink() {
        Properties properties = createConfig(mockKinesisContainer.getEndpoint());

        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

        return KinesisStreamsSink.<JsonDataWithSchema>builder()
                .setStreamArn(OUTPUT_STREAM_ARN)
                .setSerializationSchema(
                        new GlueSchemaRegistryJsonSerializationSchema<>(
                                OUTPUT_STREAM, getConfigs()))
                .setKinesisClientProperties(properties)
                .setPartitionKeyGenerator(
                        (PartitionKeyGenerator<JsonDataWithSchema>)
                                jsonDataWithSchema -> PARTITION_KEY)
                .build();
    }

    private List<JsonDataWithSchema> getGenericRecords() {
        List<JsonDataWithSchema> records = new ArrayList<>();
        String schema =
                "{\"$id\":\"https://example.com/address.schema.json\","
                        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
                        + "\"type\":\"object\","
                        + "\"properties\":{"
                        + "\"f1\":{\"type\":\"string\"},"
                        + "\"f2\":{\"type\":\"integer\",\"maximum\":10000}}}";
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"olympic\",\"f2\":2020}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"iphone\",\"f2\":12}").build());
        records.add(
                JsonDataWithSchema.builder(schema, "{\"f1\":\"Stranger Things\",\"f2\":4}")
                        .build());
        records.add(
                JsonDataWithSchema.builder(
                                schema, "{\"f1\":\"Friends\",\"f2\":6,\"f3\":\"coming soon\"}")
                        .build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"Porsche\",\"f2\":911}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"Ferrari\",\"f2\":488}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"McLaren\",\"f2\":720}").build());
        records.add(
                JsonDataWithSchema.builder(
                                schema, "{\"f1\":\"Panorama\",\"f2\":360,\"f3\":\"Fantastic!\"}")
                        .build());
        return records;
    }

    private Map<String, Object> getConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        return configs;
    }
}
