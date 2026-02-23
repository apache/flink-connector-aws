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

package org.apache.flink.glue.schema.registry.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

import java.io.IOException;
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

/** End-to-end test for Glue Schema Registry AVRO format using Localstack. */
public class GlueSchemaRegistryAvroKinesisITCase extends TestLogger {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(GlueSchemaRegistryAvroKinesisITCase.class);

    private static final String INPUT_STREAM = "gsr_avro_input_stream";
    private static final String OUTPUT_STREAM = "gsr_avro_output_stream";
    private static final String INPUT_STREAM_ARN =
            "arn:aws:kinesis:ap-southeast-1:000000000000:stream/gsr_avro_input_stream";
    private static final String OUTPUT_STREAM_ARN =
            "arn:aws:kinesis:ap-southeast-1:000000000000:stream/gsr_avro_output_stream";

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

        List<GenericRecord> messages = getRecords();

        for (GenericRecord msg : messages) {
            gsrKinesisPubsubClient.sendMessage(getSchema().toString(), INPUT_STREAM, msg);
        }
        log.info("generated records");

        DataStream<GenericRecord> input =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "source")
                        .returns(new GenericRecordAvroTypeInfo(getSchema()));

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

    private KinesisStreamsSource<GenericRecord> createSource() throws IOException {
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

        return KinesisStreamsSource.<GenericRecord>builder()
                .setStreamArn(INPUT_STREAM_ARN)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                getSchema(), getConfigs()))
                .build();
    }

    private KinesisStreamsSink<GenericRecord> createSink() throws Exception {
        Properties properties = createConfig(mockKinesisContainer.getEndpoint());

        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

        return KinesisStreamsSink.<GenericRecord>builder()
                .setStreamArn(OUTPUT_STREAM_ARN)
                .setSerializationSchema(
                        GlueSchemaRegistryAvroSerializationSchema.forGeneric(
                                getSchema(), OUTPUT_STREAM, getConfigs()))
                .setKinesisClientProperties(properties)
                .setPartitionKeyGenerator(
                        (PartitionKeyGenerator<GenericRecord>) jsonDataWithSchema -> PARTITION_KEY)
                .build();
    }

    private Map<String, Object> getConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }

    private Schema getSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse(
                GlueSchemaRegistryAvroKinesisITCase.class
                        .getClassLoader()
                        .getResourceAsStream("avro/user.avsc"));
    }

    private List<GenericRecord> getRecords() throws IOException {
        Schema userSchema = getSchema();

        GenericRecord sansa = new GenericData.Record(userSchema);
        sansa.put("name", "Sansa");
        sansa.put("favorite_number", 99);
        sansa.put("favorite_color", "white");

        GenericRecord harry = new GenericData.Record(userSchema);
        harry.put("name", "Harry");
        harry.put("favorite_number", 10);
        harry.put("favorite_color", "black");

        GenericRecord hermione = new GenericData.Record(userSchema);
        hermione.put("name", "Hermione");
        hermione.put("favorite_number", 1);
        hermione.put("favorite_color", "red");

        GenericRecord ron = new GenericData.Record(userSchema);
        ron.put("name", "Ron");
        ron.put("favorite_number", 18);
        ron.put("favorite_color", "green");

        GenericRecord jay = new GenericData.Record(userSchema);
        jay.put("name", "Jay");
        jay.put("favorite_number", 0);
        jay.put("favorite_color", "blue");

        List<GenericRecord> records = new ArrayList<>();
        records.add(sansa);
        records.add(harry);
        records.add(hermione);
        records.add(ron);
        records.add(jay);
        return records;
    }
}
