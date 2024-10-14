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

package org.apache.flink.connector.kinesis.table.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.connectors.kinesis.testutils.TestUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.Timeout;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test for Kinesis Streams Table API Sink using Kinesalite. */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class KinesisStreamsTableApiIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisStreamsTableApiIT.class);
    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

    private static final String ORDERS_STREAM = "orders";
    private static final String LARGE_ORDERS_STREAM = "large_orders";
    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;

    private final Path sqlConnectorKinesisJar =
            ResourceTestUtils.getResource(".*kinesis-streams.jar");
    private static final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @Container
    public static final LocalstackContainer LOCALSTACK_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withEnv("AWS_CBOR_DISABLE", "1")
                    .withEnv(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.kinesis-streams.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .withLogConsumer((log) -> LOGGER.info(log.getUtf8String()))
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .environmentVariable("AWS_CBOR_DISABLE", "1")
                    .environmentVariable(
                            "FLINK_ENV_JAVA_OPTS",
                            "-Dorg.apache.flink.kinesis-streams.shaded.com.amazonaws.sdk.disableCertChecking -Daws.cborEnabled=false")
                    .network(network)
                    .logger(LOGGER)
                    .dependsOn(LOCALSTACK_CONTAINER)
                    .build();

    public static final FlinkContainers FLINK =
            FlinkContainers.builder().withTestcontainersSettings(TESTCONTAINERS_SETTINGS).build();

    @BeforeClass
    public static void setupFlink() throws Exception {
        FLINK.start();
    }

    @AfterClass
    public static void stopFlink() {
        FLINK.stop();
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        LOCALSTACK_CONTAINER.getEndpoint(), httpClient, KinesisClient.builder());
        prepareStream(ORDERS_STREAM);
        prepareStream(LARGE_ORDERS_STREAM);
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    public void testTableApiSourceAndSink() throws Exception {
        List<Order> smallOrders = ImmutableList.of(new Order("A", 5), new Order("B", 10));

        // filter-large-orders.sql is supposed to preserve orders with quantity > 10
        List<Order> expected =
                ImmutableList.of(new Order("C", 15), new Order("D", 20), new Order("E", 25));

        smallOrders.forEach(
                order -> TestUtil.sendMessage(ORDERS_STREAM, kinesisClient, toJson(order)));
        expected.forEach(
                order -> TestUtil.sendMessage(ORDERS_STREAM, kinesisClient, toJson(order)));

        executeSqlStatements(readSqlFile("filter-large-orders.sql"));

        // result order is not guaranteed
        List<Order> result = readAllOrdersFromKinesis();
        assertThat(result).contains(expected.toArray(new Order[0]));
    }

    private void prepareStream(String streamName) throws Exception {
        final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(1, SECONDS)
                        .withConstantThroughput()
                        .build();

        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (!rateLimiter.getWhenReady(() -> streamExists(streamName))) {
            if (deadline.isOverdue()) {
                throw new RuntimeException("Failed to create stream within time");
            }
        }
    }

    private boolean streamExists(final String streamName) {
        try {
            return kinesisClient
                            .describeStream(
                                    DescribeStreamRequest.builder().streamName(streamName).build())
                            .streamDescription()
                            .streamStatus()
                    == StreamStatus.ACTIVE;
        } catch (Exception e) {
            return false;
        }
    }

    private List<Order> readAllOrdersFromKinesis() throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        List<Order> orders;
        do {
            Thread.sleep(1000);
            orders =
                    readMessagesFromStream(
                            recordBytes -> fromJson(new String(recordBytes), Order.class),
                            LARGE_ORDERS_STREAM);

        } while (deadline.hasTimeLeft() && orders.size() < 3);

        return orders;
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorKinesisJar)
                        .build());
    }

    private List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(Paths.get(getClass().getResource("/" + resourceName).toURI()));
    }

    private <T> T fromJson(final String json, final Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Test Failure.", e);
        }
    }

    private <T> List<T> readMessagesFromStream(Function<byte[], T> deserialiser, String streamName)
            throws Exception {

        String shardIterator =
                kinesisClient
                        .getShardIterator(
                                GetShardIteratorRequest.builder()
                                        .shardId(DEFAULT_FIRST_SHARD_NAME)
                                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                        .streamName(streamName)
                                        .build())
                        .shardIterator();

        List<Record> records =
                kinesisClient
                        .getRecords(
                                GetRecordsRequest.builder().shardIterator(shardIterator).build())
                        .records();
        List<T> messages = new ArrayList<>();
        records.forEach(record -> messages.add(deserialiser.apply(record.data().asByteArray())));
        return messages;
    }

    private <T> String toJson(final T object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Test Failure.", e);
        }
    }

    /** POJO class for orders used by e2e test. */
    public static class Order {
        private final String code;
        private final int quantity;

        public Order(
                @JsonProperty("code") final String code, @JsonProperty("quantity") int quantity) {
            this.code = code;
            this.quantity = quantity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Order order = (Order) o;
            return quantity == order.quantity && Objects.equals(code, order.code);
        }

        public String getCode() {
            return code;
        }

        public int getQuantity() {
            return quantity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, quantity);
        }

        @Override
        public String toString() {
            return String.format("Order{code: %s, quantity: %d}", code, quantity);
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        registerModules(objectMapper);
        return objectMapper;
    }

    private static void registerModules(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule())
                .registerModule((new Jdk8Module()).configureAbsentsAsNulls(true))
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
