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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL-path integration test for the Kinesis sink in upsert mode ({@code PRIMARY KEY} defined on the
 * table). Verifies end-to-end against a Localstack Kinesis stream that:
 *
 * <ul>
 *   <li>an upsert changelog produced by a {@code GROUP BY} query is accepted and written, with the
 *       latest value per key readable from the stream,
 *   <li>the Kinesis partition key of every written record is derived from the primary key fields,
 *   <li>a delete-producing query (Top-N) is rejected at planning time, since the sink does not
 *       advertise {@code DELETE} in its changelog mode.
 * </ul>
 */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class KinesisUpsertTableSinkITCase {

    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";
    private static final String STREAM_NAME = "upsert_orders";
    private static final String STREAM_ARN =
            "arn:aws:kinesis:ap-southeast-1:000000000000:stream/" + STREAM_NAME;
    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Container
    private static final LocalstackContainer LOCALSTACK =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("localstack");

    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        LOCALSTACK.getEndpoint(), httpClient, KinesisClient.builder());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());
        tEnv.executeSql(createUpsertSinkTableStmt());
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    void testUpsertSinkWritesLatestValuePerKeyWithPrimaryKeyPartitioning() throws Exception {
        prepareStream(STREAM_NAME);

        // GROUP BY over an insert-only VALUES source produces an upsert changelog:
        // INSERT for the first row of a key, UPDATE_AFTER for subsequent rows.
        tEnv.executeSql(
                        "INSERT INTO upsert_orders "
                                + "SELECT user_id, COUNT(*), SUM(amount) "
                                + "FROM (VALUES ('alice', 5), ('alice', 7), ('bob', 3)) "
                                + "    AS orders (user_id, amount) "
                                + "GROUP BY user_id")
                .await();

        List<Record> records = readAllRecordsFromStream(3);

        // every changelog event is written as a full row: alice I+UA, bob I
        assertThat(records).hasSize(3);

        // the Kinesis partition key must be derived from the primary key fields
        for (Record record : records) {
            JsonNode row = OBJECT_MAPPER.readTree(record.data().asByteArray());
            assertThat(record.partitionKey()).isEqualTo(row.get("user_id").asText());
        }

        // a consumer materializing the stream (last record per key wins) must
        // converge to the final aggregate values
        Map<String, JsonNode> materialized = new HashMap<>();
        for (Record record : records) {
            JsonNode row = OBJECT_MAPPER.readTree(record.data().asByteArray());
            materialized.put(row.get("user_id").asText(), row);
        }
        assertThat(materialized).containsOnlyKeys("alice", "bob");
        assertThat(materialized.get("alice").get("order_count").asLong()).isEqualTo(2L);
        assertThat(materialized.get("alice").get("total_amount").asLong()).isEqualTo(12L);
        assertThat(materialized.get("bob").get("order_count").asLong()).isEqualTo(1L);
        assertThat(materialized.get("bob").get("total_amount").asLong()).isEqualTo(3L);
    }

    @Test
    void testUpsertSinkRejectsDeleteProducingQueryDuringPlanning() {
        // A Top-N query produces DELETE events when rows are displaced from the top-N.
        // The sink does not advertise DELETE in upsert mode, so planning must fail --
        // no job is submitted and no connection to Kinesis is made.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO upsert_orders "
                                                + "SELECT user_id, order_count, total_amount FROM ("
                                                + "  SELECT user_id, amount AS order_count, "
                                                + "         amount AS total_amount, "
                                                + "         ROW_NUMBER() OVER ("
                                                + "             ORDER BY amount DESC) AS row_num "
                                                + "  FROM (VALUES ('alice', CAST(5 AS BIGINT)), "
                                                + "               ('bob', CAST(3 AS BIGINT))) "
                                                + "      AS orders (user_id, amount)"
                                                + ") WHERE row_num <= 1"))
                .hasMessageContaining("doesn't support consuming delete changes");
    }

    private String createUpsertSinkTableStmt() {
        return "CREATE TABLE upsert_orders ("
                + "  user_id STRING,"
                + "  order_count BIGINT,"
                + "  total_amount BIGINT,"
                + "  PRIMARY KEY (user_id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'kinesis',"
                + ("  'stream.arn' = '" + STREAM_ARN + "',")
                + "  'aws.region' = 'us-east-1',"
                + ("  'aws.endpoint' = '" + LOCALSTACK.getEndpoint() + "',")
                + "  'aws.credentials.provider' = 'BASIC',"
                + "  'aws.credentials.basic.accesskeyid' = 'access-key',"
                + "  'aws.credentials.basic.secretkey' = 'secret-key',"
                + "  'aws.trust.all.certificates' = 'true',"
                + "  'aws.http.protocol.version' = 'HTTP1_1',"
                + "  'format' = 'json'"
                + ")";
    }

    private void prepareStream(String streamName) throws Exception {
        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (!streamExists(streamName)) {
            if (deadline.isOverdue()) {
                throw new RuntimeException("Failed to create stream within time");
            }
            Thread.sleep(500);
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

    private List<Record> readAllRecordsFromStream(int expectedMinimumCount)
            throws IOException, InterruptedException {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        List<Record> records;
        do {
            Thread.sleep(1000);
            String shardIterator =
                    kinesisClient
                            .getShardIterator(
                                    GetShardIteratorRequest.builder()
                                            .shardId(DEFAULT_FIRST_SHARD_NAME)
                                            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                            .streamName(STREAM_NAME)
                                            .build())
                            .shardIterator();
            records =
                    kinesisClient
                            .getRecords(
                                    GetRecordsRequest.builder()
                                            .shardIterator(shardIterator)
                                            .build())
                            .records();
        } while (deadline.hasTimeLeft() && records.size() < expectedMinimumCount);
        return records;
    }
}
