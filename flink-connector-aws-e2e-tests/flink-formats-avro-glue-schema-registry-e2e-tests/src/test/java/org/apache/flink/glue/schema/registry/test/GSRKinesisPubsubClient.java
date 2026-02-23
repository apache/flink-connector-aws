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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.kinesis.source.proxy.ListShardsStartingPosition;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Simple client to publish and retrieve messages, using the AWS Kinesis SDK, Flink Kinesis
 * Connectors and Glue Schema Registry classes.
 */
public class GSRKinesisPubsubClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(GSRKinesisPubsubClient.class);

    private final KinesisClient kinesisClient;
    private final GlueSchemaRegistrySerializationFacade serializationFacade;
    private final GlueSchemaRegistryDeserializationFacade deserializationFacade;

    public GSRKinesisPubsubClient(
            KinesisClient kinesisClient, AwsCredentialsProvider gsrCredentialsProvider) {
        this.kinesisClient = kinesisClient;
        this.serializationFacade = createSerializationFacade(gsrCredentialsProvider);
        this.deserializationFacade = createDeserializationFacade(gsrCredentialsProvider);
    }

    public void sendMessage(String schema, String streamName, GenericRecord msg) {
        try {
            UUID schemaVersionId =
                    serializationFacade.getOrRegisterSchemaVersion(
                            AWSSerializerInput.builder()
                                    .schemaDefinition(schema.toString())
                                    .dataFormat(DataFormat.AVRO.name())
                                    .schemaName(streamName)
                                    .transportName(streamName)
                                    .build());

            sendMessage(
                    streamName,
                    serializationFacade.serialize(DataFormat.AVRO, msg, schemaVersionId));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sendMessage(String topic, byte[]... messages) {
        for (List<byte[]> partition : Lists.partition(Arrays.asList(messages), 500)) {
            List<PutRecordsRequestEntry> entries =
                    partition.stream()
                            .map(
                                    msg ->
                                            PutRecordsRequestEntry.builder()
                                                    .partitionKey("fakePartitionKey")
                                                    .data(SdkBytes.fromByteArray(msg))
                                                    .build())
                            .collect(Collectors.toList());
            PutRecordsRequest requests =
                    PutRecordsRequest.builder().streamName(topic).records(entries).build();

            PutRecordsResponse putRecordsResponse = kinesisClient.putRecords(requests);
            for (PutRecordsResultEntry result : putRecordsResponse.records()) {
                LOGGER.debug("added record: {}", result.sequenceNumber());
            }
        }
    }

    private <T> List<T> readAllMessages(
            String streamName, String streamARN, Function<byte[], T> deserialiser) {
        List<Shard> shards = new ArrayList<>();

        ListShardsResponse listShardsResponse;
        String nextToken = null;
        do {
            listShardsResponse =
                    kinesisClient.listShards(
                            ListShardsRequest.builder()
                                    .streamName(nextToken == null ? streamName : null)
                                    .shardFilter(
                                            nextToken == null
                                                    ? ListShardsStartingPosition.fromStart()
                                                            .getShardFilter()
                                                    : null)
                                    .nextToken(nextToken)
                                    .build());

            shards.addAll(listShardsResponse.shards());
            nextToken = listShardsResponse.nextToken();
        } while (nextToken != null);

        int maxRecordsToFetch = 10;
        List<T> messages = new ArrayList<>();

        for (Shard shard : shards) {
            GetShardIteratorResponse getShardIteratorResponse =
                    kinesisClient.getShardIterator(
                            GetShardIteratorRequest.builder()
                                    .shardId(shard.shardId())
                                    .streamName(streamName)
                                    .streamARN(streamARN)
                                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                    .build());

            GetRecordsResponse getRecordsResponse =
                    kinesisClient.getRecords(
                            GetRecordsRequest.builder()
                                    .streamARN(streamARN)
                                    .shardIterator(getShardIteratorResponse.shardIterator())
                                    .limit(maxRecordsToFetch)
                                    .build());

            for (Record record : getRecordsResponse.records()) {
                messages.add(deserialiser.apply(record.data().asByteArray()));
            }
        }
        return messages;
    }

    public List<Object> readAllMessages(String streamName, String streamARN) {
        return readAllMessages(
                streamName,
                streamARN,
                bytes ->
                        deserializationFacade.deserialize(
                                AWSDeserializerInput.builder()
                                        .buffer(ByteBuffer.wrap(bytes))
                                        .transportName(streamName)
                                        .build()));
    }

    private Map<String, Object> getSerDeConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }

    private GlueSchemaRegistrySerializationFacade createSerializationFacade(
            final AwsCredentialsProvider credentialsProvider) {
        return GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(credentialsProvider)
                .glueSchemaRegistryConfiguration(
                        new GlueSchemaRegistryConfiguration(getSerDeConfigs()))
                .build();
    }

    private GlueSchemaRegistryDeserializationFacade createDeserializationFacade(
            final AwsCredentialsProvider credentialsProvider) {
        return GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(credentialsProvider)
                .configs(getSerDeConfigs())
                .build();
    }

    public void prepareStream(String streamName) throws Exception {
        final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(1, SECONDS)
                        .withConstantThroughput()
                        .build();

        kinesisClient.createStream(
                CreateStreamRequest.builder().streamName(streamName).shardCount(2).build());

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
}
