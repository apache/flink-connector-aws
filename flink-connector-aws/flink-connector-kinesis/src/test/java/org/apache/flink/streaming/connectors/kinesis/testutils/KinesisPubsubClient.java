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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Simple client to publish and retrieve messages, using the AWS Kinesis SDK and the Flink Kinesis
 * Connectos classes.
 */
public class KinesisPubsubClient {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisPubsubClient.class);

    private final AmazonKinesis kinesisClient;
    private final Properties properties;

    public KinesisPubsubClient(Properties properties) {
        this.kinesisClient = createClientWithCredentials(properties);
        this.properties = properties;
    }

    public void createTopic(String stream, int shards, Properties props) throws Exception {
        try {
            kinesisClient.describeStream(stream);
            kinesisClient.deleteStream(stream);
        } catch (ResourceNotFoundException rnfe) {
            // expected when stream doesn't exist
        }

        kinesisClient.createStream(stream, shards);
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(5));
        while (deadline.hasTimeLeft()) {
            try {
                Thread.sleep(250); // sleep for a bit for stream to be created
                if (kinesisClient.describeStream(stream).getStreamDescription().getShards().size()
                        != shards) {
                    // not fully created yet
                    continue;
                }
                break;
            } catch (ResourceNotFoundException rnfe) {
                // not ready yet
            }
        }
    }

    public void sendMessage(String topic, String... messages) {
        sendMessage(topic, Arrays.stream(messages).map(String::getBytes).toArray(byte[][]::new));
    }

    public void sendMessage(String topic, byte[]... messages) {
        for (List<byte[]> partition : Lists.partition(Arrays.asList(messages), 500)) {
            List<PutRecordsRequestEntry> entries =
                    partition.stream()
                            .map(
                                    msg ->
                                            new PutRecordsRequestEntry()
                                                    .withPartitionKey("fakePartitionKey")
                                                    .withData(ByteBuffer.wrap(msg)))
                            .collect(Collectors.toList());
            PutRecordsRequest requests =
                    new PutRecordsRequest().withStreamName(topic).withRecords(entries);
            PutRecordsResult putRecordResult = kinesisClient.putRecords(requests);
            for (PutRecordsResultEntry result : putRecordResult.getRecords()) {
                LOG.debug("added record: {}", result.getSequenceNumber());
            }
        }
    }

    public List<String> readAllMessages(String streamName) throws Exception {
        return readAllMessages(streamName, String::new);
    }

    public <T> List<T> readAllMessages(String streamName, Function<byte[], T> deserialiser)
            throws Exception {
        KinesisProxyInterface kinesisProxy = KinesisProxy.create(properties);
        Map<String, String> streamNamesWithLastSeenShardIds = new HashMap<>();
        streamNamesWithLastSeenShardIds.put(streamName, null);

        GetShardListResult shardListResult =
                kinesisProxy.getShardList(streamNamesWithLastSeenShardIds);
        int maxRecordsToFetch = 10;

        List<T> messages = new ArrayList<>();
        // retrieve records from all shards
        for (StreamShardHandle ssh : shardListResult.getRetrievedShardListOfStream(streamName)) {
            String shardIterator = kinesisProxy.getShardIterator(ssh, "TRIM_HORIZON", null);
            GetRecordsResult getRecordsResult =
                    kinesisProxy.getRecords(shardIterator, maxRecordsToFetch);
            List<Record> aggregatedRecords = getRecordsResult.getRecords();
            for (Record record : aggregatedRecords) {
                messages.add(deserialiser.apply(record.getData().array()));
            }
        }
        return messages;
    }

    private static AmazonKinesis createClientWithCredentials(Properties props)
            throws AmazonClientException {
        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(props);
        return AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                props.getProperty(ConsumerConfigConstants.AWS_ENDPOINT),
                                "us-east-1"))
                .build();
    }
}
