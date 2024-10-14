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

package org.apache.flink.connectors.kinesis.testutils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Test utility methods. */
public class TestUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtil.class);

    public static void sendMessage(
            String streamName, KinesisClient kinesisClient, String... messages) {
        sendMessage(
                streamName,
                kinesisClient,
                Arrays.stream(messages).map(String::getBytes).toArray(byte[][]::new));
    }

    public static void sendMessage(
            String streamName, KinesisClient kinesisClient, byte[]... messages) {
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
                    PutRecordsRequest.builder().streamName(streamName).records(entries).build();
            PutRecordsResponse putRecordResult = kinesisClient.putRecords(requests);
            for (PutRecordsResultEntry result : putRecordResult.records()) {
                LOGGER.info(
                        "added record: {} to the shard {}",
                        result.sequenceNumber(),
                        result.shardId());
            }
        }
    }
}
