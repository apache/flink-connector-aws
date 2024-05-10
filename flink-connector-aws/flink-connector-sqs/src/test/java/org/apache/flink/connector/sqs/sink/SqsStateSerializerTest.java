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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

/** Test class for {@link SqsStateSerializer}. */
class SqsStateSerializerTest {

    private static final ElementConverter<String, SendMessageBatchRequestEntry> ELEMENT_CONVERTER =
            SqsSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Test
    void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<SendMessageBatchRequestEntry> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        SqsStateSerializer serializer = new SqsStateSerializer();

        BufferedRequestState<SendMessageBatchRequestEntry> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));

        assertThatBufferStatesAreEqual(actualState, expectedState);
    }

    private int getRequestSize(SendMessageBatchRequestEntry requestEntry) {
        return requestEntry.messageBody().length();
    }

    private <T extends Serializable> void assertThatBufferStatesAreEqual(
            BufferedRequestState<SendMessageBatchRequestEntry> actual,
            BufferedRequestState<SendMessageBatchRequestEntry> expected) {
        Assertions.assertThat(actual.getStateSize()).isEqualTo(expected.getStateSize());
        int actualLength = actual.getBufferedRequestEntries().size();
        Assertions.assertThat(actualLength).isEqualTo(expected.getBufferedRequestEntries().size());
        List<RequestEntryWrapper<SendMessageBatchRequestEntry>> actualRequests =
                actual.getBufferedRequestEntries();
        List<RequestEntryWrapper<SendMessageBatchRequestEntry>> expectedRequests =
                expected.getBufferedRequestEntries();

        for (int i = 0; i < actualLength; ++i) {
            Assertions.assertThat((actualRequests.get(i)).getRequestEntry().messageBody())
                    .isEqualTo((expectedRequests.get(i)).getRequestEntry().messageBody());
            Assertions.assertThat((actualRequests.get(i)).getRequestEntry().id())
                    .isEqualTo((expectedRequests.get(i)).getRequestEntry().id());
            Assertions.assertThat((actualRequests.get(i)).getRequestEntry().id()).isNotNull();
        }
    }
}
