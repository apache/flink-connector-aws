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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Covers construction and sanity checking of {@link SqsSinkElementConverter}. */
class SqsSinkElementConverterTest {

    @Test
    void elementConverterWillComplainASerializationSchemaIsNotSetIfBuildIsCalledWithoutIt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> SqsSinkElementConverter.<String>builder().build())
                .withMessageContaining(
                        "No SerializationSchema was supplied to the SQS Sink builder.");
    }

    @Test
    void elementConverterUsesProvidedSchemaToSerializeRecord() {
        ElementConverter<String, SendMessageBatchRequestEntry> elementConverter =
                SqsSinkElementConverter.<String>builder()
                        .setSerializationSchema(new OpenCheckingStringSchema())
                        .build();

        String testString = "{many hands make light work;";

        SendMessageBatchRequestEntry serializedRecord = elementConverter.apply(testString, null);
        byte[] serializedString = (new OpenCheckingStringSchema()).serialize(testString);
        assertThat(serializedRecord.messageBody())
                .isEqualTo(new String(serializedString, StandardCharsets.UTF_8));
    }

    private static class OpenCheckingStringSchema extends SimpleStringSchema {

        private boolean isOpen = false;

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            super.open(context);
            isOpen = true;
        }

        public Boolean isOpen() {
            return isOpen;
        }
    }
}
