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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import static org.assertj.core.api.Assertions.assertThat;

class KinesisStreamsSinkElementConverterTest {
    @Test
    void elementConverterWillOpenSerializationSchema() {
        OpenCheckingStringSchema openCheckingStringSchema = new OpenCheckingStringSchema();
        ElementConverter<String, PutRecordsRequestEntry> elementConverter =
                KinesisStreamsSinkElementConverter.<String>builder()
                        .setSerializationSchema(openCheckingStringSchema)
                        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                        .build();
        elementConverter.open(null);
        assertThat(openCheckingStringSchema.isOpen()).isTrue();
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
