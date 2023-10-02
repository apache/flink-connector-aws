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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisDeserializationSchema.Metadata;
import org.apache.flink.streaming.connectors.kinesis.testutils.StaticValueDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for methods in {@link RowDataKinesisDeserializationSchema} class. */
public class RowDataKinesisDeserializationSchemaTest {
    private static final String FIELD_NAME = "text_field";

    @Test
    public void testAddMetadataToDeserializedRecord() throws Exception {
        long timestamp = Instant.now().toEpochMilli();
        String shardName = "test shard";
        String sequenceNumber = "sequence number";
        String deserializedValue = "deserialized value";

        GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
        rowData.setField(0, StringData.fromString(deserializedValue));
        DataType dataType = DataTypes.ROW(DataTypes.FIELD(FIELD_NAME, DataTypes.STRING()));

        ScanTableSource.ScanContext scanContext = ScanRuntimeProviderContext.INSTANCE;
        TypeInformation<RowData> typeInformation = scanContext.createTypeInformation(dataType);

        RowDataKinesisDeserializationSchema rowDataKinesisDeserializationSchema =
                createSchema(rowData, typeInformation);

        RowData row =
                rowDataKinesisDeserializationSchema.deserialize(
                        deserializedValue.getBytes(StandardCharsets.UTF_8),
                        "partitionKey",
                        sequenceNumber,
                        timestamp,
                        "test_stream",
                        shardName);

        assertThat(row).isNotNull();
        assertThat(row.getString(0)).isEqualTo(StringData.fromString(deserializedValue));
        assertThat(row.getTimestamp(1, 0)).isEqualTo(TimestampData.fromEpochMillis(timestamp));
        assertThat(row.getString(2)).isEqualTo(StringData.fromString(sequenceNumber));
        assertThat(row.getString(3)).isEqualTo(StringData.fromString(shardName));
    }

    @Test
    public void testHandleNullDeserializationResult() throws Exception {
        ScanTableSource.ScanContext scanContext = ScanRuntimeProviderContext.INSTANCE;
        RowDataKinesisDeserializationSchema rowDataKinesisDeserializationSchema =
                createSchema(
                        null, scanContext.createTypeInformation(DataTypes.ROW(DataTypes.STRING())));

        RowData row =
                rowDataKinesisDeserializationSchema.deserialize(
                        new byte[0],
                        "partitionKey",
                        "sequence number",
                        Instant.now().toEpochMilli(),
                        "test_stream",
                        "test shard");

        assertThat(row).isNull();
    }

    private RowDataKinesisDeserializationSchema createSchema(
            RowData deserializedValue, TypeInformation<RowData> typeInformation) {

        DeserializationSchema<RowData> internalDeserializationSchema =
                new StaticValueDeserializationSchema<>(deserializedValue, typeInformation);

        return new RowDataKinesisDeserializationSchema(
                internalDeserializationSchema, typeInformation, Arrays.asList(Metadata.values()));
    }
}
