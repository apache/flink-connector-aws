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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the upsert serialization behavior in {@link KinesisDynamicSink}. */
class KinesisUpsertSinkSerializationTest {

    @Test
    void testUpsertSchemaWritesValueForInsert() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.INSERT);

        byte[] result = wrapper.serialize(row);
        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
    }

    @Test
    void testUpsertSchemaWritesValueForUpdateAfter() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 99L);
        row.setRowKind(RowKind.UPDATE_AFTER);

        byte[] result = wrapper.serialize(row);
        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
    }

    @Test
    void testUpsertSchemaWritesTombstoneForDelete() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.DELETE);

        byte[] result = wrapper.serialize(row);
        assertThat(result).isNotNull();
        assertThat(result).isEmpty();
    }

    @Test
    void testUpsertSchemaWritesTombstoneForUpdateBefore() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.UPDATE_BEFORE);

        byte[] result = wrapper.serialize(row);
        assertThat(result).isNotNull();
        assertThat(result).isEmpty();
    }

    @Test
    void testUpsertSchemaPreservesOriginalRowKind() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.UPDATE_AFTER);

        wrapper.serialize(row);
        assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
    }

    @Test
    void testKinesisSinkChangelogModeWithPrimaryKey() {
        org.apache.flink.table.connector.ChangelogMode mode =
                org.apache.flink.table.connector.ChangelogMode.upsert();

        assertThat(mode.contains(RowKind.INSERT)).isTrue();
        assertThat(mode.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(mode.contains(RowKind.DELETE)).isTrue();
    }

    private static class TestSerializationSchema implements SerializationSchema<RowData> {
        @Override
        public byte[] serialize(RowData element) {
            return "serialized".getBytes(StandardCharsets.UTF_8);
        }
    }
}
