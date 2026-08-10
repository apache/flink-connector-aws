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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(result).isNotEmpty();
        assertThat(inner.observedKinds).containsExactly(RowKind.INSERT);
    }

    @Test
    void testUpsertSchemaNormalizesUpdateAfterToInsert() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 99L);
        row.setRowKind(RowKind.UPDATE_AFTER);

        byte[] result = wrapper.serialize(row);
        assertThat(result).isNotEmpty();
        // the wrapped (insert-only) format must see an INSERT row
        assertThat(inner.observedKinds).containsExactly(RowKind.INSERT);
        // the caller-visible RowKind must be restored after serialization
        assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
    }

    @Test
    void testUpsertSchemaRejectsDelete() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.DELETE);

        assertThatThrownBy(() -> wrapper.serialize(row))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DELETE");
        assertThat(inner.observedKinds).isEmpty();
    }

    @Test
    void testUpsertSchemaRejectsUpdateBefore() {
        TestSerializationSchema inner = new TestSerializationSchema();
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(inner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.UPDATE_BEFORE);

        assertThatThrownBy(() -> wrapper.serialize(row))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UPDATE_BEFORE");
        assertThat(inner.observedKinds).isEmpty();
    }

    @Test
    void testUpsertSchemaRestoresRowKindWhenSerializationFails() {
        SerializationSchema<RowData> throwingInner =
                element -> {
                    throw new RuntimeException("serialization failure");
                };
        KinesisDynamicSink.UpsertSerializationSchemaWrapper wrapper =
                new KinesisDynamicSink.UpsertSerializationSchemaWrapper(throwingInner);

        GenericRowData row = GenericRowData.of(StringData.fromString("key1"), 42L);
        row.setRowKind(RowKind.UPDATE_AFTER);

        assertThatThrownBy(() -> wrapper.serialize(row))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("serialization failure");
        // RowData instances may be reused: the RowKind must be restored even on failure
        assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
    }

    private static class TestSerializationSchema implements SerializationSchema<RowData> {

        private final List<RowKind> observedKinds = new ArrayList<>();

        @Override
        public byte[] serialize(RowData element) {
            observedKinds.add(element.getRowKind());
            return "serialized".getBytes(StandardCharsets.UTF_8);
        }
    }
}
