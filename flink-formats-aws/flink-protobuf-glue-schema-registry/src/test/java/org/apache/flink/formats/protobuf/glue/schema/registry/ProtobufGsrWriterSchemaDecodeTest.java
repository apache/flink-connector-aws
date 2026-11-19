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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for review finding <b>B1</b>: Protobuf decode must resolve the <b>writer</b>
 * schema registered in AWS Glue Schema Registry and parse the on-wire bytes with a descriptor built
 * from that writer schema, mapping fields to the local {@code RowType} by <b>name</b> — rather than
 * (as the pre-fix code did) reinterpreting the bytes against a descriptor synthesized from the
 * local table DDL, which silently mis-maps whenever the writer's field order / tags differ from the
 * local columns.
 *
 * <p>Each test drives {@link GsrProtobufRowDataDeserializationSchema} through a hand-written {@link
 * GsrProtobufReader} fake that returns a writer schema definition and payload independent of the
 * local {@code RowType}, so no live registry is required.
 */
class ProtobufGsrWriterSchemaDecodeTest {

    private static final String SCHEMA_NAME = "TestMessage";

    /**
     * B1 core proof: the writer schema declares {@code name} (tag 1, string) then {@code age} (tag
     * 2, int32); the local table DDL declares the columns in the <b>opposite</b> order ({@code age}
     * then {@code name}). If decode built its descriptor from the local {@code RowType} it would
     * try to parse tag 1 as an {@code int32} (age) and tag 2 as a {@code string} (name) and either
     * throw or produce garbage. Because decode uses the GSR-registered writer descriptor and maps
     * by name, both columns decode correctly regardless of local column order.
     */
    @Test
    void testDecodeUsesWriterSchemaAndMapsByName() throws Exception {
        // Writer schema as registered in GSR: name (tag 1), age (tag 2).
        RowType writerRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        GenericRowData writerRow = new GenericRowData(2);
        writerRow.setField(0, StringData.fromString("Alice"));
        writerRow.setField(1, 30);
        byte[] payload = encode(writerRow, writerRowType);
        String writerSchema =
                ProtobufSchemaConverter.convertToProtobufSchema(writerRowType, SCHEMA_NAME);

        // Local table DDL: columns declared in the OPPOSITE order.
        RowType localRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("age", new IntType()),
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH))));

        RowData decoded = decode(localRowType, writerSchema, payload);

        assertThat(decoded).isNotNull();
        // Mapped by name against the writer schema, not by local position/tag.
        assertThat(decoded.getInt(0)).isEqualTo(30);
        assertThat(decoded.getString(1).toString()).isEqualTo("Alice");
    }

    /**
     * B1: a NULLABLE local column that is absent from the GSR-registered writer schema decodes to
     * {@code null} (the writer never produced it), rather than reading a bogus on-wire tag.
     */
    @Test
    void testNullableColumnMissingFromWriterSchemaDecodesToNull() throws Exception {
        RowType writerRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        GenericRowData writerRow = new GenericRowData(2);
        writerRow.setField(0, StringData.fromString("Bob"));
        writerRow.setField(1, 41);
        byte[] payload = encode(writerRow, writerRowType);
        String writerSchema =
                ProtobufSchemaConverter.convertToProtobufSchema(writerRowType, SCHEMA_NAME);

        // Local DDL adds a nullable 'nickname' column the writer schema does not carry.
        RowType localRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType()),
                                new RowType.RowField(
                                        "nickname",
                                        new VarCharType(true, VarCharType.MAX_LENGTH))));

        RowData decoded = decode(localRowType, writerSchema, payload);

        assertThat(decoded).isNotNull();
        assertThat(decoded.getString(0).toString()).isEqualTo("Bob");
        assertThat(decoded.getInt(1)).isEqualTo(41);
        assertThat(decoded.isNullAt(2)).isTrue();
    }

    /**
     * B1: a NOT NULL local column that is absent from the GSR-registered writer schema fails fast
     * with a clear error, instead of silently decoding a wrong or default value.
     */
    @Test
    void testNotNullColumnMissingFromWriterSchemaThrowsClearError() throws Exception {
        RowType writerRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType())));

        GenericRowData writerRow = new GenericRowData(2);
        writerRow.setField(0, StringData.fromString("Carol"));
        writerRow.setField(1, 52);
        byte[] payload = encode(writerRow, writerRowType);
        String writerSchema =
                ProtobufSchemaConverter.convertToProtobufSchema(writerRowType, SCHEMA_NAME);

        // Local DDL requires a NOT NULL 'email' column the writer schema does not carry.
        RowType localRowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("age", new IntType()),
                                new RowType.RowField(
                                        "email", new VarCharType(false, VarCharType.MAX_LENGTH))));

        assertThatThrownBy(() -> decode(localRowType, writerSchema, payload))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("email")
                .hasMessageContaining("NOT NULL")
                .hasMessageContaining("writer schema");
    }

    /**
     * Encodes a RowData into raw Protobuf bytes using a descriptor built from the WRITER RowType —
     * this stands in for what an upstream writer registered in GSR would have produced.
     */
    private static byte[] encode(RowData row, RowType writerRowType) {
        DynamicMessage message =
                RowDataToProtobufConverter.convertRowData(
                        row, writerRowType, buildDescriptor(writerRowType));
        return message.toByteArray();
    }

    /**
     * Runs the decode path of {@link GsrProtobufRowDataDeserializationSchema} against the given
     * local {@code RowType}, with a fake reader that returns the supplied writer schema definition
     * and payload (independent of the local DDL).
     */
    private static RowData decode(RowType localRowType, String writerSchema, byte[] payload)
            throws Exception {
        GsrProtobufRowDataDeserializationSchema deser =
                new GsrProtobufRowDataDeserializationSchema(
                        localRowType,
                        InternalTypeInfo.of(localRowType),
                        SCHEMA_NAME,
                        new HashMap<>());
        deser.setReader(new StaticGsrReader(writerSchema, payload));
        deser.open(null);
        return deser.deserialize(payload);
    }

    private static Descriptors.Descriptor buildDescriptor(RowType rowType) {
        try {
            DescriptorProtos.FileDescriptorProto fileProto =
                    ProtobufSchemaConverter.buildFileDescriptorProto(rowType, SCHEMA_NAME);
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[] {});
            return fileDescriptor.findMessageTypeByName(SCHEMA_NAME);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fake {@link GsrProtobufReader} that returns a fixed writer schema definition and payload,
     * standing in for the GSR facade resolving the writer schema from the record's schema-version
     * UUID. Both values are independent of the local {@code RowType}, which is exactly what lets
     * the tests prove decode is driven by the writer schema.
     */
    private static final class StaticGsrReader implements GsrProtobufReader {
        private static final long serialVersionUID = 1L;
        private final String writerSchemaDefinition;
        private final byte[] payload;

        StaticGsrReader(String writerSchemaDefinition, byte[] payload) {
            this.writerSchemaDefinition = writerSchemaDefinition;
            this.payload = payload;
        }

        @Override
        public String writerSchemaDefinition(byte[] gsrEncoded) {
            return writerSchemaDefinition;
        }

        @Override
        public byte[] actualData(byte[] gsrEncoded) {
            return payload;
        }
    }
}
