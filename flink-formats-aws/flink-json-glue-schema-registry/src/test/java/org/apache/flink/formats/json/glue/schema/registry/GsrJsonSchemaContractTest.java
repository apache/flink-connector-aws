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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards for two places where the JSON format used to publish or accept data that contradicts the
 * schema registered for the table, without an error:
 *
 * <ul>
 *   <li><b>Write, NOT NULL.</b> {@link JsonSchemaConverter} registers a {@code NOT NULL} column as
 *       {@code required} with a non-nullable type, but Flink's JSON serializer writes {@code null}
 *       for any null field and the GSR encode path skips validation. The record used to go out in
 *       violation of its own schema; it must be rejected naming the column.
 *   <li><b>Read, DECIMAL.</b> Flink's JSON reader turns a decimal whose integer part exceeds the
 *       reader's precision into {@code null} ({@code DecimalData.fromBigDecimal} contract). A
 *       producer with a wider schema silently lost data; it must fail naming value and type.
 * </ul>
 *
 * <p>Both are the JSON counterparts of the protobuf guards in {@code RowDataToProtobufConverter}
 * and {@code ProtobufToRowDataConverter}; the format modules are siblings and a fix in one has to
 * be checked against the others.
 */
class GsrJsonSchemaContractTest {

    private static final int GSR_HEADER_SIZE = 18;

    // ------------------------------------------------------------------------
    //  NOT NULL on the write path
    // ------------------------------------------------------------------------

    @Test
    void nullInNotNullTopLevelColumnIsRejected() throws Exception {
        RowType rowType =
                row(
                        field("name", new VarCharType(false, VarCharType.MAX_LENGTH)),
                        field("age", new IntType(false)));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        GenericRowData row = new GenericRowData(2);
        row.setField(0, StringData.fromString("Alice"));
        row.setField(1, null);

        assertThatThrownBy(() -> ser.serialize(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Column 'age' is declared NOT NULL");
    }

    @Test
    void nullInNullableColumnIsWrittenAsJsonNull() throws Exception {
        RowType rowType =
                row(
                        field("name", new VarCharType(false, VarCharType.MAX_LENGTH)),
                        field("age", new IntType(true)));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        GenericRowData row = new GenericRowData(2);
        row.setField(0, StringData.fromString("Alice"));
        row.setField(1, null);

        String json = payloadOf(ser.serialize(row));
        assertThat(json).isEqualTo("{\"name\":\"Alice\",\"age\":null}");
    }

    @Test
    void nullInNotNullNestedRowFieldIsRejectedWithPath() throws Exception {
        RowType inner = row(field("city", new VarCharType(false, VarCharType.MAX_LENGTH)));
        RowType rowType = row(field("address", inner.copy(false)));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, null);
        GenericRowData row = new GenericRowData(1);
        row.setField(0, innerRow);

        assertThatThrownBy(() -> ser.serialize(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Column 'address.city' is declared NOT NULL");
    }

    @Test
    void nullElementInArrayOfNotNullIsRejectedWithIndex() throws Exception {
        RowType rowType = row(field("tags", new ArrayType(false, new IntType(false))));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        GenericRowData row = new GenericRowData(1);
        row.setField(0, new GenericArrayData(new Object[] {1, null, 3}));

        assertThatThrownBy(() -> ser.serialize(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Column 'tags[1]' is declared NOT NULL");
    }

    @Test
    void nullMapValueOfNotNullIsRejected() throws Exception {
        RowType rowType =
                row(
                        field(
                                "scores",
                                new MapType(
                                        false,
                                        new VarCharType(false, VarCharType.MAX_LENGTH),
                                        new IntType(false))));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        Map<Object, Object> map = new HashMap<>();
        map.put(StringData.fromString("a"), null);
        GenericRowData row = new GenericRowData(1);
        row.setField(0, new GenericMapData(map));

        assertThatThrownBy(() -> ser.serialize(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Column 'scores[value 0]' is declared NOT NULL");
    }

    @Test
    void nullInsideNullableContainersIsAccepted() throws Exception {
        RowType inner = row(field("city", new VarCharType(true, VarCharType.MAX_LENGTH)));
        RowType rowType =
                row(
                        field("address", inner.copy(true)),
                        field("tags", new ArrayType(true, new IntType(true))));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);

        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, null);
        GenericRowData row = new GenericRowData(2);
        row.setField(0, innerRow);
        row.setField(1, new GenericArrayData(new Object[] {null}));

        assertThat(payloadOf(ser.serialize(row)))
                .isEqualTo("{\"address\":{\"city\":null},\"tags\":[null]}");
    }

    // ------------------------------------------------------------------------
    //  DECIMAL range on the read path
    // ------------------------------------------------------------------------

    @Test
    void decimalExceedingReaderPrecisionFailsInsteadOfReadingNull() throws Exception {
        // Writer registered DECIMAL(12,2); reader table declares DECIMAL(5,2).
        RowType reader = row(field("price", new DecimalType(false, 5, 2)));
        GsrJsonRowDataDeserializationSchema deser = newDeser(reader);

        byte[] encoded = gsrEncoded("{\"price\":123456.78}");

        assertThatThrownBy(() -> deser.deserialize(encoded))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to deserialize JSON")
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasStackTraceContaining("Value 123456.78 at 'price'")
                .hasStackTraceContaining("DECIMAL(5, 2) NOT NULL")
                .hasStackTraceContaining("6 digit(s) before the decimal point, 3 allowed");
    }

    @Test
    void decimalExceedingReaderPrecisionFailsOnNullableColumnToo() throws Exception {
        // On a nullable column the old behaviour was a silent null, not a NOT NULL violation.
        RowType reader = row(field("price", new DecimalType(true, 5, 2)));
        GsrJsonRowDataDeserializationSchema deser = newDeser(reader);

        assertThatThrownBy(() -> deser.deserialize(gsrEncoded("{\"price\":\"1000.00\"}")))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Value 1000.00 at 'price'");
    }

    @Test
    void decimalNarrowerScaleIsRoundedLikeCast() throws Exception {
        RowType reader = row(field("price", new DecimalType(false, 5, 2)));
        GsrJsonRowDataDeserializationSchema deser = newDeser(reader);

        RowData row = deser.deserialize(gsrEncoded("{\"price\":123.456}"));

        DecimalData expected = DecimalData.fromBigDecimal(new BigDecimal("123.46"), 5, 2);
        assertThat(row.getDecimal(0, 5, 2)).isEqualTo(expected);
    }

    @Test
    void decimalOverflowInsideNestedContainersIsReportedWithPath() throws Exception {
        RowType inner = row(field("amount", new DecimalType(false, 4, 1)));
        RowType reader =
                row(
                        field("lines", new ArrayType(false, inner.copy(false))),
                        field(
                                "byKey",
                                new MapType(
                                        false,
                                        new VarCharType(false, VarCharType.MAX_LENGTH),
                                        new DecimalType(false, 4, 1))));
        GsrJsonRowDataDeserializationSchema deser = newDeser(reader);

        assertThatThrownBy(
                        () ->
                                deser.deserialize(
                                        gsrEncoded(
                                                "{\"lines\":[{\"amount\":1.5},{\"amount\":12345.6}],"
                                                        + "\"byKey\":{}}")))
                .hasStackTraceContaining("at 'lines[1].amount'");

        assertThatThrownBy(
                        () ->
                                deser.deserialize(
                                        gsrEncoded(
                                                "{\"lines\":[],\"byKey\":{\"x\":1.0,\"y\":99999.9}}")))
                .hasStackTraceContaining("at 'byKey['y']'");
    }

    @Test
    void decimalWithinRangeAndNullsRoundTrip() throws Exception {
        RowType rowType =
                row(
                        field("price", new DecimalType(false, 5, 2)),
                        field("discount", new DecimalType(true, 5, 2)));
        GsrJsonRowDataSerializationSchema ser = newSer(rowType);
        GsrJsonRowDataDeserializationSchema deser = newDeser(rowType);

        GenericRowData row = new GenericRowData(2);
        row.setField(0, DecimalData.fromBigDecimal(new BigDecimal("999.99"), 5, 2));
        row.setField(1, null);

        RowData back = deser.deserialize(ser.serialize(row));
        assertThat(back.getDecimal(0, 5, 2).toBigDecimal()).isEqualTo(new BigDecimal("999.99"));
        assertThat(back.isNullAt(1)).isTrue();
    }

    @Test
    void malformedJsonStillFailsTheFlinkJsonWay() throws Exception {
        GsrJsonRowDataDeserializationSchema deser =
                newDeser(row(field("price", new DecimalType(false, 5, 2))));

        assertThatThrownBy(() -> deser.deserialize(gsrEncoded("{\"price\":")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to deserialize JSON");
    }

    // ------------------------------------------------------------------------
    //  Fixtures
    // ------------------------------------------------------------------------

    private static RowType row(RowType.RowField... fields) {
        return new RowType(false, Arrays.asList(fields));
    }

    private static RowType.RowField field(String name, LogicalType type) {
        return new RowType.RowField(name, type);
    }

    private static GsrJsonRowDataSerializationSchema newSer(RowType rowType) throws Exception {
        JsonRowDataSerializationSchema jsonSer =
                new JsonRowDataSerializationSchema(
                        rowType,
                        TimestampFormat.SQL,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        false,
                        false);
        GsrJsonRowDataSerializationSchema ser =
                new GsrJsonRowDataSerializationSchema(
                        rowType, jsonSer, "stream", "schema", offlineConfigs());
        ser.setSerializationFacade(new HeaderPrependingFacade());
        ser.open(null);
        return ser;
    }

    private static GsrJsonRowDataDeserializationSchema newDeser(RowType rowType) throws Exception {
        JsonRowDataDeserializationSchema jsonDeser =
                new JsonRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), false, false, TimestampFormat.SQL);
        GsrJsonRowDataDeserializationSchema deser =
                new GsrJsonRowDataDeserializationSchema(
                        rowType, jsonDeser, InternalTypeInfo.of(rowType), offlineConfigs());
        deser.setReader(bytes -> Arrays.copyOfRange(bytes, GSR_HEADER_SIZE, bytes.length));
        deser.open(null);
        return deser;
    }

    private static Map<String, Object> offlineConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        // Closed loopback port: nothing here may reach a registry.
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "http://127.0.0.1:1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, false);
        configs.put("aws.credentials.provider", "BASIC");
        configs.put("aws.credentials.provider.basic.accesskeyid", "x");
        configs.put("aws.credentials.provider.basic.secretkey", "x");
        return configs;
    }

    private static byte[] gsrEncoded(String json) {
        return prependHeader(json.getBytes(StandardCharsets.UTF_8));
    }

    private static String payloadOf(byte[] gsrEncoded) {
        return new String(
                Arrays.copyOfRange(gsrEncoded, GSR_HEADER_SIZE, gsrEncoded.length),
                StandardCharsets.UTF_8);
    }

    private static byte[] prependHeader(byte[] payload) {
        UUID schemaId = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(GSR_HEADER_SIZE + payload.length);
        buffer.put((byte) 0x03);
        buffer.put((byte) 0x00);
        buffer.putLong(schemaId.getMostSignificantBits());
        buffer.putLong(schemaId.getLeastSignificantBits());
        buffer.put(payload);
        return buffer.array();
    }

    /** Facade stand-in: prepends a header, never talks to a registry. */
    private static final class HeaderPrependingFacade
            extends GlueSchemaRegistrySerializationFacade {
        HeaderPrependingFacade() {
            super(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")),
                    null,
                    new GlueSchemaRegistryConfiguration(
                            Collections.singletonMap(
                                    AWSSchemaRegistryConstants.AWS_REGION, "us-east-1")),
                    Collections.singletonMap(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1"),
                    null);
        }

        @Override
        public byte[] encode(String transportName, Schema schema, byte[] data) {
            return prependHeader(data);
        }
    }
}
