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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Type-coverage tests for the {@code protobuf-glue} format, exercising the review fixes:
 *
 * <ul>
 *   <li><b>B4</b> — TIMESTAMP/DATE/TIME/DECIMAL encode via the correct accessors (no {@code
 *       ClassCastException}) and decode symmetrically.
 *   <li><b>C3</b> — genuinely unsupported complex types (ARRAY/MAP/ROW/...) fail fast instead of
 *       being silently coerced to {@code string}.
 *   <li><b>C4</b> — column names that are not valid proto identifiers are sanitized so descriptor
 *       construction does not throw {@code DescriptorValidationException}.
 * </ul>
 */
class ProtobufTypeCoverageTest {

    private static final String SCHEMA_NAME = "TestMessage";

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

    private static RowData roundTrip(RowData in, RowType rowType) throws Exception {
        Descriptors.Descriptor descriptor = buildDescriptor(rowType);
        DynamicMessage message = RowDataToProtobufConverter.convertRowData(in, rowType, descriptor);
        DynamicMessage reparsed = DynamicMessage.parseFrom(descriptor, message.toByteArray());
        return ProtobufToRowDataConverter.convertToRowData(reparsed, rowType);
    }

    /** B4: TIMESTAMP (epoch millis int64) round-trips without ClassCastException. */
    @Test
    void testTimestampRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("ts", new TimestampType(3)),
                                new RowType.RowField("id", new IntType())));
        long millis = 1_700_000_000_123L;
        GenericRowData in = new GenericRowData(2);
        in.setField(0, TimestampData.fromEpochMillis(millis));
        in.setField(1, 7);

        RowData out = roundTrip(in, rowType);
        assertThat(out.getTimestamp(0, 3).getMillisecond()).isEqualTo(millis);
        assertThat(out.getInt(1)).isEqualTo(7);
    }

    /** B4: DATE (epoch day int32) and TIME (millis-of-day int32) round-trip. */
    @Test
    void testDateAndTimeRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("d", new DateType()),
                                new RowType.RowField("t", new TimeType(3))));
        int epochDay = 20_000; // 2024-10-04
        int millisOfDay = 45_296_000; // 12:34:56
        GenericRowData in = new GenericRowData(2);
        in.setField(0, epochDay);
        in.setField(1, millisOfDay);

        RowData out = roundTrip(in, rowType);
        assertThat(out.getInt(0)).isEqualTo(epochDay);
        assertThat(out.getInt(1)).isEqualTo(millisOfDay);
    }

    /** B4: DECIMAL round-trips losslessly via its BigDecimal text form. */
    @Test
    void testDecimalRoundTrip() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(new RowType.RowField("amount", new DecimalType(10, 2))));
        BigDecimal value = new BigDecimal("12345.67");
        GenericRowData in = new GenericRowData(1);
        in.setField(0, DecimalData.fromBigDecimal(value, 10, 2));

        RowData out = roundTrip(in, rowType);
        assertThat(out.getDecimal(0, 10, 2).toBigDecimal()).isEqualByComparingTo(value);
    }

    // ---- Review lenses: composition of writer and reader types, swallowed conditions ----------

    /**
     * Asymmetry: the writer serializes DECIMAL as text with its own scale; the reader rescales to
     * its declared type. A narrower reader scale rounds like CAST does, a narrower reader precision
     * must fail rather than yield null.
     */
    @Test
    void testDecimalWrittenWiderThanReaderType() throws Exception {
        RowType writer =
                new RowType(
                        false,
                        Arrays.asList(new RowType.RowField("amount", new DecimalType(12, 4))));
        Descriptors.Descriptor descriptor = buildDescriptor(writer);

        // Same integer digits, more fractional digits: rounds HALF_UP to the reader's scale.
        RowType readerNarrowScale =
                new RowType(
                        false,
                        Arrays.asList(new RowType.RowField("amount", new DecimalType(12, 2))));
        GenericRowData in = new GenericRowData(1);
        in.setField(0, DecimalData.fromBigDecimal(new BigDecimal("1.2345"), 12, 4));
        DynamicMessage message = RowDataToProtobufConverter.convertRowData(in, writer, descriptor);
        RowData out =
                ProtobufToRowDataConverter.convertToRowData(
                        DynamicMessage.parseFrom(descriptor, message.toByteArray()),
                        readerNarrowScale);
        assertThat(out.getDecimal(0, 12, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("1.23"));

        // Integer part does not fit the reader's precision: fail with the numbers, not null.
        RowType readerNarrowPrecision =
                new RowType(
                        false,
                        Arrays.asList(new RowType.RowField("amount", new DecimalType(4, 2))));
        GenericRowData big = new GenericRowData(1);
        big.setField(0, DecimalData.fromBigDecimal(new BigDecimal("12345.6789"), 12, 4));
        DynamicMessage bigMessage =
                RowDataToProtobufConverter.convertRowData(big, writer, descriptor);
        assertThatThrownBy(
                        () ->
                                ProtobufToRowDataConverter.convertToRowData(
                                        DynamicMessage.parseFrom(
                                                descriptor, bigMessage.toByteArray()),
                                        readerNarrowPrecision))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12345.6789")
                .hasMessageContaining("DECIMAL(4, 2)");
    }

    /**
     * Swallow: timestamps travel as epoch millis, so TIMESTAMP(6)/(9) would lose digits silently.
     * The schema converter must reject them; TIMESTAMP(3) and below stay supported.
     */
    @Test
    void testTimestampPrecisionAboveMillisIsRejected() {
        for (LogicalType type :
                Arrays.asList(
                        new TimestampType(6),
                        new TimestampType(9),
                        new LocalZonedTimestampType(6),
                        new TimeType(6))) {
            RowType rowType = new RowType(false, Arrays.asList(new RowType.RowField("ts", type)));
            assertThatThrownBy(
                            () ->
                                    ProtobufSchemaConverter.convertToProtobufSchema(
                                            rowType, SCHEMA_NAME))
                    .as(type.asSummaryString())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("millisecond precision");
            assertThatThrownBy(() -> buildDescriptor(rowType))
                    .as(type.asSummaryString() + " (descriptor)")
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (LogicalType type :
                Arrays.asList(new TimestampType(3), new TimestampType(0), new TimeType(0))) {
            RowType rowType = new RowType(false, Arrays.asList(new RowType.RowField("ts", type)));
            assertThat(buildDescriptor(rowType).findFieldByName("ts")).isNotNull();
        }
    }

    /**
     * Asymmetry: two columns that sanitize to the same proto identifier would produce a schema
     * declaring the field twice. Reject it with both column names.
     */
    @Test
    void testCollidingSanitizedFieldNamesAreRejected() {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("a b", new IntType()),
                                new RowType.RowField("a-b", new IntType())));
        assertThatThrownBy(
                        () -> ProtobufSchemaConverter.convertToProtobufSchema(rowType, SCHEMA_NAME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'a b'")
                .hasMessageContaining("'a-b'")
                .hasMessageContaining("'a_b'");
        assertThatThrownBy(
                        () ->
                                ProtobufSchemaConverter.buildFileDescriptorProto(
                                        rowType, SCHEMA_NAME))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Swallow: a null in a NOT NULL column must not be written as the proto type default. */
    @Test
    void testNullInNotNullColumnFailsOnWrite() {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("id", new IntType(false)),
                                new RowType.RowField("name", new VarCharType(true, 10))));
        Descriptors.Descriptor descriptor = buildDescriptor(rowType);
        GenericRowData in = new GenericRowData(2);
        in.setField(0, null);
        in.setField(1, null);
        assertThatThrownBy(() -> RowDataToProtobufConverter.convertRowData(in, rowType, descriptor))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'id'")
                .hasMessageContaining("NOT NULL");
    }

    /** C4: a column name with a space / leading digit is sanitized and still round-trips. */
    @Test
    void testFieldNameSanitizationDoesNotThrow() throws Exception {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "user name", new VarCharType(VarCharType.MAX_LENGTH)),
                                new RowType.RowField("1st_place", new IntType())));

        Descriptors.Descriptor descriptor = buildDescriptor(rowType);
        // Descriptor built without DescriptorValidationException; names are valid proto idents.
        assertThat(descriptor.findFieldByName("user_name")).isNotNull();
        assertThat(descriptor.findFieldByName("_1st_place")).isNotNull();

        GenericRowData in = new GenericRowData(2);
        in.setField(0, StringData.fromString("Alice"));
        in.setField(1, 42);
        RowData out = roundTrip(in, rowType);
        assertThat(out.getString(0).toString()).isEqualTo("Alice");
        assertThat(out.getInt(1)).isEqualTo(42);
    }

    /** C4: original SQL column name is preserved as the proto field's json_name. */
    @Test
    void testOriginalNamePreservedAsJsonName() {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField(
                                        "user name", new VarCharType(VarCharType.MAX_LENGTH))));
        Descriptors.Descriptor descriptor = buildDescriptor(rowType);
        Descriptors.FieldDescriptor fd = descriptor.findFieldByName("user_name");
        assertThat(fd).isNotNull();
        assertThat(fd.toProto().getJsonName()).isEqualTo("user name");
    }

    /** C3: an unsupported complex type (ARRAY) fails fast rather than coercing to string. */
    @Test
    void testUnsupportedComplexTypeFailsFast() {
        RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(new RowType.RowField("tags", new ArrayType(new IntType()))));
        assertThatThrownBy(
                        () ->
                                ProtobufSchemaConverter.buildFileDescriptorProto(
                                        rowType, SCHEMA_NAME))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support");
    }
}
