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

package org.apache.flink.connector.redshift.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import com.amazon.redshift.jdbc.RedshiftPreparedStatement;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/** Redshift Row Converter. */
public class RedshiftRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private RowType rowType;

    private final DeserializationConverter[] toInternalConverters;

    private final SerializationConverter[] toExternalConverters;

    public RedshiftRowConverter(RowType rowType) {
        this.rowType = Preconditions.checkNotNull(rowType);
        LogicalType[] logicalTypes =
                rowType.getFields().stream().map(RowField::getType).toArray(LogicalType[]::new);
        this.toInternalConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.toExternalConverters = new SerializationConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.toInternalConverters[i] = createToInternalConverter(rowType.getTypeAt(i));
            this.toExternalConverters[i] = createToExternalConverter(logicalTypes[i]);
        }
    }

    public RowData toInternal(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = resultSet.getObject(pos + 1);
            if (field != null) {
                genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
            } else {
                genericRowData.setField(pos, null);
            }
        }
        return genericRowData;
    }

    public void toExternal(RowData rowData, RedshiftPreparedStatement insertStatement)
            throws SQLException {
        for (int index = 0; index < rowData.getArity(); index++) {
            if (!rowData.isNullAt(index)) {
                toExternalConverters[index].serialize(rowData, index, insertStatement);
            } else {
                insertStatement.setObject(index + 1, null);
            }
        }
    }

    protected RedshiftRowConverter.DeserializationConverter createToInternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> RedshiftConverterUtils.BOOL_TRUE == ((Number) val).intValue();
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case BINARY:
            case VARBINARY:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) ((Date) val).toLocalDate().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> TimestampData.fromInstant(((Timestamp) val).toInstant());
            case CHAR:
            case VARCHAR:
                return val ->
                        val instanceof UUID
                                ? StringData.fromString(val.toString())
                                : StringData.fromString((String) val);
            case ARRAY:
            case MAP:
                return val -> RedshiftConverterUtils.toInternal(val, type);
            case ROW:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected RedshiftRowConverter.SerializationConverter createToExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(index + 1, val.getBoolean(index));
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(index + 1, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(index + 1, val.getDouble(index));
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (val, index, statement) -> statement.setInt(index + 1, val.getInt(index));
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (val, index, statement) -> statement.setLong(index + 1, val.getLong(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index + 1, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) ->
                        statement.setShort(index + 1, val.getShort(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, statement) ->
                        statement.setString(index + 1, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, statement) ->
                        statement.setBytes(index + 1, val.getBinary(index));
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) -> {
                    LocalTime localTime = LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L);
                    statement.setTimestamp(
                            index + 1, RedshiftConverterUtils.toEpochDayOneTimestamp(localTime));
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index + 1,
                                val.getTimestamp(index, timestampPrecision).toTimestamp());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localZonedTimestampPrecision =
                        ((LocalZonedTimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index + 1,
                                Timestamp.from(
                                        val.getTimestamp(index, localZonedTimestampPrecision)
                                                .toInstant()));
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index + 1,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case MAP:
                return (val, index, statement) ->
                        statement.setObject(
                                index + 1,
                                RedshiftConverterUtils.toExternal(val.getMap(index), type));
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        /** Convert an internal field to java object and fill into. */
        void serialize(RowData rowData, int index, RedshiftPreparedStatement statement)
                throws SQLException;
    }

    @FunctionalInterface
    interface DeserializationConverter extends Serializable {
        /** Convert an object to the internal data structure object. */
        Object deserialize(Object field) throws SQLException;
    }
}
