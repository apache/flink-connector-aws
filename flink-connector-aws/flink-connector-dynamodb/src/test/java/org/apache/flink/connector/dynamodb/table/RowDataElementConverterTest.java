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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link RowDataElementConverter}. */
public class RowDataElementConverterTest {

    private static final DataType DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("partition_key", DataTypes.STRING()),
                    DataTypes.FIELD("payload", DataTypes.STRING()));
    private static final RowDataElementConverter elementConverter =
            new RowDataElementConverter(DATA_TYPE, null);
    private static final SinkWriter.Context context = new UnusedSinkWriterContext();
    private static final RowDataToAttributeValueConverter rowDataToAttributeValueConverter =
            new RowDataToAttributeValueConverter(DATA_TYPE, null);

    private static final Set<String> primaryKeys = new HashSet<>(Collections.singletonList("partition_key"));
    private static final RowDataElementConverter elementConverterWithPK =
            new RowDataElementConverter(DATA_TYPE, primaryKeys);
    private static final RowDataToAttributeValueConverter rowDataToAttributeValueConverterWithPK =
            new RowDataToAttributeValueConverter(DATA_TYPE, primaryKeys);

    @Test
    void testInsert() {
        RowData rowData = createElement(RowKind.INSERT);
        DynamoDbWriteRequest actualWriteRequest = elementConverter.apply(rowData, context);
        DynamoDbWriteRequest expectedWriterequest =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.PUT)
                        .setItem(rowDataToAttributeValueConverter.convertRowData(rowData))
                        .build();

        assertThat(actualWriteRequest).usingRecursiveComparison().isEqualTo(expectedWriterequest);
    }

    @Test
    void testUpdateAfter() {
        RowData rowData = createElement(RowKind.UPDATE_AFTER);
        DynamoDbWriteRequest actualWriteRequest = elementConverter.apply(rowData, context);
        DynamoDbWriteRequest expectedWriterequest =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.PUT)
                        .setItem(rowDataToAttributeValueConverter.convertRowData(rowData))
                        .build();

        assertThat(actualWriteRequest).usingRecursiveComparison().isEqualTo(expectedWriterequest);
    }

    @Test
    void testUpdateBeforeIsUnsupported() {
        // UPDATE_BEFORE only makes sense in tables that do not have a uniquely identifiable index
        // for each row.
        // DynamoDB requires a partition key to be specified, so we do not support UPDATE_BEFORE
        RowData rowData = createElement(RowKind.UPDATE_BEFORE);

        assertThatExceptionOfType(TableException.class)
                .isThrownBy(() -> elementConverter.apply(rowData, context))
                .withMessageContaining("Unsupported message kind: UPDATE_BEFORE");
    }

    @Test
    void testDelete() {
        RowData rowData = createElement(RowKind.DELETE);
        // In case of DELETE, a set of Primary Key(s) is required.
        DynamoDbWriteRequest actualWriteRequest = elementConverterWithPK.apply(rowData, context);
        DynamoDbWriteRequest expectedWriterequest =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.DELETE)
                        .setItem(rowDataToAttributeValueConverterWithPK.convertRowData(rowData))
                        .build();

        assertThat(actualWriteRequest).usingRecursiveComparison().isEqualTo(expectedWriterequest);
    }

    @Test
    void testAttributeConverterReinitializedAfterSerialization()
            throws IOException, ClassNotFoundException {
        RowData rowData = createElement(RowKind.INSERT);

        RowDataElementConverter originalConverter = new RowDataElementConverter(DATA_TYPE, null);
        RowDataElementConverter transformedConverter =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(originalConverter),
                        this.getClass().getClassLoader());

        assertThat(transformedConverter).extracting("rowDataToAttributeValueConverter").isNull();

        DynamoDbWriteRequest actualWriteRequest = transformedConverter.apply(rowData, context);
        DynamoDbWriteRequest expectedWriterequest =
                DynamoDbWriteRequest.builder()
                        .setType(DynamoDbWriteRequestType.PUT)
                        .setItem(rowDataToAttributeValueConverter.convertRowData(rowData))
                        .build();

        assertThat(transformedConverter).extracting("rowDataToAttributeValueConverter").isNotNull();

        assertThat(actualWriteRequest).usingRecursiveComparison().isEqualTo(expectedWriterequest);
    }

    private RowData createElement(RowKind kind) {
        GenericRowData element = new GenericRowData(kind, 2);
        element.setField(0, StringData.fromString("some_partition_key"));
        element.setField(1, StringData.fromString("some_payload"));
        return element;
    }

    private static class UnusedSinkWriterContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long timestamp() {
            throw new UnsupportedOperationException();
        }
    }
}
