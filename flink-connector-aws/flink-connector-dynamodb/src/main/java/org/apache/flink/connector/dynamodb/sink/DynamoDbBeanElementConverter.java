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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collection;
import java.util.Map;

/**
 * A generic {@link ElementConverter} that uses the dynamodb-enhanced client to build a {@link
 * DynamoDbWriteRequest} from a POJO annotated with {@link DynamoDbBean}.
 *
 * <p>Supports all three write request types:
 *
 * <ul>
 *   <li>{@link DynamoDbWriteRequestType#PUT} - maps the full POJO to an item
 *   <li>{@link DynamoDbWriteRequestType#DELETE} - extracts only the primary key from the POJO
 *   <li>{@link DynamoDbWriteRequestType#UPDATE} - extracts the primary key from the POJO and uses
 *       the provided update expression
 * </ul>
 *
 * <p>Condition expressions can be set for any type to enable conditional writes via individual API
 * calls instead of {@code BatchWriteItem}.
 *
 * @param <InputT> The type of the {@link DynamoDbBean} to convert into {@link DynamoDbWriteRequest}
 */
@PublicEvolving
public class DynamoDbBeanElementConverter<InputT>
        implements ElementConverter<InputT, DynamoDbWriteRequest> {

    private static final long serialVersionUID = 1L;

    private final Class<InputT> recordType;
    private final boolean ignoreNulls;
    private final DynamoDbWriteRequestType type;
    private final String updateExpression;
    private final String conditionExpression;
    private final Map<String, String> expressionAttributeNames;
    private final Map<String, AttributeValue> expressionAttributeValues;
    private transient BeanTableSchema<InputT> tableSchema;
    private transient Collection<String> keyAttributeNames;

    public DynamoDbBeanElementConverter(final Class<InputT> recordType) {
        this(recordType, false);
    }

    public DynamoDbBeanElementConverter(final Class<InputT> recordType, final boolean ignoreNulls) {
        this(recordType, ignoreNulls, DynamoDbWriteRequestType.PUT, null, null, null, null);
    }

    private DynamoDbBeanElementConverter(
            final Class<InputT> recordType,
            final boolean ignoreNulls,
            final DynamoDbWriteRequestType type,
            final String updateExpression,
            final String conditionExpression,
            final Map<String, String> expressionAttributeNames,
            final Map<String, AttributeValue> expressionAttributeValues) {
        this.recordType = recordType;
        this.ignoreNulls = ignoreNulls;
        this.type = Preconditions.checkNotNull(type, "Type must not be null");
        this.updateExpression = updateExpression;
        this.conditionExpression = conditionExpression;
        this.expressionAttributeNames = expressionAttributeNames;
        this.expressionAttributeValues = expressionAttributeValues;

        if (type == DynamoDbWriteRequestType.UPDATE) {
            Preconditions.checkNotNull(
                    updateExpression,
                    "Update expression must not be null for UPDATE requests.");
        }

        // Attempt to create a table schema now to bubble up errors before starting job
        createTableSchema(recordType);
    }

    @Override
    public DynamoDbWriteRequest apply(InputT element, SinkWriter.Context context) {
        Preconditions.checkNotNull(tableSchema, "Table schema has not been initialized");
        Map<String, AttributeValue> item =
                type == DynamoDbWriteRequestType.PUT
                        ? tableSchema.itemToMap(element, ignoreNulls)
                        : tableSchema.itemToMap(element, keyAttributeNames);
        DynamoDbWriteRequest.Builder builder =
                new DynamoDbWriteRequest.Builder().setType(type).setItem(item);
        if (updateExpression != null) {
            builder.setUpdateExpression(updateExpression);
        }
        if (conditionExpression != null) {
            builder.setConditionExpression(conditionExpression);
        }
        if (expressionAttributeNames != null) {
            builder.setExpressionAttributeNames(expressionAttributeNames);
        }
        if (expressionAttributeValues != null) {
            builder.setExpressionAttributeValues(expressionAttributeValues);
        }
        return builder.build();
    }

    @Override
    public void open(WriterInitContext context) {
        tableSchema = createTableSchema(recordType);
        // DELETE and UPDATE only need the primary key attributes from the POJO, not the full item.
        // PUT uses the complete item map.
        if (type == DynamoDbWriteRequestType.DELETE || type == DynamoDbWriteRequestType.UPDATE) {
            keyAttributeNames = tableSchema.tableMetadata().primaryKeys();
        }
    }

    private BeanTableSchema<InputT> createTableSchema(final Class<InputT> recordType) {
        return BeanTableSchema.create(recordType);
    }

    /** Builder for {@link DynamoDbBeanElementConverter}. */
    public static class ElementConverterBuilder<InputT> {
        private final Class<InputT> recordType;
        private boolean ignoreNulls = false;
        private DynamoDbWriteRequestType type = DynamoDbWriteRequestType.PUT;
        private String updateExpression;
        private String conditionExpression;
        private Map<String, String> expressionAttributeNames;
        private Map<String, AttributeValue> expressionAttributeValues;

        private ElementConverterBuilder(Class<InputT> recordType) {
            this.recordType = recordType;
        }

        public ElementConverterBuilder<InputT> setIgnoreNulls(boolean ignoreNulls) {
            this.ignoreNulls = ignoreNulls;
            return this;
        }

        public ElementConverterBuilder<InputT> setType(DynamoDbWriteRequestType type) {
            this.type = type;
            return this;
        }

        public ElementConverterBuilder<InputT> setUpdateExpression(String updateExpression) {
            this.updateExpression = updateExpression;
            return this;
        }

        public ElementConverterBuilder<InputT> setConditionExpression(String conditionExpression) {
            this.conditionExpression = conditionExpression;
            return this;
        }

        public ElementConverterBuilder<InputT> setExpressionAttributeNames(
                Map<String, String> expressionAttributeNames) {
            this.expressionAttributeNames = expressionAttributeNames;
            return this;
        }

        public ElementConverterBuilder<InputT> setExpressionAttributeValues(
                Map<String, AttributeValue> expressionAttributeValues) {
            this.expressionAttributeValues = expressionAttributeValues;
            return this;
        }

        public DynamoDbBeanElementConverter<InputT> build() {
            Preconditions.checkArgument(
                    type != DynamoDbWriteRequestType.UPDATE || updateExpression != null,
                    "updateExpression is required for UPDATE type.");
            Preconditions.checkArgument(
                    type == DynamoDbWriteRequestType.UPDATE || updateExpression == null,
                    "updateExpression is only allowed for UPDATE type.");
            return new DynamoDbBeanElementConverter<>(
                    recordType,
                    ignoreNulls,
                    type,
                    updateExpression,
                    conditionExpression,
                    expressionAttributeNames,
                    expressionAttributeValues);
        }
    }

    public static <InputT> ElementConverterBuilder<InputT> builder(Class<InputT> recordType) {
        return new ElementConverterBuilder<>(recordType);
    }
}
