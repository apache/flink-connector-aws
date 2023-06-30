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
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

/**
 * A generic {@link ElementConverter} that uses the dynamodb-enhanced client to build a {@link
 * DynamoDbWriteRequest} from a POJO annotated with {@link DynamoDbBean}.
 *
 * @param <InputT> The type of the {@link DynamoDbBean} to convert into {@link DynamoDbWriteRequest}
 */
@PublicEvolving
public class DynamoDbBeanElementConverter<InputT>
        implements ElementConverter<InputT, DynamoDbWriteRequest> {

    private static final long serialVersionUID = 1L;

    private final Class<InputT> recordType;
    private final boolean ignoreNulls;
    private transient BeanTableSchema<InputT> tableSchema;

    public DynamoDbBeanElementConverter(final Class<InputT> recordType) {
        this(recordType, false);
    }

    public DynamoDbBeanElementConverter(final Class<InputT> recordType, final boolean ignoreNulls) {
        this.recordType = recordType;
        this.ignoreNulls = ignoreNulls;

        // Attempt to create a table schema now to bubble up errors before starting job
        createTableSchema(recordType);
    }

    @Override
    public DynamoDbWriteRequest apply(InputT element, SinkWriter.Context context) {
        if (tableSchema == null) {
            // We have to lazily initialize this because BeanTableSchema is not serializable and
            // there is no open() method on ElementConverter (FLINK-29938)
            tableSchema = createTableSchema(recordType);
        }

        return new DynamoDbWriteRequest.Builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(tableSchema.itemToMap(element, ignoreNulls))
                .build();
    }

    private BeanTableSchema<InputT> createTableSchema(final Class<InputT> recordType) {
        return BeanTableSchema.create(recordType);
    }
}
