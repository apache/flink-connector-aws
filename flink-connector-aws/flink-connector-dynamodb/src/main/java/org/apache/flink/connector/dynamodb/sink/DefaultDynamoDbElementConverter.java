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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

/**
 * Default implementation of {@link ElementConverter} that lazily falls back to {@link
 * DynamoDbTypeInformedElementConverter}.
 */
@PublicEvolving
public class DefaultDynamoDbElementConverter<T>
        implements ElementConverter<T, DynamoDbWriteRequest> {

    private ElementConverter<T, DynamoDbWriteRequest> elementConverter;

    public DefaultDynamoDbElementConverter() {}

    @Override
    public DynamoDbWriteRequest apply(T t, SinkWriter.Context context) {
        if (elementConverter == null) {
            TypeInformation<T> typeInfo = (TypeInformation<T>) TypeInformation.of(t.getClass());
            if (!(typeInfo instanceof CompositeType<?>)) {
                throw new IllegalArgumentException("The input type must be a CompositeType.");
            }

            elementConverter =
                    new DynamoDbTypeInformedElementConverter<>((CompositeType<T>) typeInfo);
        }

        return elementConverter.apply(t, context);
    }

    @Override
    public void open(WriterInitContext context) {}
}
