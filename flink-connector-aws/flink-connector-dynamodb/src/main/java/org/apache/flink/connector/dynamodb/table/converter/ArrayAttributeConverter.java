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

package org.apache.flink.connector.dynamodb.table.converter;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.stream.Collectors;

/** A converter between T[] and {@link AttributeValue}. */
@Internal
public class ArrayAttributeConverter<T> implements AttributeConverter<T[]> {

    private final AttributeConverter<T> tAttributeConverter;
    private final EnhancedType<T[]> enhancedType;

    public ArrayAttributeConverter(
            AttributeConverter<T> tAttributeConverter, EnhancedType<T[]> enhancedType) {
        this.tAttributeConverter = tAttributeConverter;
        this.enhancedType = enhancedType;
    }

    @Override
    public AttributeValue transformFrom(T[] input) {
        return AttributeValue.builder()
                .l(
                        Arrays.stream(input)
                                .map(tAttributeConverter::transformFrom)
                                .collect(Collectors.toList()))
                .build();
    }

    @Override
    public T[] transformTo(AttributeValue input) {
        throw new UnsupportedOperationException(
                "transformTo() is unsupported because the DynamoDB sink does not need it.");
    }

    @Override
    public EnhancedType<T[]> type() {
        return enhancedType;
    }

    @Override
    public AttributeValueType attributeValueType() {
        return AttributeValueType.L;
    }
}
