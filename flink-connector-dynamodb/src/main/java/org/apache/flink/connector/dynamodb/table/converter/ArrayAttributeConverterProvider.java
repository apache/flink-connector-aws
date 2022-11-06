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
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/** Attribute converter provider for String Array. */
@Internal
public class ArrayAttributeConverterProvider implements AttributeConverterProvider {

    private static final AttributeConverterProvider defaultAttributeConverterProvider =
            AttributeConverterProvider.defaultProvider();

    @Override
    public <T> AttributeConverter<T> converterFor(EnhancedType<T> enhancedType) {
        if (enhancedType.equals(EnhancedType.of(String[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(String.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Boolean[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Boolean.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(BigDecimal[].class))) {
            return (AttributeConverter<T>)
                    getArrayAttributeConverter(BigDecimal.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Byte[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Byte.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Double[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Double.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Short[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Short.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Integer[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Integer.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Long[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Long.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Float[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Float.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(LocalDate[].class))) {
            return (AttributeConverter<T>)
                    getArrayAttributeConverter(LocalDate.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(LocalTime[].class))) {
            return (AttributeConverter<T>)
                    getArrayAttributeConverter(LocalTime.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(LocalDateTime[].class))) {
            return (AttributeConverter<T>)
                    getArrayAttributeConverter(LocalDateTime.class, enhancedType);
        } else if (enhancedType.equals(EnhancedType.of(Instant[].class))) {
            return (AttributeConverter<T>) getArrayAttributeConverter(Instant.class, enhancedType);
        }
        return null;
    }

    private <R, T> ArrayAttributeConverter<R> getArrayAttributeConverter(
            Class<R> clazz, EnhancedType<T> enhancedType) {
        return new ArrayAttributeConverter<>(
                defaultAttributeConverterProvider.converterFor(EnhancedType.of(clazz)),
                (EnhancedType<R[]>) enhancedType);
    }
}
