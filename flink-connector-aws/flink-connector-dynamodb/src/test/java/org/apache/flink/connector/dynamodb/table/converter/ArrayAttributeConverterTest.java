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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link ArrayAttributeConverter}. */
public class ArrayAttributeConverterTest {

    @Test
    void testTransformFrom() {
        String[] initialStringArray = {"Some", "test", "strings"};

        ArrayAttributeConverter<String> arrayAttributeConverter =
                new ArrayAttributeConverter<>(
                        AttributeConverterProvider.defaultProvider()
                                .converterFor(EnhancedType.of(String.class)),
                        EnhancedType.of(String[].class));

        AttributeValue actualAttributeValue =
                arrayAttributeConverter.transformFrom(initialStringArray);
        AttributeValue expectedAttributeValue =
                AttributeValue.builder()
                        .l(
                                Arrays.stream(initialStringArray)
                                        .map(s -> AttributeValue.builder().s(s).build())
                                        .collect(Collectors.toList()))
                        .build();

        assertThat(actualAttributeValue).isEqualTo(expectedAttributeValue);
    }

    @Test
    void testTransformTo() {
        String[] expectedStringArray = {"Some", "test", "strings"};
        AttributeValue initialAttributeValue =
                AttributeValue.builder()
                        .l(
                                Arrays.stream(expectedStringArray)
                                        .map(s -> AttributeValue.builder().s(s).build())
                                        .collect(Collectors.toList()))
                        .build();

        ArrayAttributeConverter<String> arrayAttributeConverter =
                new ArrayAttributeConverter<>(
                        AttributeConverterProvider.defaultProvider()
                                .converterFor(EnhancedType.of(String.class)),
                        EnhancedType.of(String[].class));

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> arrayAttributeConverter.transformTo(initialAttributeValue))
                .withMessageContaining(
                        "transformTo() is unsupported because the DynamoDB sink does not need it.");
    }

    @Test
    void testGetEnhancedType() {
        ArrayAttributeConverter<String> arrayAttributeConverter =
                new ArrayAttributeConverter<>(
                        AttributeConverterProvider.defaultProvider()
                                .converterFor(EnhancedType.of(String.class)),
                        EnhancedType.of(String[].class));
        assertThat(arrayAttributeConverter.type()).isEqualTo(EnhancedType.of(String[].class));
    }
}
