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

package org.apache.flink.connector.dynamodb.sink;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDbWriteRequestTest {

    @Test
    public void testAttributeValueExpectedFields() {
        List<Field> fields =
                Arrays.stream(AttributeValue.class.getDeclaredFields())
                        .filter(field -> !Modifier.isStatic(field.getModifiers()))
                        .collect(Collectors.toList());

        assertThat(fields)
                .as(
                        "If this test fails the DynamoDB AWS SDK may have changed. "
                                + "We need to check this, and update the DynamoDbWriterStateSerializer if required.")
                .hasSize(11);
    }

    @Test
    public void testToString() {
        DynamoDbWriteRequest dynamoDbWriteRequest =
                DynamoDbWriteRequest.builder()
                        .setItem(
                                ImmutableMap.of(
                                        "testKey", AttributeValue.builder().s("testValue").build()))
                        .setType(DynamoDbWriteRequestType.PUT)
                        .build();
        assertThat(dynamoDbWriteRequest.toString())
                .contains("testKey")
                .contains("testValue")
                .contains(DynamoDbWriteRequestType.PUT.toString());
    }
}
