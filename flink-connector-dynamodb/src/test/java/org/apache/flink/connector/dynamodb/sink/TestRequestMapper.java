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

import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.commons.math3.random.RandomDataGenerator;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

/** Mapper function to test DynamoDB sync implementation. */
public class TestRequestMapper extends RichMapFunction<String, Map<String, AttributeValue>> {

    private final String partitionKey;
    private final String sortKey;
    private final RandomDataGenerator random = new RandomDataGenerator();

    public TestRequestMapper(String partitionKey, String sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }

    @Override
    public Map<String, AttributeValue> map(String data) throws Exception {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(partitionKey, AttributeValue.builder().s(this.random.nextHexString(5)).build());
        item.put(sortKey, AttributeValue.builder().s(this.random.nextHexString(5)).build());
        item.put("payload", AttributeValue.builder().s(data).build());
        return item;
    }
}
