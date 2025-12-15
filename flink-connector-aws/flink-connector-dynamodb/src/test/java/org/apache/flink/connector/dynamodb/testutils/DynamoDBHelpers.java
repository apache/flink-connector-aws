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

package org.apache.flink.connector.dynamodb.testutils;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** Helper methods to call dynamoDB service. */
public class DynamoDBHelpers {
    private final DynamoDbAsyncClient client;

    public DynamoDBHelpers(DynamoDbAsyncClient hostClient) {
        this.client = hostClient;
    }

    public void createTable(String tableName, String partitionKey, String sortKey)
            throws ExecutionException, InterruptedException {
        this.createTable(tableName, partitionKey, sortKey, false);
    }

    public void createTable(
            String tableName, String partitionKey, String sortKey, boolean streamEnabled)
            throws ExecutionException, InterruptedException {
        client.createTable(
                CreateTableRequest.builder()
                        .tableName(tableName)
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName(partitionKey)
                                        .attributeType(ScalarAttributeType.S)
                                        .build(),
                                AttributeDefinition.builder()
                                        .attributeName(sortKey)
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName(partitionKey)
                                        .keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder()
                                        .attributeName(sortKey)
                                        .keyType(KeyType.RANGE)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(1000L)
                                        .writeCapacityUnits(1000L)
                                        .build())
                        .streamSpecification(
                                StreamSpecification.builder()
                                        .streamEnabled(streamEnabled)
                                        .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                                        .build())
                        .build());

        DynamoDbAsyncWaiter waiter = client.waiter();
        waiter.waitUntilTableExists(r -> r.tableName(tableName)).get();
    }

    public String getTableStreamArn(String tableName)
            throws ExecutionException, InterruptedException {
        DescribeTableResponse describeTableResponse =
                client.describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                        .get();
        return describeTableResponse.table().latestStreamArn();
    }

    public void putItem(String tableName, Map<String, AttributeValue> item)
            throws ExecutionException, InterruptedException {
        client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build()).get();
    }

    public void deleteItem(String tableName, Map<String, AttributeValue> key)
            throws ExecutionException, InterruptedException {
        client.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key).build()).get();
    }

    public int getItemsCount(String tableName) throws ExecutionException, InterruptedException {
        return client.scan(ScanRequest.builder().tableName(tableName).build()).get().count();
    }

    public boolean containsAttributeValue(
            String tableName, String attributeName, String attributeValue)
            throws ExecutionException, InterruptedException {
        ScanRequest containsRequest =
                ScanRequest.builder()
                        .expressionAttributeNames(Collections.singletonMap("#A", attributeName))
                        .expressionAttributeValues(
                                Collections.singletonMap(
                                        ":val", AttributeValue.builder().s(attributeValue).build()))
                        .filterExpression("#A = :val")
                        .tableName(tableName)
                        .build();
        return client.scan(containsRequest).get().count() > 0;
    }

    public void deleteTable(String tableName) {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    }
}
