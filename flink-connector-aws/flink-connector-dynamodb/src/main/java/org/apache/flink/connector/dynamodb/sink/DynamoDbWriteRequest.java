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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single Write Request to DynamoDb. Contains the item to be written as well as the
 * type of the Write Request (PUT/DELETE/UPDATE).
 *
 * <p>For PUT requests, {@code item} contains the full item attributes. For DELETE requests, {@code
 * item} contains the primary key attributes. For UPDATE requests, {@code item} contains the primary
 * key attributes and the update expression fields must be set.
 */
@PublicEvolving
public class DynamoDbWriteRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, AttributeValue> item;
    private final DynamoDbWriteRequestType type;
    private final String updateExpression;
    private final Map<String, String> expressionAttributeNames;
    private final Map<String, AttributeValue> expressionAttributeValues;
    private final String conditionExpression;

    private DynamoDbWriteRequest(
            Map<String, AttributeValue> item,
            DynamoDbWriteRequestType type,
            String updateExpression,
            Map<String, String> expressionAttributeNames,
            Map<String, AttributeValue> expressionAttributeValues,
            String conditionExpression) {
        this.item = item;
        this.type = type;
        this.updateExpression = updateExpression;
        this.expressionAttributeNames = expressionAttributeNames;
        this.expressionAttributeValues = expressionAttributeValues;
        this.conditionExpression = conditionExpression;
    }

    public Map<String, AttributeValue> getItem() {
        return item;
    }

    public DynamoDbWriteRequestType getType() {
        return type;
    }

    public String getUpdateExpression() {
        return updateExpression;
    }

    public Map<String, String> getExpressionAttributeNames() {
        return expressionAttributeNames;
    }

    public Map<String, AttributeValue> getExpressionAttributeValues() {
        return expressionAttributeValues;
    }

    public String getConditionExpression() {
        return conditionExpression;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "DynamoDbWriteRequest{"
                + "item="
                + item
                + ", type="
                + type
                + ", updateExpression='"
                + updateExpression
                + '\''
                + ", expressionAttributeNames="
                + expressionAttributeNames
                + ", expressionAttributeValues="
                + expressionAttributeValues
                + ", conditionExpression='"
                + conditionExpression
                + '\''
                + '}';
    }

    /** Builder for DynamoDbWriteRequest. */
    public static class Builder {
        private Map<String, AttributeValue> item;
        private DynamoDbWriteRequestType type;
        private String updateExpression;
        private Map<String, String> expressionAttributeNames;
        private Map<String, AttributeValue> expressionAttributeValues;
        private String conditionExpression;

        public Builder setItem(Map<String, AttributeValue> item) {
            this.item = item;
            return this;
        }

        public Builder setType(DynamoDbWriteRequestType type) {
            this.type = type;
            return this;
        }

        public Builder setUpdateExpression(String updateExpression) {
            this.updateExpression = updateExpression;
            return this;
        }

        public Builder setExpressionAttributeNames(
                Map<String, String> expressionAttributeNames) {
            this.expressionAttributeNames = expressionAttributeNames;
            return this;
        }

        public Builder setExpressionAttributeValues(
                Map<String, AttributeValue> expressionAttributeValues) {
            this.expressionAttributeValues = expressionAttributeValues;
            return this;
        }

        public Builder setConditionExpression(String conditionExpression) {
            this.conditionExpression = conditionExpression;
            return this;
        }

        public DynamoDbWriteRequest build() {
            Preconditions.checkNotNull(
                    item, "No Item was supplied to the DynamoDbWriteRequest builder.");
            Preconditions.checkNotNull(
                    type, "No type was supplied to the DynamoDbWriteRequest builder.");
            if (type == DynamoDbWriteRequestType.UPDATE) {
                Preconditions.checkNotNull(
                        updateExpression,
                        "No updateExpression was supplied for UPDATE DynamoDbWriteRequest.");
            }
            return new DynamoDbWriteRequest(
                    item,
                    type,
                    updateExpression,
                    expressionAttributeNames,
                    expressionAttributeValues,
                    conditionExpression);
        }
    }
}
