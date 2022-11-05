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
import java.util.Objects;

/**
 * Represents a single Write Request to DynamoDb. Contains the item to be written as well as the
 * type of the Write Request (PUT/DELETE)
 */
@PublicEvolving
public class DynamoDbWriteRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, AttributeValue> item;
    private final DynamoDbWriteRequestType type;

    private DynamoDbWriteRequest(Map<String, AttributeValue> item, DynamoDbWriteRequestType type) {
        this.item = item;
        this.type = type;
    }

    public Map<String, AttributeValue> getItem() {
        return item;
    }

    public DynamoDbWriteRequestType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbWriteRequest that = (DynamoDbWriteRequest) o;
        return item.equals(that.item) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(item, type);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for DynamoDbWriteRequest. */
    public static class Builder {
        private Map<String, AttributeValue> item;
        private DynamoDbWriteRequestType type;

        public Builder setItem(Map<String, AttributeValue> item) {
            this.item = item;
            return this;
        }

        public Builder setType(DynamoDbWriteRequestType type) {
            this.type = type;
            return this;
        }

        public DynamoDbWriteRequest build() {
            Preconditions.checkNotNull(
                    item, "No Item was supplied to the " + "DynamoDbWriteRequest builder.");
            Preconditions.checkNotNull(
                    type, "No type was supplied to the " + "DynamoDbWriteRequest builder.");
            return new DynamoDbWriteRequest(item, type);
        }
    }
}
