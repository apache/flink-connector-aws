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

package org.apache.flink.connector.dynamodb.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.sink.InvalidConfigurationException;
import org.apache.flink.connector.dynamodb.sink.InvalidRequestException;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.List;
import java.util.Map;

/** Helper to construct primary(composite) key for a DynamoDB request. */
@Internal
public class PrimaryKeyBuilder {

    private static final String DELIMITER = "$";

    private final List<String> partitionKeys;

    public PrimaryKeyBuilder(List<String> partitionKeys) {
        if (CollectionUtil.isNullOrEmpty(partitionKeys)) {
            throw new InvalidConfigurationException(
                    "Unable to construct partition key as overwriteByPartitionKeys configuration not provided.");
        }

        this.partitionKeys = partitionKeys;
    }

    public String build(WriteRequest request) {

        Map<String, AttributeValue> requestItems = getRequestItems(request);

        StringBuilder builder = new StringBuilder();
        for (String keyName : partitionKeys) {
            AttributeValue keyAttribute = requestItems.get(keyName);

            if (keyAttribute == null) {
                throw new InvalidRequestException(
                        "Request " + request + " does not contain partition key " + keyName + ".");
            }

            String keyValue = getKeyValue(keyAttribute);

            if (StringUtils.isBlank(keyValue)) {
                throw new InvalidRequestException(
                        "Partition key or sort key attributes require non-empty values. Request "
                                + request
                                + " contains empty key "
                                + keyName
                                + ".");
            }

            builder.append(keyValue).append(DELIMITER);
        }

        return builder.toString();
    }

    /**
     * Returns string value of a partition key attribute. Each primary key attribute must be defined
     * as type String, Number, or binary as per DynamoDB specification.
     */
    private static String getKeyValue(AttributeValue value) {
        StringBuilder builder = new StringBuilder();

        if (value.n() != null) {
            builder.append(value.n());
        }

        if (value.s() != null) {
            builder.append(value.s());
        }

        if (value.b() != null) {
            builder.append(value.b().asUtf8String());
        }

        return builder.toString();
    }

    private static Map<String, AttributeValue> getRequestItems(WriteRequest request) {
        if (request.putRequest() != null) {
            if (request.putRequest().hasItem()) {
                return request.putRequest().item();
            } else {
                throw new InvalidRequestException(
                        "PutItemRequest " + request + " does not contain request items.");
            }
        } else if (request.deleteRequest() != null) {
            if (request.deleteRequest().hasKey()) {
                return request.deleteRequest().key();
            } else {
                throw new InvalidRequestException(
                        "DeleteItemRequest " + request + " does not contain request key.");
            }
        } else {
            throw new InvalidRequestException("Empty write request " + request);
        }
    }
}
