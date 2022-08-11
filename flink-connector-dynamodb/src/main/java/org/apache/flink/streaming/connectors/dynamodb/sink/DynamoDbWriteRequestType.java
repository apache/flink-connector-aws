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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Represents a DynamoDb Write Request type. The following types are currently supported
 *
 * <ul>
 *   <li>PUT - Put Request
 *   <li>DELETE - Delete Request
 * </ul>
 */
@PublicEvolving
public enum DynamoDbWriteRequestType {

    // Note: Enums have no stable hash code across different JVMs, use toByteValue() for
    // this purpose.
    PUT((byte) 0),
    DELETE((byte) 1);
    private final byte value;

    DynamoDbWriteRequestType(byte value) {
        this.value = value;
    }

    /**
     * Returns the byte value representation of this {@link DynamoDbWriteRequestType}. The byte
     * value is used for serialization and deserialization.
     *
     * <ul>
     *   <li>"0" represents {@link #PUT}.
     *   <li>"1" represents {@link #DELETE}.
     * </ul>
     */
    public byte toByteValue() {
        return value;
    }

    /**
     * Creates a {@link DynamoDbWriteRequestType} from the given byte value. Each {@link
     * DynamoDbWriteRequestType} has a byte value representation.
     *
     * @see #toByteValue() for mapping of byte value and {@link DynamoDbWriteRequestType}.
     */
    public static DynamoDbWriteRequestType fromByteValue(byte value) {
        switch (value) {
            case 0:
                return PUT;
            case 1:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for DynamoDb request type.");
        }
    }
}
