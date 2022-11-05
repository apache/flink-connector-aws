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

/**
 * enum representing the dynamodb types.
 *
 * <ul>
 *   <li>String
 *   <li>Number
 *   <li>Boolean
 *   <li>Null
 *   <li>Binary
 *   <li>String Set
 *   <li>Number Set
 *   <li>Binary Set
 *   <li>List
 *   <li>Map
 * </ul>
 */
@Internal
public enum DynamoDbType {

    // Note: Enums have no stable hash code across different JVMs, use toByteValue() for
    // this purpose.
    STRING((byte) 0),
    NUMBER((byte) 1),
    BOOLEAN((byte) 2),
    NULL((byte) 3),
    BINARY((byte) 4),
    STRING_SET((byte) 5),
    NUMBER_SET((byte) 6),
    BINARY_SET((byte) 7),
    LIST((byte) 8),
    MAP((byte) 9);

    private final byte value;

    DynamoDbType(byte value) {
        this.value = value;
    }

    /**
     * Returns the byte value representation of this {@link DynamoDbType}. The byte value is used
     * for serialization and deserialization.
     *
     * <ul>
     *   <li>"0" represents {@link #STRING}.
     *   <li>"1" represents {@link #NUMBER}.
     *   <li>"2" represents {@link #BOOLEAN}.
     *   <li>"3" represents {@link #NULL}.
     *   <li>"4" represents {@link #BINARY}.
     *   <li>"5" represents {@link #STRING_SET}.
     *   <li>"6" represents {@link #NUMBER_SET}.
     *   <li>"7" represents {@link #BINARY_SET}.
     *   <li>"8" represents {@link #LIST}.
     *   <li>"9" represents {@link #MAP}.
     * </ul>
     */
    public byte toByteValue() {
        return value;
    }

    /**
     * Creates a {@link DynamoDbType} from the given byte value. Each {@link DynamoDbType} has a
     * byte value representation.
     *
     * @see #toByteValue() for mapping of byte value and {@link DynamoDbType}.
     */
    public static DynamoDbType fromByteValue(byte value) {
        switch (value) {
            case 0:
                return STRING;
            case 1:
                return NUMBER;
            case 2:
                return BOOLEAN;
            case 3:
                return NULL;
            case 4:
                return BINARY;
            case 5:
                return STRING_SET;
            case 6:
                return NUMBER_SET;
            case 7:
                return BINARY_SET;
            case 8:
                return LIST;
            case 9:
                return MAP;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for DynamoDb type.");
        }
    }
}
