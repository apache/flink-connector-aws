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

package org.apache.flink.table.catalog.glue.exception;

/**
 * Exception thrown when a data type cannot be mapped between Flink and AWS Glue.
 * This is used specifically for cases where a type conversion between the two systems
 * is not supported or cannot be performed.
 */
public class UnsupportedDataTypeMappingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new UnsupportedDataTypeMappingException with the given message.
     *
     * @param message The detail message
     */
    public UnsupportedDataTypeMappingException(String message) {
        super(message);
    }

    /**
     * Creates a new UnsupportedDataTypeMappingException with the given message and cause.
     *
     * @param message The detail message
     * @param cause The cause of this exception
     */
    public UnsupportedDataTypeMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
