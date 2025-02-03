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

package org.apache.flink.table.catalog.glue.operations;

import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Abstract base class for Glue operations that contains common functionality
 * for interacting with the AWS Glue service.
 */
public abstract class AbstractGlueOperations {

    /** The Glue client used for interacting with AWS Glue. */
    protected final GlueClient glueClient;

    /** The catalog name associated with the Glue operations. */
    protected final String catalogName;

    /**
     * Constructor to initialize the shared fields.
     *
     * @param glueClient The Glue client used for interacting with the AWS Glue service.
     * @param catalogName The catalog name associated with the Glue operations.
     */
    protected AbstractGlueOperations(GlueClient glueClient, String catalogName) {
        this.glueClient = glueClient;
        this.catalogName = catalogName;
    }
}
