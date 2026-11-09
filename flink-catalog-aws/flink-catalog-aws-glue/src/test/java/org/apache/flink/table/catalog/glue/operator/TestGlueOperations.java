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

package org.apache.flink.table.catalog.glue.operator;

import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Test implementation of AbstractGlueOperations. This class is used for testing the base
 * functionality provided by AbstractGlueOperations.
 */
public class TestGlueOperations extends GlueOperator {

    /**
     * Constructor for TestGlueOperations.
     *
     * @param glueClient The AWS Glue client to use for operations.
     * @param catalogName The name of the Glue catalog.
     */
    public TestGlueOperations(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Gets the catalog name for testing purposes.
     *
     * @return The catalog name configured in this operations object.
     */
    public String getCatalogNameForTest() {
        return this.catalogName;
    }
}
