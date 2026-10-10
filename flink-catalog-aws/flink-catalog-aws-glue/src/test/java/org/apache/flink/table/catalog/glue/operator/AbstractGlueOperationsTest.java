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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Tests for the AbstractGlueOperations class. This tests the initialization of fields in the
 * abstract class.
 */
class AbstractGlueOperationsTest {

    /**
     * Tests that the AbstractGlueOperations properly initializes the GlueClient and catalog name.
     */
    @Test
    void testAbstractGlueOperationsInitialization() {
        GlueClient fakeGlueClient = new FakeGlueClient();
        TestGlueOperations testOps = new TestGlueOperations(fakeGlueClient, "testCatalog");

        Assertions.assertNotNull(testOps.glueClient, "GlueClient should be initialized");
        Assertions.assertEquals(
                "testCatalog", testOps.getCatalogNameForTest(), "Catalog name should match");
    }
}
