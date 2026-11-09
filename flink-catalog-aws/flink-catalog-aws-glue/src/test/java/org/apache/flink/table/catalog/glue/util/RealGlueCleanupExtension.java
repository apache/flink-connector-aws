/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.util;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Keeps a real AWS Glue account clean when the catalog test suites run against the real service
 * (see {@link GlueTestClientFactory}): after each test it deletes every database (and its tables)
 * whose name the test obtained from {@link GlueTestClientFactory#uniqueName(String)}. The
 * fake-backed default mode is a no-op.
 *
 * <p>Deleting only the names this JVM handed out matters because surefire runs the suites in
 * several forks against the same account: a "delete everything that appeared during the test"
 * approach would remove another fork's live database mid-test. Databases a test already dropped
 * itself are skipped. The shared cleanup client is closed once the test class finishes.
 */
public class RealGlueCleanupExtension
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private GlueClient cleanupClient;

    @Override
    public void beforeEach(ExtensionContext context) {
        if (!GlueTestClientFactory.REAL_GLUE) {
            return;
        }
        if (cleanupClient == null) {
            cleanupClient = GlueTestClientFactory.createClient();
        }
        // Names registered before this test belong to nobody now.
        GlueTestClientFactory.drainCreatedNames();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (!GlueTestClientFactory.REAL_GLUE || cleanupClient == null) {
            return;
        }
        for (String database : GlueTestClientFactory.drainCreatedNames()) {
            try {
                for (GetTablesResponse page :
                        cleanupClient.getTablesPaginator(b -> b.databaseName(database))) {
                    for (Table table : page.tableList()) {
                        cleanupClient.deleteTable(b -> b.databaseName(database).name(table.name()));
                    }
                }
                cleanupClient.deleteDatabase(b -> b.name(database));
            } catch (EntityNotFoundException ignored) {
                // never created as a database, or already removed by the test itself
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (cleanupClient != null) {
            cleanupClient.close();
            cleanupClient = null;
        }
    }
}
