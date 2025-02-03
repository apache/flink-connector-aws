package org.apache.flink.table.catalog.glue.operations;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Tests for the AbstractGlueOperations class.
 * This tests the initialization of fields in the abstract class.
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
        Assertions.assertEquals("testCatalog", testOps.getCatalogNameForTest(), "Catalog name should match");
    }
}
