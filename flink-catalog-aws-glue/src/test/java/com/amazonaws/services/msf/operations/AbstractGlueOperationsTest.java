package com.amazonaws.services.msf.operations;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import software.amazon.awssdk.services.glue.GlueClient;

class AbstractGlueOperationsTest {

    @Test
    void testAbstractGlueOperationsInitialization() {
        GlueClient fakeGlueClient = new FakeGlueClient();
        TestGlueOperations testOps = new TestGlueOperations(fakeGlueClient, "testCatalog");

        assertNotNull(testOps.glueClient, "GlueClient should be initialized");
        assertEquals("testCatalog", testOps.getCatalogNameForTest(), "Catalog name should match");
    }
}
