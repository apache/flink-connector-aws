package org.apache.flink.table.catalog.glue.operations;

import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Test implementation of AbstractGlueOperations.
 * This class is used for testing the base functionality provided by AbstractGlueOperations.
 */
public class TestGlueOperations extends AbstractGlueOperations {

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
