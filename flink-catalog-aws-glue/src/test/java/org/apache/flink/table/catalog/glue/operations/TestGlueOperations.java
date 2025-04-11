package org.apache.flink.table.catalog.glue.operations;

import software.amazon.awssdk.services.glue.GlueClient;

public class TestGlueOperations extends AbstractGlueOperations {

    public TestGlueOperations(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    public String getCatalogNameForTest() {
        return this.catalogName;
    }
}
