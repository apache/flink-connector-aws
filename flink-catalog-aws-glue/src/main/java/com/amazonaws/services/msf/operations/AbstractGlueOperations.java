package com.amazonaws.services.msf.operations;

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
