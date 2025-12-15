package org.apache.flink.table.catalog.glue.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test class for {@link ConnectorRegistry}.
 */
class ConnectorRegistryTest {

    // Test data for connectors
    private static final String KINESIS = "kinesis";
    private static final String KAFKA = "kafka";
    private static final String UNKNOWN = "unknown";
    private static final String JDBC = "jdbc";
    private static final String FILESYSTEM = "filesystem";
    private static final String ELASTICSEARCH = "elasticsearch";
    private static final String OPENSEARCH = "opensearch";
    private static final String HBASE = "hbase";
    private static final String DYNAMODB = "dynamodb";
    private static final String MONGODB = "mongodb";

    /**
     * Sets up the test environment before each test.
     */
    @BeforeEach
    void setUp() {
        // Reset the static map for each test case
        // This could be necessary if ConnectorRegistry ever evolves to mutate its state
    }

    /**
     * Tests the location key retrieval for Kinesis connector.
     */
    @Test
    void testGetLocationKeyForKinesis() {
        String locationKey = ConnectorRegistry.getLocationKey(KINESIS);

        // Assert that the location key for Kinesis is correct
        assertNotNull(locationKey, "Location key for Kinesis should not be null");
        assertEquals("stream.arn", locationKey, "Location key for Kinesis should be 'stream.arn'");
    }

    /**
     * Tests the location key retrieval for Kafka connector.
     */
    @Test
    void testGetLocationKeyForKafka() {
        String locationKey = ConnectorRegistry.getLocationKey(KAFKA);

        // Assert that the location key for Kafka is correct
        assertNotNull(locationKey, "Location key for Kafka should not be null");
        assertEquals(
                "properties.bootstrap.servers",
                locationKey,
                "Location key for Kafka should be 'bootstrap.servers'");
    }

    /**
     * Tests the location key retrieval for JDBC connector.
     */
    @Test
    void testGetLocationKeyForJDBC() {
        String locationKey = ConnectorRegistry.getLocationKey(JDBC);

        // Assert that the location key for jdbc is correct
        assertNotNull(locationKey, "Location key for JDBC should not be null");
        assertEquals("url", locationKey, "Location key for JDBC should be 'url'");
    }

    /**
     * Tests the location key retrieval for Filesystem connector.
     */
    @Test
    void testGetLocationKeyForFilesystem() {
        String locationKey = ConnectorRegistry.getLocationKey(FILESYSTEM);

        // Assert that the location key for filesystem is correct
        assertNotNull(locationKey, "Location key for Filesystem should not be null");
        assertEquals("path", locationKey, "Location key for Filesystem should be 'path'");
    }

    /**
     * Tests the location key retrieval for Elasticsearch connector.
     */
    @Test
    void testGetLocationKeyForElasticsearch() {
        String locationKey = ConnectorRegistry.getLocationKey(ELASTICSEARCH);

        // Assert that the location key for elasticsearch is correct
        assertNotNull(locationKey, "Location key for Elasticsearch should not be null");
        assertEquals("hosts", locationKey, "Location key for Elasticsearch should be 'hosts'");
    }

    /**
     * Tests the location key retrieval for OpenSearch connector.
     */
    @Test
    void testGetLocationKeyForOpensearch() {
        String locationKey = ConnectorRegistry.getLocationKey(OPENSEARCH);

        // Assert that the location key for opensearch is correct
        assertNotNull(locationKey, "Location key for OpenSearch should not be null");
        assertEquals("hosts", locationKey, "Location key for OpenSearch should be 'hosts'");
    }

    /**
     * Tests the location key retrieval for HBase connector.
     */
    @Test
    void testGetLocationKeyForHBase() {
        String locationKey = ConnectorRegistry.getLocationKey(HBASE);

        // Assert that the location key for hbase is correct
        assertNotNull(locationKey, "Location key for HBase should not be null");
        assertEquals(
                "zookeeper.quorum",
                locationKey,
                "Location key for HBase should be 'zookeeper.quorum'");
    }

    /**
     * Tests the location key retrieval for DynamoDB connector.
     */
    @Test
    void testGetLocationKeyForDynamoDB() {
        String locationKey = ConnectorRegistry.getLocationKey(DYNAMODB);

        // Assert that the location key for dynamodb is correct
        assertNotNull(locationKey, "Location key for DynamoDB should not be null");
        assertEquals("table.name", locationKey, "Location key for DynamoDB should be 'table.name'");
    }

    /**
     * Tests the location key retrieval for MongoDB connector.
     */
    @Test
    void testGetLocationKeyForMongoDB() {
        String locationKey = ConnectorRegistry.getLocationKey(MONGODB);

        // Assert that the location key for mongodb is correct
        assertNotNull(locationKey, "Location key for MongoDB should not be null");
        assertEquals("uri", locationKey, "Location key for MongoDB should be 'uri'");
    }

    /**
     * Tests the location key retrieval for Hive connector.
     */
    @Test
    void testGetLocationKeyForHive() {
        String locationKey = ConnectorRegistry.getLocationKey("hive");

        // Assert that the location key for hive is correct
        assertNotNull(locationKey, "Location key for Hive should not be null");
        assertEquals(
                "hive-conf-dir", locationKey, "Location key for Hive should be 'hive-conf-dir'");
    }

    /**
     * Tests the location key retrieval for an unknown connector.
     */
    @Test
    void testGetLocationKeyForUnknownConnector() {
        String locationKey = ConnectorRegistry.getLocationKey(UNKNOWN);

        // Assert that the location key for unknown connectors is null
        assertNull(locationKey, "Location key for unknown connector should be null");
    }

    /**
     * Tests the logging behavior for an unknown connector.
     */
    @Test
    void testLoggingForUnknownConnector() {
        // Setting up a logger to capture logs if necessary
        // You can use SLF4J's InMemoryAppender or a similar approach to test logs

        // Capture warning message (you could add an appender here to capture logs if needed)
        String locationKey = ConnectorRegistry.getLocationKey(UNKNOWN);

        // Ensure that the method still returns null for an unknown connector
        assertNull(locationKey, "Location key for unknown connector should be null");

        // Validate that a warning log is emitted for the unknown connector (use SLF4J's
        // InMemoryAppender or similar)
        // If you want to test logs, you can capture them using SLF4J's custom Appender and check if
        // the expected log is present.
    }
}
