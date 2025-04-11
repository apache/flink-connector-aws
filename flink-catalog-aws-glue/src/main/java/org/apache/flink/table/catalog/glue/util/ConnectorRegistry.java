package org.apache.flink.table.catalog.glue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for storing and retrieving location-specific keys for different connectors.
 * It maps connector types to their corresponding location keys (e.g., Kinesis, Kafka).
 */
public class ConnectorRegistry {

    /** Logger for logging connector registry actions. */
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorRegistry.class);

    /** Map to store connector types and their corresponding location-specific keys. */
    private static final Map<String, String> connectorLocationKeys = new HashMap<>();

    // Static block to initialize the connector keys mapping.
    static {
        connectorLocationKeys.put("kinesis", "stream.arn");
        connectorLocationKeys.put("kafka", "properties.bootstrap.servers");
        connectorLocationKeys.put("jdbc", "url");
        connectorLocationKeys.put("filesystem", "path");
        connectorLocationKeys.put("elasticsearch", "hosts");
        connectorLocationKeys.put("opensearch", "hosts");
        connectorLocationKeys.put("hbase", "zookeeper.quorum");
        connectorLocationKeys.put("dynamodb", "table.name");
        connectorLocationKeys.put("mongodb", "uri");
        connectorLocationKeys.put("hive", "hive-conf-dir");
        // Additional connectors can be added here as needed.
    }

    /**
     * Retrieves the location-specific key for a given connector type.
     *
     * @param connectorType The type of the connector (e.g., "kinesis", "kafka").
     * @return The location-specific key corresponding to the connector type, or null if not found.
     */
    public static String getLocationKey(String connectorType) {
        // Log the lookup request.
        LOG.debug("Looking up location key for connector type: {}", connectorType);

        // Check if the connector type exists and return the corresponding key.
        String locationKey = connectorLocationKeys.get(connectorType);
        if (locationKey == null) {
            LOG.warn("No location key found for connector type: {}", connectorType);
        }
        return locationKey;
    }
}
