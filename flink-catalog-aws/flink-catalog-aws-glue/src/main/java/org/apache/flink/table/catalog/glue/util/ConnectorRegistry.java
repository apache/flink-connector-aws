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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Flink connector identifiers to the table option that names the connector's data location
 * (for example {@code path} for the filesystem connector). Used to populate the Glue {@code
 * StorageDescriptor.location} for connectors whose location is a URI.
 */
@Internal
public final class ConnectorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorRegistry.class);

    /** Connector identifier to the option key holding its location. */
    private static final Map<String, String> CONNECTOR_LOCATION_KEYS = new HashMap<>();

    static {
        CONNECTOR_LOCATION_KEYS.put("kinesis", "stream.arn");
        CONNECTOR_LOCATION_KEYS.put("kafka", "properties.bootstrap.servers");
        CONNECTOR_LOCATION_KEYS.put("jdbc", "url");
        CONNECTOR_LOCATION_KEYS.put("filesystem", "path");
        CONNECTOR_LOCATION_KEYS.put("elasticsearch", "hosts");
        CONNECTOR_LOCATION_KEYS.put("opensearch", "hosts");
        CONNECTOR_LOCATION_KEYS.put("hbase", "zookeeper.quorum");
        CONNECTOR_LOCATION_KEYS.put("dynamodb", "table.name");
        CONNECTOR_LOCATION_KEYS.put("mongodb", "uri");
        CONNECTOR_LOCATION_KEYS.put("hive", "hive-conf-dir");
    }

    private ConnectorRegistry() {}

    /**
     * Retrieves the location option key for a given connector identifier.
     *
     * @param connectorType The connector identifier (e.g., "kinesis", "kafka").
     * @return The location option key, or null if the connector is not registered.
     */
    public static String getLocationKey(String connectorType) {
        String locationKey = CONNECTOR_LOCATION_KEYS.get(connectorType);
        if (locationKey == null) {
            // Not an error: most connectors have no location to record in Glue.
            LOG.debug("No location key registered for connector type: {}", connectorType);
        }
        return locationKey;
    }
}
