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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link ConnectorRegistry}. */
class ConnectorRegistryTest {

    private final GlueTableUtils glueTableUtils = new GlueTableUtils(new GlueTypeConverter());

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        "jdbc, url, jdbc:postgresql://db.example.com:5432/shop",
        "filesystem, path, s3://bucket/warehouse/orders",
        "elasticsearch, hosts, http://es.example.com:9200",
        "opensearch, hosts, https://os.example.com:9200",
        "mongodb, uri, mongodb://mongo.example.com:27017"
    })
    void testRegisteredConnectorsHaveUriLocationOptions(
            String connector, String expectedKey, String sampleValue) {
        assertThat(ConnectorRegistry.getLocationKey(connector)).isEqualTo(expectedKey);

        // Every registered key must actually produce a Glue location: a registered option whose
        // values never contain "://" would be dead (see GlueTableUtils#extractTableLocation).
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", connector);
        tableProperties.put(expectedKey, sampleValue);
        assertThat(glueTableUtils.extractTableLocation(tableProperties)).isEqualTo(sampleValue);
    }

    /**
     * Connectors addressed by a non-URI value are deliberately not registered: a Kinesis stream
     * ARN, Kafka bootstrap servers or a DynamoDB table name are kept as table options only and
     * never become the Glue storage location.
     */
    @ParameterizedTest
    @ValueSource(strings = {"kinesis", "kafka", "dynamodb", "hbase", "hive", "unknown"})
    void testConnectorsWithoutUriLocationAreNotRegistered(String connector) {
        assertThat(ConnectorRegistry.getLocationKey(connector)).isNull();
    }

    @Test
    void testUnregisteredConnectorYieldsNoTableLocation() {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("connector", "kinesis");
        tableProperties.put("stream.arn", "arn:aws:kinesis:us-east-1:123456789012:stream/orders");

        assertThat(glueTableUtils.extractTableLocation(tableProperties)).isNull();
    }
}
