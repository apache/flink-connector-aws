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

package org.apache.flink.table.catalog.glue.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.glue.GlueCatalogOptions;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.CREDENTIAL_PROVIDER;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_ACCOUNT_ID;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_CATALOG_ENDPOINT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_CATALOG_ID;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.HTTP_CLIENT_TYPE;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.INPUT_FORMAT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.REGION;

class GlueCatalogFactoryTest extends TestLogger {

    public static GlueCatalogFactory factory;

    @BeforeAll
    public static void setup() {
        factory = new GlueCatalogFactory();
    }

    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals(GlueCatalogOptions.IDENTIFIER, factory.factoryIdentifier());
    }

    @Test
    public void testOptionalOptions() {
        Set<ConfigOption<?>> configs = factory.optionalOptions();
        Assertions.assertNotNull(configs);
        Assertions.assertEquals(9, configs.size());
        Assertions.assertTrue(configs.contains(INPUT_FORMAT));
        Assertions.assertTrue(configs.contains(OUTPUT_FORMAT));
        Assertions.assertTrue(configs.contains(GLUE_CATALOG_ENDPOINT));
        Assertions.assertTrue(configs.contains(GLUE_ACCOUNT_ID));
        Assertions.assertTrue(configs.contains(GLUE_CATALOG_ID));
        Assertions.assertTrue(configs.contains(DEFAULT_DATABASE));
        Assertions.assertTrue(configs.contains(HTTP_CLIENT_TYPE));
        Assertions.assertTrue(configs.contains(REGION));
        Assertions.assertTrue(configs.contains(CREDENTIAL_PROVIDER));
    }

    @Test
    public void testGetRequiredOptions() {
        Set<ConfigOption<?>> configs = factory.requiredOptions();
        Assertions.assertNotNull(configs);
        Assertions.assertEquals(new HashSet<>(), configs);
    }

    @Test
    public void testCreateCatalog() {
        Map<String, String> options = new HashMap<>();
        ReadableConfig configs = new Configuration();
        CatalogFactory.Context context =
                new FactoryUtil.DefaultCatalogContext(
                        "TestContext", options, configs, ClassLoader.getSystemClassLoader());
        Assertions.assertThrows(NullPointerException.class, () -> factory.createCatalog(context));
        options.put(AWS_REGION, "us-east-1");
        Assertions.assertDoesNotThrow(() -> factory.createCatalog(context));
    }
}
