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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.CREDENTIAL_PROVIDER;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_ACCOUNT_ID;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_CATALOG_ENDPOINT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.GLUE_CATALOG_ID;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.HTTP_CLIENT_TYPE;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.INPUT_FORMAT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.glue.GlueCatalogOptions.REGION;

class GlueCatalogOptionsTest {

    @Test
    public void testGetAllConfigOptions() {
        Set<ConfigOption<?>> allConfigOptions = GlueCatalogOptions.getAllConfigOptions();
        Assertions.assertEquals(9, allConfigOptions.size());
        Assertions.assertTrue(allConfigOptions.contains(INPUT_FORMAT));
        Assertions.assertTrue(allConfigOptions.contains(OUTPUT_FORMAT));
        Assertions.assertTrue(allConfigOptions.contains(GLUE_CATALOG_ENDPOINT));
        Assertions.assertTrue(allConfigOptions.contains(GLUE_ACCOUNT_ID));
        Assertions.assertTrue(allConfigOptions.contains(GLUE_CATALOG_ID));
        Assertions.assertTrue(allConfigOptions.contains(DEFAULT_DATABASE));
        Assertions.assertTrue(allConfigOptions.contains(HTTP_CLIENT_TYPE));
        Assertions.assertTrue(allConfigOptions.contains(REGION));
        Assertions.assertTrue(allConfigOptions.contains(CREDENTIAL_PROVIDER));
    }

    @Test
    public void testGetRequiredConfigOptions() {
        Set<ConfigOption<?>> requiredOptions = GlueCatalogOptions.getRequiredConfigOptions();
        Assertions.assertEquals(0, requiredOptions.size());
    }
}
