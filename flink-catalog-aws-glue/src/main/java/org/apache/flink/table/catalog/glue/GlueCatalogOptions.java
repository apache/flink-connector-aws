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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

import java.util.HashMap;
import java.util.Map;

/** A collection of {@link ConfigOption} which is used in GlueCatalog. */
public class GlueCatalogOptions extends CommonCatalogOptions {
    /** {@link ConfigOption} This is used for getting aws-related properties. */
    public static final ConfigOption<Map<String, String>> CATALOG_PROPERTIES_KEY =
            ConfigOptions.key("aws-properties").mapType().defaultValue(new HashMap<>());
}
