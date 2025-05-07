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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.GlueCatalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating GlueCatalog instances.
 */
public class GlueCatalogFactory implements CatalogFactory {

    // Define configuration options that users must provide
    public static final ConfigOption<String> REGION =
            ConfigOptions.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region for the Glue catalog");

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Default database to use in Glue catalog");

    @Override
    public String factoryIdentifier() {
        return "glue";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REGION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        Map<String, String> config = context.getOptions();
        String name = context.getName();
        String region = config.get(REGION.key());
        String defaultDatabase = config.getOrDefault(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());

        // Ensure required properties are present
        if (region == null || region.isEmpty()) {
            throw new CatalogException("The 'region' property must be specified for the Glue catalog.");
        }

        return new GlueCatalog(name, defaultDatabase, region);
    }
}
