/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.table.util.AWSOptionUtils;
import org.apache.flink.connector.aws.table.util.HttpClientOptionUtils;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;
import org.apache.flink.connector.base.table.options.TableOptionsUtils;
import org.apache.flink.table.catalog.glue.GlueCatalogOptions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Option Handler for Glue Catalog. */
@Internal
public class GlueCatalogOptionsUtils implements TableOptionsUtils, ConfigurationValidator {

    /** Allowed Http Client Types. */
    private static final String[] ALLOWED_GLUE_HTTP_CLIENTS =
            new String[] {
                AWSConfigConstants.CLIENT_TYPE_URLCONNECTION, AWSConfigConstants.CLIENT_TYPE_APACHE
            };

    private final AWSOptionUtils awsOptionUtils;
    private final HttpClientOptionUtils httpClientOptionUtils;
    private final ReadableConfig tableConfig;

    public GlueCatalogOptionsUtils(
            Map<String, String> resolvedOptions, ReadableConfig tableConfig) {
        this.awsOptionUtils = new AWSOptionUtils(resolvedOptions);
        this.httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, resolvedOptions);
        this.tableConfig = tableConfig;
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties validatedConfigs = new Properties();
        validatedConfigs.putAll(awsOptionUtils.getValidatedConfigurations());
        validatedConfigs.putAll(httpClientOptionUtils.getValidatedConfigurations());

        for (ConfigOption<?> option : GlueCatalogOptions.getAllConfigOptions()) {
            if (tableConfig.getOptional(option).isPresent()) {
                validatedConfigs.put(option.key(), tableConfig.getOptional(option).get());
            }
        }
        return validatedConfigs;
    }

    @Override
    public Map<String, String> getProcessedResolvedOptions() {
        Map<String, String> processedOptions = awsOptionUtils.getProcessedResolvedOptions();
        processedOptions.putAll(httpClientOptionUtils.getProcessedResolvedOptions());
        return processedOptions;
    }

    @Override
    public List<String> getNonValidatedPrefixes() {
        // Glue Specific Options are handled by FactoryHelper
        return Arrays.asList(
                AWSOptionUtils.AWS_PROPERTIES_PREFIX, HttpClientOptionUtils.CLIENT_PREFIX);
    }
}
