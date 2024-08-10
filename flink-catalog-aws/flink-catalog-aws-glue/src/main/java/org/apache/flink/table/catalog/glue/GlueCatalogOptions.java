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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.glue.constants.AWSGlueConfigConstants;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;

import software.amazon.awssdk.regions.Region;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.glue.GlueCatalog.DEFAULT_DB;

/** Collection of {@link ConfigOption} used in GlueCatalog. */
@Internal
public class GlueCatalogOptions extends CommonCatalogOptions {

    public static final String IDENTIFIER = "glue";
    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .defaultValue(DEFAULT_DB);

    public static final ConfigOption<String> INPUT_FORMAT =
            ConfigOptions.key(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> OUTPUT_FORMAT =
            ConfigOptions.key(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> GLUE_CATALOG_ENDPOINT =
            ConfigOptions.key(AWSGlueConfigConstants.GLUE_CATALOG_ENDPOINT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> GLUE_CATALOG_ID =
            ConfigOptions.key(AWSGlueConfigConstants.GLUE_CATALOG_ID).stringType().noDefaultValue();

    public static final ConfigOption<String> GLUE_ACCOUNT_ID =
            ConfigOptions.key(AWSGlueConfigConstants.GLUE_ACCOUNT_ID).stringType().noDefaultValue();

    public static final ConfigOption<String> CREDENTIAL_PROVIDER =
            ConfigOptions.key(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)
                    .stringType()
                    .defaultValue(String.valueOf(AWSConfigConstants.CredentialProvider.AUTO));

    public static final ConfigOption<String> HTTP_CLIENT_TYPE =
            ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_TYPE)
                    .stringType()
                    .defaultValue(AWSConfigConstants.CLIENT_TYPE_APACHE);

    public static final ConfigOption<String> REGION =
            ConfigOptions.key(AWSConfigConstants.AWS_REGION)
                    .stringType()
                    .defaultValue(Region.US_WEST_1.toString());

    public static Set<ConfigOption<?>> getAllConfigOptions() {
        Set<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.add(INPUT_FORMAT);
        configOptions.add(OUTPUT_FORMAT);
        configOptions.add(GLUE_CATALOG_ENDPOINT);
        configOptions.add(GLUE_ACCOUNT_ID);
        configOptions.add(GLUE_CATALOG_ID);
        configOptions.add(DEFAULT_DATABASE);
        configOptions.add(HTTP_CLIENT_TYPE);
        configOptions.add(REGION);
        configOptions.add(CREDENTIAL_PROVIDER);
        return configOptions;
    }

    public static Set<ConfigOption<?>> getRequiredConfigOptions() {
        return new HashSet<>();
    }
}
