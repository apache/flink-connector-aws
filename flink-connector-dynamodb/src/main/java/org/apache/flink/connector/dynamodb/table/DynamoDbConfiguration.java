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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.table.util.AsyncClientOptionsUtils;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.FAIL_ON_ERROR;
import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.TABLE_NAME;

/** DynamoDb specific configuration. */
@Internal
public class DynamoDbConfiguration {

    private final Map<String, String> rawTableOptions;
    private final ReadableConfig tableOptions;
    private final AsyncSinkConfigurationValidator asyncSinkConfigurationValidator;
    private final AsyncClientOptionsUtils asyncClientOptionsUtils;

    public DynamoDbConfiguration(Map<String, String> rawTableOptions, ReadableConfig tableOptions) {
        this.rawTableOptions = rawTableOptions;
        this.tableOptions = tableOptions;
        this.asyncSinkConfigurationValidator = new AsyncSinkConfigurationValidator(tableOptions);
        this.asyncClientOptionsUtils = new AsyncClientOptionsUtils(rawTableOptions);
    }

    public String getTableName() {
        return tableOptions.get(TABLE_NAME);
    }

    public boolean getFailOnError() {
        return tableOptions.get(FAIL_ON_ERROR);
    }

    public Properties getAsyncSinkProperties() {
        return asyncSinkConfigurationValidator.getValidatedConfigurations();
    }

    public Properties getSinkClientProperties() {
        return asyncClientOptionsUtils.getValidatedConfigurations();
    }
}
