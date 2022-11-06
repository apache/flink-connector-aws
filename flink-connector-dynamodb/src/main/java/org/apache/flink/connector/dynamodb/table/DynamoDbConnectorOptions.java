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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableDescriptor;

/** DynamoDb connector options. Made public for {@link TableDescriptor} to access it. */
@PublicEvolving
public class DynamoDbConnectorOptions {

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of destination DynamoDB table.");

    public static final ConfigOption<String> AWS_REGION =
            ConfigOptions.key("aws.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region for the destination DynamoDB table.");

    public static final ConfigOption<Boolean> FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Determines whether an exception should fail the job, otherwise failed requests are retried.");

    private DynamoDbConnectorOptions() {
        // private constructor to prevent initialization of static class
    }
}
