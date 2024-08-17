/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.sqs.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** Options for the SQS connector. */
@PublicEvolving
public class SqsConnectorOptions {
    public static final ConfigOption<String> QUEUE_URL =
            ConfigOptions.key("queue-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL of the SQS queue.");

    public static final ConfigOption<String> AWS_REGION =
            ConfigOptions.key("aws.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region of used SQS queue.");

    public static final ConfigOption<Map<String, String>> AWS_CONFIG_PROPERTIES =
            ConfigOptions.key("aws")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("AWS configuration properties.");

    public static final ConfigOption<Boolean> FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Flag to trigger global failure on error.");
}
