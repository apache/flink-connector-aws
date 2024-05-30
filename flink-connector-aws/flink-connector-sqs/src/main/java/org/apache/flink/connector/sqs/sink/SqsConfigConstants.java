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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Defaults for {@link SqsSinkWriter}. */
@PublicEvolving
public class SqsConfigConstants {

    public static final ConfigOption<String> BASE_SQS_USER_AGENT_PREFIX_FORMAT =
            ConfigOptions.key("Apache Flink %s (%s) SQS Connector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQS useragent prefix format.");

    public static final ConfigOption<String> SQS_CLIENT_USER_AGENT_PREFIX =
            ConfigOptions.key("aws.sqs.client.user-agent-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQS identifier for user agent prefix.");

    public static final ConfigOption<Integer> SQS_CLIENT_RETRY_STRATEGY_MAX_ATTEMPTS =
            ConfigOptions.key("aws.sqs.client.retry-strategy.max-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum number of attempts that the retry strategy will allow.");
}
