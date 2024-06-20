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

package org.apache.flink.connector.dynamodb.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.aws.config.AWSConfigConstants;

/**
 * Optional consumer specific configuration keys and default values for {@link
 * org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSource}.
 */
@PublicEvolving
public class ConsumerConfigConstants extends AWSConfigConstants {

    public static final int DEFAULT_STREAM_DESCRIBE_RETRIES = 50;

    public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 2000L;

    public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final long DEFAULT_LIST_SHARDS_BACKOFF_BASE = 1000L;

    public static final long DEFAULT_LIST_SHARDS_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_LIST_SHARDS_RETRIES = 10;

    public static final int DEFAULT_DESCRIBE_STREAM_CONSUMER_RETRIES = 50;

    public static final long DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE = 2000L;

    public static final long DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;
}
