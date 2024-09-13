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

package org.apache.flink.connector.dynamodb.source.metrics;

import org.apache.flink.annotation.Internal;

/** A collection of consumer metric related constant names. */
@Internal
public class MetricConstants {
    public static final String DYNAMODB_STREAMS_SOURCE_METRIC_GROUP = "DynamoDbStreamsSource";
    public static final String STREAM_METRIC_GROUP = "stream";
    public static final String SHARD_METRIC_GROUP = "shardId";
    public static final String REGION_METRIC_GROUP = "region";
    public static final String ACCOUNT_ID_METRIC_GROUP = "accountId";
    public static final String MILLIS_BEHIND_LATEST = "millisBehindLatest";
}
