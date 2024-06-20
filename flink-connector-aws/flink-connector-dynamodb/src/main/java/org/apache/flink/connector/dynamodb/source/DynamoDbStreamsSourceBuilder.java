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

package org.apache.flink.connector.dynamodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsShardAssigner;
import org.apache.flink.connector.dynamodb.source.enumerator.assigner.ShardAssignerFactory;
import org.apache.flink.connector.dynamodb.source.enumerator.assigner.UniformShardAssigner;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;

/**
 * Builder to construct the {@link DynamoDbStreamsSource}.
 *
 * <p>The following example shows the minimum setup to create a {@link DynamoDbStreamsSource} that
 * reads String values from a DynamoDb stream with ARN of
 * arn:aws:dynamodb:us-east-1:012345678901:table/your_table_name/stream/stream-label.
 *
 * <pre>{@code
 * DynamoDbStreamsSource<String> dynamoDbStreamsSource =
 *                 DynamoDbStreamsSource.<String>builder()
 *                         .setStreamArn("arn:aws:dynamodb:us-east-1:012345678901:table/your_table_name/stream/stream-label")
 *                         .setDeserializationSchema(new SimpleStringSchema())
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code dynamoDbStreamsShardAssigner} will be {@link UniformShardAssigner}
 * </ul>
 *
 * @param <T> type of elements that should be read from the source stream
 */
@Experimental
public class DynamoDbStreamsSourceBuilder<T> {
    private String streamArn;
    private Configuration sourceConfig;
    private DynamoDbStreamsDeserializationSchema<T> deserializationSchema;
    private DynamoDbStreamsShardAssigner dynamoDbStreamsShardAssigner = ShardAssignerFactory.uniformShardAssigner();

    public DynamoDbStreamsSourceBuilder<T> setStreamArn(String streamArn) {
        this.streamArn = streamArn;
        return this;
    }

    public DynamoDbStreamsSourceBuilder<T> setSourceConfig(Configuration sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public DynamoDbStreamsSourceBuilder<T> setDeserializationSchema(
            DynamoDbStreamsDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    public DynamoDbStreamsSourceBuilder<T> setDeserializationSchema(
            DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = DynamoDbStreamsDeserializationSchema.of(deserializationSchema);
        return this;
    }

    public DynamoDbStreamsSourceBuilder<T> setDynamoDbStreamsShardAssigner(
            DynamoDbStreamsShardAssigner dynamoDbStreamsShardAssigner) {
        this.dynamoDbStreamsShardAssigner = dynamoDbStreamsShardAssigner;
        return this;
    }

    public DynamoDbStreamsSource<T> build() {
        return new DynamoDbStreamsSource<>(
                streamArn, sourceConfig, deserializationSchema, dynamoDbStreamsShardAssigner);
    }
}
