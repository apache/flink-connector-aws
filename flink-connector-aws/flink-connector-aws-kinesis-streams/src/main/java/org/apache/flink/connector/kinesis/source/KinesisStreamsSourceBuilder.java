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

package org.apache.flink.connector.kinesis.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.connector.kinesis.source.enumerator.assigner.ShardAssignerFactory;
import org.apache.flink.connector.kinesis.source.enumerator.assigner.UniformShardAssigner;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;

/**
 * Builder to construct the {@link KinesisStreamsSource}.
 *
 * <p>The following example shows the minimum setup to create a {@link KinesisStreamsSource} that
 * reads String values from a Kinesis Data Streams stream with ARN of
 * arn:aws:kinesis:us-east-1:012345678901:stream/your_stream_name.
 *
 * <pre>{@code
 * KinesisStreamsSource<String> kdsSource =
 *                 KinesisStreamsSource.<String>builder()
 *                         .setStreamArn("arn:aws:kinesis:us-east-1:012345678901:stream/your_stream_name")
 *                         .setDeserializationSchema(new SimpleStringSchema())
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code kinesisShardAssigner} will be {@link UniformShardAssigner}
 * </ul>
 *
 * @param <T> type of elements that should be read from the source stream
 */
@Experimental
public class KinesisStreamsSourceBuilder<T> {
    private String streamArn;
    private Configuration sourceConfig;
    private KinesisDeserializationSchema<T> deserializationSchema;
    private KinesisShardAssigner kinesisShardAssigner = ShardAssignerFactory.uniformShardAssigner();
    private boolean preserveShardOrder = true;

    public KinesisStreamsSourceBuilder<T> setStreamArn(String streamArn) {
        this.streamArn = streamArn;
        return this;
    }

    public KinesisStreamsSourceBuilder<T> setSourceConfig(Configuration sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public KinesisStreamsSourceBuilder<T> setDeserializationSchema(
            KinesisDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    public KinesisStreamsSourceBuilder<T> setDeserializationSchema(
            DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = KinesisDeserializationSchema.of(deserializationSchema);
        return this;
    }

    public KinesisStreamsSourceBuilder<T> setKinesisShardAssigner(
            KinesisShardAssigner kinesisShardAssigner) {
        this.kinesisShardAssigner = kinesisShardAssigner;
        return this;
    }

    public KinesisStreamsSourceBuilder<T> setPreserveShardOrder(boolean preserveShardOrder) {
        this.preserveShardOrder = preserveShardOrder;
        return this;
    }

    public KinesisStreamsSource<T> build() {
        return new KinesisStreamsSource<>(
                streamArn,
                sourceConfig,
                deserializationSchema,
                kinesisShardAssigner,
                preserveShardOrder);
    }
}
