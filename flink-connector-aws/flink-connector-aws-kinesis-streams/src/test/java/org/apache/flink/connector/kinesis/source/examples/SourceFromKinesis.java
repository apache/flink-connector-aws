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

package org.apache.flink.connector.kinesis.source.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.enumerator.assigner.ShardAssignerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * An example application demonstrating how to use the {@link KinesisStreamsSource} to read from
 * KDS.
 */
public class SourceFromKinesis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);
        env.setParallelism(2);

        Configuration sourceConfig = new Configuration();
        KinesisStreamsSource<String> kdsSource =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn("arn:aws:kinesis:us-east-1:123456789012:stream/test-stream")
                        .setSourceConfig(sourceConfig)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .setKinesisShardAssigner(ShardAssignerFactory.uniformShardAssigner())
                        .build();
        env.fromSource(kdsSource, WatermarkStrategy.noWatermarks(), "Kinesis source")
                .returns(TypeInformation.of(String.class))
                .print();

        env.execute("KinesisSource Example Program");
    }
}
