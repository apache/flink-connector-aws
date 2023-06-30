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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;

/** A {@link KinesisDynamicShardAssignerFactory} that loads the default shard assigner. */
@Internal
public class DefaultShardAssignerFactory implements KinesisDynamicShardAssignerFactory {

    private static final KinesisShardAssigner DEFAULT_SHARD_ASSIGNER =
            (shard, subtasks) -> shard.hashCode();

    public static final String IDENTIFIER = "default";

    @Override
    public String shardAssignerIdentifer() {
        return IDENTIFIER;
    }

    @Override
    public KinesisShardAssigner getShardAssigner() {
        return DEFAULT_SHARD_ASSIGNER;
    }
}
