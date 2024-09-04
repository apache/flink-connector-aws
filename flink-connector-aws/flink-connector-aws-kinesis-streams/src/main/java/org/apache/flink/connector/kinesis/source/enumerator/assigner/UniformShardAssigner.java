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

package org.apache.flink.connector.kinesis.source.enumerator.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.util.Preconditions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of the {@link KinesisShardAssigner} that maps Kinesis shard hash-key ranges to
 * Flink subtasks. This shard assigner will ensure that the hash-key range across all shards are
 * split across all subtasks (on a shard-level granularity). This is essential to ensure even
 * distribution of open shards across subtasks when the Kinesis stream is scaled.
 */
@Internal
public class UniformShardAssigner implements KinesisShardAssigner {

    private static final BigInteger TWO = BigInteger.valueOf(2);

    private static final BigInteger HASH_KEY_BOUND = TWO.pow(128);

    @Override
    public int assign(KinesisShardSplit split, Context context) {
        Preconditions.checkArgument(
                !context.getRegisteredReaders().isEmpty(),
                "Expected at least one registered reader. Unable to assign split.");
        BigInteger hashKeyStart = new BigInteger(split.getStartingHashKey());
        BigInteger hashKeyEnd = new BigInteger(split.getEndingHashKey());
        BigInteger hashKeyMid = hashKeyStart.add(hashKeyEnd).divide(TWO);
        // index = hashKeyMid / HASH_KEY_BOUND * nSubtasks + stream-specific offset
        // The stream specific offset is added so that different streams will be less likely to be
        // distributed in the same way, even if they are sharded in the same way.
        // (The caller takes result modulo nSubtasks.)
        List<Integer> registeredReaders = new ArrayList<>(context.getRegisteredReaders().keySet());
        int selectedReaderIndex =
                Math.abs(
                        hashKeyMid
                                .multiply(BigInteger.valueOf(registeredReaders.size()))
                                .divide(HASH_KEY_BOUND)
                                .intValue());
        return registeredReaders.get(selectedReaderIndex);
    }
}
