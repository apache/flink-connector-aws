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

package org.apache.flink.connector.dynamodb.source.enumerator;

import org.apache.flink.annotation.Internal;

import java.time.Instant;
import java.util.List;

/**
 * State for the {@link DynamoDbStreamsSourceEnumerator}. This class is stored in state, so any
 * changes need to be backwards compatible
 */
@Internal
public class DynamoDbStreamsSourceEnumeratorState {
    private final List<DynamoDBStreamsShardSplitWithAssignmentStatus> knownSplits;
    private final Instant startTimestamp;

    public DynamoDbStreamsSourceEnumeratorState(
            final List<DynamoDBStreamsShardSplitWithAssignmentStatus> knownSplits,
            final Instant startTimestamp) {
        this.knownSplits = knownSplits;
        this.startTimestamp = startTimestamp;
    }

    public List<DynamoDBStreamsShardSplitWithAssignmentStatus> getKnownSplits() {
        return knownSplits;
    }

    public Instant getStartTimestamp() {
        return startTimestamp;
    }
}
