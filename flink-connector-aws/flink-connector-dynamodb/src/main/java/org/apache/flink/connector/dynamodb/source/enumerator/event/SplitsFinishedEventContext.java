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

package org.apache.flink.connector.dynamodb.source.enumerator.event;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Context which contains the split id and the finished splits for a finished split event. */
@Internal
public class SplitsFinishedEventContext implements Serializable {
    private static final long serialVersionUID = 2L;
    private final String splitId;
    private final List<Shard> childSplits;

    public SplitsFinishedEventContext(String splitId, List<Shard> childSplits) {
        this.splitId = splitId;
        this.childSplits = childSplits;
    }

    public String getSplitId() {
        return splitId;
    }

    public List<Shard> getChildSplits() {
        return childSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SplitsFinishedEventContext that = (SplitsFinishedEventContext) o;

        if (!splitId.equals(that.splitId)) {
            return false;
        }
        return childSplits.equals(that.childSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, childSplits);
    }
}
