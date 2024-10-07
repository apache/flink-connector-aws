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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Objects;

/** Internal type for enumerating available metadata. */
@Internal
public enum RowMetadata {
    Timestamp("timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()),
    SequenceNumber("sequence-number", DataTypes.VARCHAR(128).notNull()),
    ShardId("shard-id", DataTypes.VARCHAR(128).notNull());

    private final String fieldName;
    private final DataType dataType;

    RowMetadata(String fieldName, DataType dataType) {
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public static RowMetadata of(String fieldName) {
        return Arrays.stream(RowMetadata.values())
                .filter(m -> Objects.equals(m.fieldName, fieldName))
                .findFirst()
                .orElseThrow(
                        () -> {
                            String msg =
                                    "Cannot find RowMetadata instance for field name '"
                                            + fieldName
                                            + "'";
                            return new IllegalArgumentException(msg);
                        });
    }
}
