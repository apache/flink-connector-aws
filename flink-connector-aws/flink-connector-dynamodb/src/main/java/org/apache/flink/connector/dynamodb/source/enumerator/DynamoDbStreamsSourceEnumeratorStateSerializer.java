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
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Used to serialize and deserialize the {@link DynamoDbStreamsSourceEnumeratorState}. */
@Internal
public class DynamoDbStreamsSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<DynamoDbStreamsSourceEnumeratorState> {
    private static final int CURRENT_VERSION = 0;

    private final DynamoDbStreamsShardSplitSerializer splitSerializer;

    public DynamoDbStreamsSourceEnumeratorStateSerializer(
            DynamoDbStreamsShardSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(
            DynamoDbStreamsSourceEnumeratorState dynamoDbStreamsSourceEnumeratorState)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(dynamoDbStreamsSourceEnumeratorState.getKnownSplits().size());
            out.writeInt(splitSerializer.getVersion());
            for (DynamoDBStreamsShardSplitWithAssignmentStatus split :
                    dynamoDbStreamsSourceEnumeratorState.getKnownSplits()) {
                byte[] serializedSplit = splitSerializer.serialize(split.split());
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
                out.writeInt(split.assignmentStatus().getStatusCode());
            }
            out.writeLong(dynamoDbStreamsSourceEnumeratorState.getStartTimestamp().toEpochMilli());

            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public DynamoDbStreamsSourceEnumeratorState deserialize(
            int version, byte[] serializedEnumeratorState) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedEnumeratorState);
                DataInputStream in = new DataInputStream(bais)) {

            if (version != getVersion()) {
                throw new VersionMismatchException(
                        "Trying to deserialize DynamoDbStreamsSourceEnumeratorState serialized with unsupported version "
                                + version
                                + ". Serializer version is "
                                + getVersion());
            }
            final int numKnownSplits = in.readInt();
            final int splitSerializerVersion = in.readInt();

            List<DynamoDBStreamsShardSplitWithAssignmentStatus> knownSplits =
                    new ArrayList<>(numKnownSplits);

            for (int i = 0; i < numKnownSplits; i++) {
                int serializedLength = in.readInt();
                byte[] serializedSplit = new byte[serializedLength];
                if (in.read(serializedSplit) != -1) {
                    DynamoDbStreamsShardSplit deserializedSplit =
                            splitSerializer.deserialize(splitSerializerVersion, serializedSplit);
                    SplitAssignmentStatus assignmentStatus =
                            SplitAssignmentStatus.fromStatusCode(in.readInt());
                    knownSplits.add(
                            new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                    deserializedSplit, assignmentStatus));
                } else {
                    throw new IOException(
                            "Unexpectedly reading more bytes than is present in stream.");
                }
            }

            Instant startTimestamp = Instant.ofEpochMilli(in.readLong());
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes when deserializing.");
            }

            return new DynamoDbStreamsSourceEnumeratorState(knownSplits, startTimestamp);
        }
    }
}
