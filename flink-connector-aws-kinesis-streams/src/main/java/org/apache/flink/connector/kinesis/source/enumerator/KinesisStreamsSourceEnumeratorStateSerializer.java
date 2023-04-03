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

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Used to serialize and deserialize the {@link KinesisStreamsSourceEnumeratorState}. */
@Internal
public class KinesisStreamsSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    private final KinesisShardSplitSerializer splitSerializer;

    public KinesisStreamsSourceEnumeratorStateSerializer(
            KinesisShardSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KinesisStreamsSourceEnumeratorState kinesisStreamsSourceEnumeratorState)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            boolean hasLastSeenShardId =
                    kinesisStreamsSourceEnumeratorState.getLastSeenShardId() != null;
            out.writeBoolean(hasLastSeenShardId);
            if (hasLastSeenShardId) {
                out.writeUTF(kinesisStreamsSourceEnumeratorState.getLastSeenShardId());
            }

            out.writeInt(kinesisStreamsSourceEnumeratorState.getUnassignedSplits().size());
            out.writeInt(splitSerializer.getVersion());
            for (KinesisShardSplit split :
                    kinesisStreamsSourceEnumeratorState.getUnassignedSplits()) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }

            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public KinesisStreamsSourceEnumeratorState deserialize(
            int version, byte[] serializedEnumeratorState) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedEnumeratorState);
                DataInputStream in = new DataInputStream(bais)) {

            if (version != getVersion()) {
                throw new VersionMismatchException(
                        "Trying to deserialize KinesisStreamsSourceEnumeratorState serialized with unsupported version "
                                + version
                                + ". Serializer version is "
                                + getVersion());
            }

            String lastSeenShardId = null;

            final boolean hasLastSeenShardId = in.readBoolean();
            if (hasLastSeenShardId) {
                lastSeenShardId = in.readUTF();
            }

            final int numUnassignedSplits = in.readInt();
            final int splitSerializerVersion = in.readInt();
            if (splitSerializerVersion != splitSerializer.getVersion()) {
                throw new VersionMismatchException(
                        "Trying to deserialize KinesisShardSplit serialized with unsupported version "
                                + splitSerializerVersion
                                + ". Serializer version is "
                                + splitSerializer.getVersion());
            }
            Set<KinesisShardSplit> unassignedSplits = new HashSet<>(numUnassignedSplits);
            for (int i = 0; i < numUnassignedSplits; i++) {
                int serializedLength = in.readInt();
                byte[] serializedSplit = new byte[serializedLength];
                if (in.read(serializedSplit) != -1) {
                    unassignedSplits.add(
                            splitSerializer.deserialize(splitSerializerVersion, serializedSplit));
                } else {
                    throw new IOException(
                            "Unexpectedly reading more bytes than is present in stream.");
                }
            }

            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes when deserializing.");
            }

            return new KinesisStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);
        }
    }
}
