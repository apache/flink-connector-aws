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

package org.apache.flink.connector.kinesis.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

/**
 * Serializes and deserializes the {@link KinesisShardSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class KinesisShardSplitSerializer implements SimpleVersionedSerializer<KinesisShardSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KinesisShardSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeUTF(split.getStreamArn());
            out.writeUTF(split.getShardId());
            out.writeUTF(split.getStartingPosition().getShardIteratorType().toString());
            if (split.getStartingPosition().getStartingMarker() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Object startingMarker = split.getStartingPosition().getStartingMarker();
                out.writeBoolean(startingMarker instanceof Instant);
                if (startingMarker instanceof Instant) {
                    out.writeLong(((Instant) startingMarker).toEpochMilli());
                }
                out.writeBoolean(startingMarker instanceof String);
                if (startingMarker instanceof String) {
                    out.writeUTF((String) startingMarker);
                }
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KinesisShardSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            if (version != getVersion()) {
                throw new VersionMismatchException(
                        "Trying to deserialize KinesisShardSplit serialized with unsupported version "
                                + version
                                + ". Version of serializer is "
                                + getVersion());
            }

            final String streamArn = in.readUTF();
            final String shardId = in.readUTF();
            final ShardIteratorType shardIteratorType = ShardIteratorType.fromValue(in.readUTF());
            Object startingMarker = null;

            final boolean hasStartingMarker = in.readBoolean();
            if (hasStartingMarker) {
                if (in.readBoolean()) {
                    startingMarker = Instant.ofEpochMilli(in.readLong());
                }
                if (in.readBoolean()) {
                    startingMarker = in.readUTF();
                }
            }

            return new KinesisShardSplit(
                    streamArn, shardId, new StartingPosition(shardIteratorType, startingMarker));
        }
    }
}
