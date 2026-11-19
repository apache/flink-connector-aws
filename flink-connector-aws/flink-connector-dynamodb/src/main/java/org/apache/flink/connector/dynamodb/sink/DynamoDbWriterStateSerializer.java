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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.dynamodb.util.DynamoDbSerializationUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** DynamoDb implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class DynamoDbWriterStateSerializer
        extends AsyncSinkWriterStateSerializer<DynamoDbWriteRequest> {

    // The parent class does not pass the version to deserializeRequestFromStream(),
    // so we stash it here before calling super.deserialize() and read it back in
    // deserializeRequestFromStream().
    private final ThreadLocal<Integer> deserializingVersion =
            ThreadLocal.withInitial(this::getVersion);

    /**
     * Serializes {@link DynamoDbWriteRequest} in form of
     * [TABLE_NAME,WRITE_REQUEST_TYPE(PUT/DELETE),WRITE_REQUEST].
     */
    @Override
    protected void serializeRequestToStream(DynamoDbWriteRequest request, DataOutputStream out)
            throws IOException {
        DynamoDbSerializationUtil.serializeWriteRequest(request, out);
    }

    @Override
    protected DynamoDbWriteRequest deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        return DynamoDbSerializationUtil.deserializeWriteRequest(in, deserializingVersion.get());
    }

    @Override
    public BufferedRequestState<DynamoDbWriteRequest> deserialize(int version, byte[] serialized)
            throws IOException {
        deserializingVersion.set(version);
        try {
            return super.deserialize(version, serialized);
        } finally {
            deserializingVersion.set(getVersion());
        }
    }

    @Override
    public int getVersion() {
        return 2;
    }
}
