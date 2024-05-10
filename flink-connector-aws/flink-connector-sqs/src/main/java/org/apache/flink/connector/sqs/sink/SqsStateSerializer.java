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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** SQS implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class SqsStateSerializer
        extends AsyncSinkWriterStateSerializer<SendMessageBatchRequestEntry> {
    @Override
    protected void serializeRequestToStream(
            final SendMessageBatchRequestEntry request, final DataOutputStream out)
            throws IOException {
        out.write(request.messageBody().getBytes(StandardCharsets.UTF_8));
        serializeRequestId(request.id(), out);
    }

    protected void serializeRequestId(String requestId, DataOutputStream out) throws IOException {
        out.writeInt(requestId.length());
        out.write(requestId.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected SendMessageBatchRequestEntry deserializeRequestFromStream(
            final long requestSize, final DataInputStream in) throws IOException {
        final byte[] requestData = new byte[(int) requestSize];
        in.read(requestData);
        return SendMessageBatchRequestEntry.builder()
                .id(deserializeRequestId(in))
                .messageBody(new String(requestData, StandardCharsets.UTF_8))
                .build();
    }

    protected String deserializeRequestId(DataInputStream in) throws IOException {
        int requestIdLength = in.readInt();
        byte[] requestIdData = new byte[(int) requestIdLength];
        in.read(requestIdData);
        return new String(requestIdData, StandardCharsets.UTF_8);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
