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

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import software.amazon.awssdk.utils.StringUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** CloudWatch implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class CloudWatchStateSerializer extends AsyncSinkWriterStateSerializer<MetricWriteRequest> {
    private static final Set<Integer> COMPATIBLE_VERSIONS =
            new HashSet<>(Collections.singletonList(0));
    private static final int CURRENT_VERSION = 0;

    @Override
    protected void serializeRequestToStream(
            final MetricWriteRequest request, final DataOutputStream out) throws IOException {

        out.writeInt(getVersion());
        serializeMetricName(request.getMetricName(), out);
        serializeValues(request, out);
        serializeCounts(request, out);
        serializeDimensions(request, out);
        serializeDoubleValue(request.getStatisticMax(), out);
        serializeDoubleValue(request.getStatisticMin(), out);
        serializeDoubleValue(request.getStatisticSum(), out);
        serializeDoubleValue(request.getStatisticCount(), out);
        serializeUnit(request.getUnit(), out);
        serializeStorageResolution(request.getStorageResolution(), out);
        serializeTimestamp(request.getTimestamp(), out);
    }

    @Override
    protected MetricWriteRequest deserializeRequestFromStream(
            final long requestSize, final DataInputStream in) throws IOException {
        final int stateSerializerVersion = in.readInt();

        if (!COMPATIBLE_VERSIONS.contains(stateSerializerVersion)) {
            throw new VersionMismatchException(
                    "Trying to deserialize CloudWatchState serialized with unsupported version "
                            + stateSerializerVersion
                            + ". Serializer version is "
                            + getVersion());
        }

        MetricWriteRequest.Builder builder = MetricWriteRequest.builder();

        builder.withMetricName(deserializeMetricName(in));
        deserializeValues(in).forEach(builder::addValue);
        deserializeCounts(in).forEach(builder::addCount);
        deserializeDimensions(in)
                .forEach(
                        dimension ->
                                builder.addDimension(dimension.getName(), dimension.getValue()));
        Optional.ofNullable(deserializeDoubleValue(in)).ifPresent(builder::withStatisticMax);
        Optional.ofNullable(deserializeDoubleValue(in)).ifPresent(builder::withStatisticMin);
        Optional.ofNullable(deserializeDoubleValue(in)).ifPresent(builder::withStatisticSum);
        Optional.ofNullable(deserializeDoubleValue(in)).ifPresent(builder::withStatisticCount);
        Optional.ofNullable(deserializeUnit(in)).ifPresent(builder::withUnit);
        Optional.ofNullable(deserializeStorageResolution(in))
                .ifPresent(builder::withStorageResolution);
        Optional.ofNullable(deserializeTimestamp(in)).ifPresent(builder::withTimestamp);

        return builder.build();
    }

    private void serializeMetricName(String metricName, DataOutputStream out) throws IOException {
        out.writeUTF(metricName);
    }

    private String deserializeMetricName(DataInputStream in) throws IOException {
        return in.readUTF();
    }

    private void serializeValues(MetricWriteRequest request, DataOutputStream out)
            throws IOException {
        boolean hasValues = request.getValues() != null && request.getValues().length > 0;
        out.writeBoolean(hasValues);
        if (hasValues) {
            out.writeInt(request.getValues().length);
            for (Double value : request.getValues()) {
                out.writeDouble(value);
            }
        }
    }

    private List<Double> deserializeValues(DataInputStream in) throws IOException {
        return getDoubles(in);
    }

    private void serializeCounts(MetricWriteRequest request, DataOutputStream out)
            throws IOException {
        boolean hasCounts = request.getCounts() != null && request.getCounts().length > 0;
        out.writeBoolean(hasCounts);
        if (hasCounts) {
            out.writeInt(request.getCounts().length);
            for (Double count : request.getCounts()) {
                out.writeDouble(count);
            }
        }
    }

    private List<Double> deserializeCounts(DataInputStream in) throws IOException {
        return getDoubles(in);
    }

    private List<Double> getDoubles(DataInputStream in) throws IOException {
        boolean hasDoubles = in.readBoolean();
        if (!hasDoubles) {
            return new ArrayList<>();
        }

        int size = in.readInt();
        List<Double> doubles = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            doubles.add(in.readDouble());
        }
        return doubles;
    }

    private void serializeDimensions(MetricWriteRequest request, DataOutputStream out)
            throws IOException {
        boolean hasDimensions =
                request.getDimensions() != null && request.getDimensions().length > 0;
        out.writeBoolean(hasDimensions);
        if (hasDimensions) {
            out.writeInt(request.getDimensions().length);
            for (MetricWriteRequest.Dimension dimension : request.getDimensions()) {
                out.writeUTF(dimension.getName());
                out.writeUTF(dimension.getValue());
            }
        }
    }

    private List<MetricWriteRequest.Dimension> deserializeDimensions(DataInputStream in)
            throws IOException {
        boolean hasDimensions = in.readBoolean();
        if (!hasDimensions) {
            return new ArrayList<>();
        }

        int size = in.readInt();
        List<MetricWriteRequest.Dimension> dimensions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            String value = in.readUTF();
            dimensions.add(new MetricWriteRequest.Dimension(name, value));
        }
        return dimensions;
    }

    private void serializeDoubleValue(Double value, DataOutputStream out) throws IOException {
        boolean hasValue = value != null;

        out.writeBoolean(hasValue);
        if (hasValue) {
            out.writeDouble(value);
        }
    }

    private Double deserializeDoubleValue(DataInputStream in) throws IOException {
        boolean hasValue = in.readBoolean();
        if (!hasValue) {
            return null;
        }
        return in.readDouble();
    }

    private void serializeUnit(String unit, DataOutputStream out) throws IOException {
        boolean hasUnit = !StringUtils.isEmpty(unit);
        out.writeBoolean(hasUnit);
        if (hasUnit) {
            out.writeUTF(unit);
        }
    }

    private String deserializeUnit(DataInputStream in) throws IOException {
        boolean hasUnit = in.readBoolean();
        if (!hasUnit) {
            return null;
        }
        return in.readUTF();
    }

    private void serializeStorageResolution(Integer storageRes, DataOutputStream out)
            throws IOException {
        boolean hasStorageResolution = storageRes != null;
        out.writeBoolean(hasStorageResolution);
        if (hasStorageResolution) {
            out.writeInt(storageRes);
        }
    }

    private Integer deserializeStorageResolution(DataInputStream in) throws IOException {
        boolean hasStorageResolution = in.readBoolean();
        if (!hasStorageResolution) {
            return null;
        }
        return in.readInt();
    }

    private void serializeTimestamp(Instant timestamp, DataOutputStream out) throws IOException {
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeLong(timestamp.toEpochMilli());
        }
    }

    private Instant deserializeTimestamp(DataInputStream in) throws IOException {
        boolean hasTimestamp = in.readBoolean();
        if (!hasTimestamp) {
            return null;
        }
        return Instant.ofEpochMilli(in.readLong());
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }
}
