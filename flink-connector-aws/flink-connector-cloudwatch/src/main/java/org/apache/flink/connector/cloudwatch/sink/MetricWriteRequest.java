/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Pojo used as sink input, containing information for a single CloudWatch MetricDatum Object. */
@PublicEvolving
public class MetricWriteRequest implements Serializable {

    public String metricName;
    public Dimension[] dimensions;
    public Double[] values;
    public Double[] counts;
    public Instant timestamp;
    public String unit;
    public Integer storageResolution;
    public Double statisticMax;
    public Double statisticMin;
    public Double statisticSum;
    public Double statisticCount;

    public MetricWriteRequest() {}

    public MetricWriteRequest(
            String metricName,
            Dimension[] dimensions,
            Double[] values,
            Double[] counts,
            Instant timestamp,
            String unit,
            Integer storageResolution,
            Double statisticMax,
            Double statisticMin,
            Double statisticSum,
            Double statisticCount) {
        Preconditions.checkNotNull(metricName);

        this.metricName = metricName;
        this.dimensions = dimensions;
        this.values = values;
        this.counts = counts;
        this.timestamp = timestamp;
        this.unit = unit;
        this.storageResolution = storageResolution;
        this.statisticMax = statisticMax;
        this.statisticMin = statisticMin;
        this.statisticSum = statisticSum;
        this.statisticCount = statisticCount;
    }

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public Double[] getValues() {
        return values;
    }

    public String getMetricName() {
        return metricName;
    }

    public Double[] getCounts() {
        return counts;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getUnit() {
        return unit;
    }

    public Integer getStorageResolution() {
        return storageResolution;
    }

    public Double getStatisticMax() {
        return statisticMax;
    }

    public Double getStatisticMin() {
        return statisticMin;
    }

    public Double getStatisticSum() {
        return statisticSum;
    }

    public Double getStatisticCount() {
        return statisticCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A single Dimension. */
    public static class Dimension implements Serializable {
        public String name;
        public String value;

        public Dimension() {}

        public Dimension(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public software.amazon.awssdk.services.cloudwatch.model.Dimension toCloudWatchDimension() {
            return software.amazon.awssdk.services.cloudwatch.model.Dimension.builder()
                    .name(this.name)
                    .value(this.value)
                    .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Dimension label = (Dimension) o;
            return new EqualsBuilder()
                    .append(name, label.name)
                    .append(value, label.value)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(name).append(value).toHashCode();
        }

        @Override
        public String toString() {
            return "Dimension{name='" + name + "', value=" + value + "}";
        }
    }

    /** Builder for sink input pojo instance. */
    public static final class Builder {
        private final List<Dimension> dimensions = new ArrayList<>();
        private final List<Double> values = new ArrayList<>();
        private final List<Double> counts = new ArrayList<>();
        private String metricName;
        private Instant timestamp;
        private String unit;
        private Integer storageResolution;
        private Double statisticMax;
        private Double statisticMin;
        private Double statisticSum;
        private Double statisticCount;

        private Builder() {}

        public Builder withMetricName(String metricName) {
            this.metricName = metricName;
            return this;
        }

        public Builder addDimension(String dimensionName, String dimensionValue) {
            dimensions.add(new Dimension(dimensionName, dimensionValue));
            return this;
        }

        public Builder addValue(Double value) {
            values.add(value);
            return this;
        }

        public Builder addCount(Double count) {
            counts.add(count);
            return this;
        }

        public Builder withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withUnit(String unit) {
            this.unit = unit;
            return this;
        }

        public Builder withStorageResolution(Integer storageResolution) {
            this.storageResolution = storageResolution;
            return this;
        }

        public Builder withStatisticMax(Double statisticMax) {
            this.statisticMax = statisticMax;
            return this;
        }

        public Builder withStatisticMin(Double statisticMin) {
            this.statisticMin = statisticMin;
            return this;
        }

        public Builder withStatisticSum(Double statisticSum) {
            this.statisticSum = statisticSum;
            return this;
        }

        public Builder withStatisticCount(Double statisticCount) {
            this.statisticCount = statisticCount;
            return this;
        }

        public MetricWriteRequest build() {
            return new MetricWriteRequest(
                    metricName,
                    dimensions.toArray(new Dimension[0]),
                    values.toArray(new Double[0]),
                    counts.toArray(new Double[0]),
                    timestamp,
                    unit,
                    storageResolution,
                    statisticMax,
                    statisticMin,
                    statisticSum,
                    statisticCount);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricWriteRequest that = (MetricWriteRequest) o;
        return Arrays.equals(dimensions, that.dimensions)
                && Arrays.equals(values, that.values)
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(timestamp, that.timestamp)
                && Arrays.equals(counts, that.counts)
                && Objects.equals(unit, that.unit)
                && Objects.equals(storageResolution, that.storageResolution)
                && Objects.equals(statisticMax, that.statisticMax)
                && Objects.equals(statisticMin, that.statisticMin)
                && Objects.equals(statisticSum, that.statisticSum)
                && Objects.equals(statisticCount, that.statisticCount);
    }

    @Override
    public int hashCode() {
        Integer result = Objects.hash(metricName);
        result = 31 * result + Arrays.hashCode(dimensions);
        result = 31 * result + Arrays.hashCode(values);
        result = 31 * result + Arrays.hashCode(counts);
        result = 31 * result + Objects.hash(timestamp);
        result = 31 * result + Objects.hashCode(unit);
        result = 31 * result + Objects.hashCode(storageResolution);
        result = 31 * result + Objects.hashCode(statisticMax);
        result = 31 * result + Objects.hashCode(statisticMin);
        result = 31 * result + Objects.hashCode(statisticSum);
        result = 31 * result + Objects.hashCode(statisticCount);
        return result;
    }

    @Override
    public String toString() {
        return toMetricDatum().toString();
    }

    public MetricDatum toMetricDatum() {
        MetricDatum.Builder builder =
                MetricDatum.builder().metricName(metricName).values(values).counts(counts);

        Optional.ofNullable(timestamp).ifPresent(builder::timestamp);
        Optional.ofNullable(unit).ifPresent(builder::unit);
        Optional.ofNullable(storageResolution).ifPresent(builder::storageResolution);

        if (dimensions.length > 0) {
            builder.dimensions(
                    Arrays.stream(dimensions)
                            .map(Dimension::toCloudWatchDimension)
                            .collect(Collectors.toList()));
        }

        if (statisticMax != null
                | statisticMin != null
                | statisticSum != null
                | statisticCount != null) {
            StatisticSet.Builder statisticBuilder = StatisticSet.builder();
            Optional.ofNullable(statisticMax).ifPresent(statisticBuilder::maximum);
            Optional.ofNullable(statisticMin).ifPresent(statisticBuilder::minimum);
            Optional.ofNullable(statisticSum).ifPresent(statisticBuilder::sum);
            Optional.ofNullable(statisticCount).ifPresent(statisticBuilder::sampleCount);
            builder.statisticValues(statisticBuilder.build());
        }

        return builder.build();
    }
}
