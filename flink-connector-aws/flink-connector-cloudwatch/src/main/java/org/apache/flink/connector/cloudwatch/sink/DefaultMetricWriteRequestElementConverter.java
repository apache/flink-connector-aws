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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.util.UnknownFormatConversionException;

/**
 * An implementation of the {@link ElementConverter} that uses the AWS CloudWatch SDK v2. The user
 * needs to provide the {@code InputT} in the form of {@link MetricWriteRequest}.
 *
 * <p>The InputT needs to be structured and only supports {@link MetricWriteRequest}
 */
@Internal
public class DefaultMetricWriteRequestElementConverter<InputT>
        extends MetricWriteRequestElementConverter<InputT> {

    @Override
    public MetricWriteRequest apply(InputT element, SinkWriter.Context context) {
        if (element instanceof MetricWriteRequest) {
            return (MetricWriteRequest) element;
        }

        throw new UnknownFormatConversionException(
                "DefaultMetricWriteRequestElementConverter only supports MetricWriteRequest element.");
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the DefaultMetricWriteRequestElementConverter. */
    public static class Builder<InputT> {

        public DefaultMetricWriteRequestElementConverter<InputT> build() {
            return new DefaultMetricWriteRequestElementConverter<>();
        }
    }
}
