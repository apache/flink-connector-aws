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

package org.apache.flink.connector.dynamodb.source.util;

import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

/** Provides {@link TestingReaderContext} with mocked DynamoDb Stream behaviour. */
public class DynamoDbStreamsContextProvider {
    /**
     * An implementation of the {@link TestingReaderContext} that allows control over ReaderContext
     * methods.
     */
    public static class DynamoDbStreamsTestingContext extends TestingReaderContext {
        public static DynamoDbStreamsTestingContext getDynamoDbStreamsTestingContext(
                MetricListener metricListener) {
            return new DynamoDbStreamsTestingContext(metricListener);
        }

        private final MetricListener metricListener;

        public DynamoDbStreamsTestingContext(MetricListener metricListener) {
            this.metricListener = metricListener;
        }

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return InternalSourceReaderMetricGroup.mock(this.metricListener.getMetricGroup());
        }
    }
}
