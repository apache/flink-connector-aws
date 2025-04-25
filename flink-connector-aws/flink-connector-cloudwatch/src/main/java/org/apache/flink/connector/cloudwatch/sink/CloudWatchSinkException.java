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

/**
 * A {@link RuntimeException} wrapper indicating the exception was thrown from the CloudWatch Sink.
 */
@PublicEvolving
class CloudWatchSinkException extends RuntimeException {

    public CloudWatchSinkException(final String message) {
        super(message);
    }

    public CloudWatchSinkException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * When the flag {@code failOnError} is set in {@link CloudWatchSinkWriter}, this exception is
     * raised as soon as any exception occurs when writing to CloudWatch.
     */
    static class CloudWatchFailFastSinkException extends CloudWatchSinkException {

        private static final String ERROR_MESSAGE =
                "Encountered an exception while persisting records, not retrying due to {failOnError} being set.";

        public CloudWatchFailFastSinkException() {
            super(ERROR_MESSAGE);
        }

        public CloudWatchFailFastSinkException(final String errorMessage) {
            super(errorMessage);
        }

        public CloudWatchFailFastSinkException(final String errorMessage, final Throwable cause) {
            super(errorMessage, cause);
        }

        public CloudWatchFailFastSinkException(final Throwable cause) {
            super(ERROR_MESSAGE, cause);
        }
    }
}
