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

package org.apache.flink.connector.sqs.sink;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/** Covers construction, defaults and sanity checking of {@link SqsSinkBuilder}. */
class SqsSinkBuilderTest {

    @Test
    void sqsUrlOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> SqsSink.<String>builder().build())
                .withMessageContaining(
                        "The sqs url must not be null when initializing the SQS Sink.");
    }

    @Test
    void sqsUrlOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                SqsSink.<String>builder()
                                        .setSqsUrl("")
                                        .setSqsClientProperties(new Properties())
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining("The sqs url must be set when initializing the SQS Sink.");
    }
}
