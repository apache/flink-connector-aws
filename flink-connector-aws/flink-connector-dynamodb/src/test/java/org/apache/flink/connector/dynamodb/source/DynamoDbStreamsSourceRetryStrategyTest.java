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

package org.apache.flink.connector.dynamodb.source;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the retry strategy of the non-blocking calls of {@link DynamoDbStreamsSource}. */
class DynamoDbStreamsSourceRetryStrategyTest {

    /** More first retries than the token bucket of the default strategies allows. */
    private static final int RETRIES = 1_000;

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testNonBlockingRetryStrategyFollowsRetryMode(RetryMode retryMode) {
        RetryStrategy retryStrategy = DynamoDbStreamsSource.nonBlockingRetryStrategy(retryMode);

        if (retryMode == RetryMode.LEGACY) {
            assertThat(retryStrategy).isInstanceOf(LegacyRetryStrategy.class);
        } else {
            assertThat(retryStrategy).isInstanceOf(StandardRetryStrategy.class);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = RetryMode.class,
            names = {"LEGACY", "STANDARD"})
    void testNonBlockingRetryStrategyHasNoCircuitBreaker(RetryMode retryMode) {
        RetryStrategy retryStrategy = DynamoDbStreamsSource.nonBlockingRetryStrategy(retryMode);

        for (int i = 0; i < RETRIES; i++) {
            retryOnce(retryStrategy);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = RetryMode.class,
            names = {"LEGACY", "STANDARD"})
    void testDefaultRetryStrategyHasCircuitBreaker(RetryMode retryMode) {
        RetryStrategy retryStrategy = SdkDefaultRetryStrategy.forRetryMode(retryMode);

        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < RETRIES; i++) {
                                retryOnce(retryStrategy);
                            }
                        })
                .isInstanceOf(TokenAcquisitionFailedException.class);
    }

    /** Starts a call and retries it once after a retryable error, as an SDK client does. */
    private static void retryOnce(RetryStrategy retryStrategy) {
        RetryToken token =
                retryStrategy
                        .acquireInitialToken(AcquireInitialTokenRequest.create("dynamodb-streams"))
                        .token();
        retryStrategy.refreshRetryToken(
                RefreshRetryTokenRequest.builder()
                        .token(token)
                        .failure(RetryableException.create("retryable error"))
                        .build());
    }
}
