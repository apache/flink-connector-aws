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

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sqs.model.SqsException;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Unit tests for {@link SqsExceptionClassifiers}. */
public class SqsExceptionClassifiersTest {

    private final FatalExceptionClassifier classifier =
            FatalExceptionClassifier.createChain(
                    SqsExceptionClassifiers.getAccessDeniedExceptionClassifier(),
                    SqsExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
                    SqsExceptionClassifiers.getResourceNotFoundExceptionClassifier());

    @Test
    public void shouldClassifyNotAuthorizedAsFatal() {
        AwsServiceException sqsException =
                SqsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder().errorCode("NotAuthorized").build())
                        .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(sqsException, ex -> {}));
    }

    @Test
    public void shouldClassifyAccessDeniedExceptionAsFatal() {
        AwsServiceException sqsException =
                SqsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("AccessDeniedException")
                                        .build())
                        .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(sqsException, ex -> {}));
    }

    @Test
    public void shouldClassifySocketTimeoutExceptionAsNonFatal() {
        AwsServiceException sqsException =
                SqsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SocketTimeoutException")
                                        .build())
                        .build();

        // isFatal returns `true` if an exception is non-fatal
        assertTrue(classifier.isFatal(sqsException, ex -> {}));
    }

    @Test
    public void shouldClassifyResourceNotFoundAsFatal() {
        AwsServiceException sqsException = ResourceNotFoundException.builder().build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(sqsException, ex -> {}));
    }
}
