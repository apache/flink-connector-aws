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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** Unit tests for {@link AWSFirehoseExceptionClassifiers}. */
public class AWSFirehoseExceptionClassifiersTest {

    private final FatalExceptionClassifier classifier =
            FatalExceptionClassifier.createChain(
                    AWSFirehoseExceptionClassifiers.getAccessDeniedExceptionClassifier(),
                    AWSFirehoseExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
                    AWSFirehoseExceptionClassifiers.getResourceNotFoundExceptionClassifier());

    @Test
    public void shouldClassifyNotAuthorizedAsFatal() {
        AwsServiceException firehoseException =
                FirehoseException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder().errorCode("NotAuthorized").build())
                        .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }

    @Test
    public void shouldClassifyAccessDeniedExceptionAsFatal() {
        AwsServiceException firehoseException =
                FirehoseException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("AccessDeniedException")
                                        .build())
                        .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }

    @Test
    public void shouldClassifyResourceNotFoundAsFatal() {
        AwsServiceException firehoseException = ResourceNotFoundException.builder().build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }
}
