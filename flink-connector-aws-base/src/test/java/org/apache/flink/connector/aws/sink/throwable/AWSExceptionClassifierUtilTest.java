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

package org.apache.flink.connector.aws.sink.throwable;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link AWSExceptionClassifierUtil}. */
public class AWSExceptionClassifierUtilTest {

    @Test
    public void shouldCreateFatalExceptionClassifierThatClassifiesAsFatalIfMatchingErrorCode() {
        // given
        AwsServiceException exception =
                StsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("NotAuthorizedException")
                                        .build())
                        .build();

        FatalExceptionClassifier classifier =
                AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                        StsException.class,
                        "NotAuthorizedException",
                        (err) -> new RuntimeException());

        // when (FatalExceptionClassifier#isFatal returns false if exception is fatal)
        boolean isFatal = !classifier.isFatal(exception, (err) -> {});

        // then
        assertTrue(isFatal);
    }

    @Test
    public void
            shouldCreateFatalExceptionClassifierThatClassifiesAsNonFatalIfNotMatchingErrorCode() {
        // given
        AwsServiceException exception =
                StsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder().errorCode("SomeOtherException").build())
                        .build();

        FatalExceptionClassifier classifier =
                AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                        StsException.class,
                        "NotAuthorizedException",
                        (err) -> new RuntimeException());

        // when (FatalExceptionClassifier#isFatal returns true if exception is non-fatal)
        boolean isFatal = !classifier.isFatal(exception, (err) -> {});

        // then
        assertFalse(isFatal);
    }

    @Test
    public void shouldCreateFatalExceptionClassifierThatAppliesThrowableMapper() {
        // given
        AwsServiceException exception =
                StsException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("NotAuthorizedException")
                                        .build())
                        .build();

        Exception mappedException =
                new RuntimeException(
                        "shouldCreateFatalExceptionClassifierThatAppliesThrowableMapper");
        FatalExceptionClassifier classifier =
                AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                        StsException.class, "NotAuthorizedException", (err) -> mappedException);

        Set<Exception> consumedExceptions = new HashSet<>();

        // when
        classifier.isFatal(exception, consumedExceptions::add);

        // then mappedException has been consumed
        assertEquals(1, consumedExceptions.size());
        assertTrue(consumedExceptions.contains(mappedException));
    }
}
