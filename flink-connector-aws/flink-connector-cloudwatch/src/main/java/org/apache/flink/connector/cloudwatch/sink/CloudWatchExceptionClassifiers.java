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
import org.apache.flink.connector.aws.sink.throwable.AWSExceptionClassifierUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.ResourceNotFoundException;

/**
 * Class containing set of {@link FatalExceptionClassifier} for {@link
 * software.amazon.awssdk.services.cloudwatch.model.CloudWatchException}.
 */
@Internal
public class CloudWatchExceptionClassifiers {

    public static FatalExceptionClassifier getNotAuthorizedExceptionClassifier() {
        return AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                CloudWatchException.class,
                "NotAuthorized",
                err ->
                        new CloudWatchSinkException(
                                "Encountered non-recoverable exception: NotAuthorized", err));
    }

    public static FatalExceptionClassifier getAccessDeniedExceptionClassifier() {
        return AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                CloudWatchException.class,
                "AccessDeniedException",
                err ->
                        new CloudWatchSinkException(
                                "Encountered non-recoverable exception: AccessDeniedException",
                                err));
    }

    public static FatalExceptionClassifier getResourceNotFoundExceptionClassifier() {
        return FatalExceptionClassifier.withRootCauseOfType(
                ResourceNotFoundException.class,
                err ->
                        new CloudWatchSinkException(
                                "Encountered non-recoverable exception relating to not being able to find the specified resources",
                                err));
    }
}
