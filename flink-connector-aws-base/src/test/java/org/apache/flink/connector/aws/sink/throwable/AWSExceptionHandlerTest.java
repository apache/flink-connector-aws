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

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link AWSExceptionHandler}. */
public class AWSExceptionHandlerTest {

    private final RuntimeException mappedException =
            new RuntimeException("AWSExceptionHandlerTest");

    private final AWSExceptionHandler exceptionHandler =
            AWSExceptionHandler.withClassifier(
                    FatalExceptionClassifier.withRootCauseOfType(
                            UnsupportedOperationException.class, (err) -> mappedException));

    @Test
    public void shouldReturnTrueIfFatal() {
        assertTrue(
                exceptionHandler.consumeIfFatal(new UnsupportedOperationException(), (err) -> {}));
    }

    @Test
    public void shouldReturnFalseIfNonFatal() {
        assertFalse(exceptionHandler.consumeIfFatal(new IndexOutOfBoundsException(), (err) -> {}));
    }

    @Test
    public void shouldConsumeMappedExceptionIfFatal() {
        Set<Exception> consumedExceptions = new HashSet<>();
        assertTrue(
                exceptionHandler.consumeIfFatal(
                        new UnsupportedOperationException(), consumedExceptions::add));

        assertEquals(1, consumedExceptions.size());
        assertTrue(consumedExceptions.contains(mappedException));
    }

    @Test
    public void shouldNotConsumeMappedExceptionIfNonFatal() {
        assertFalse(
                exceptionHandler.consumeIfFatal(
                        new IndexOutOfBoundsException(),
                        // consumer should not be called
                        (err) -> assertTrue(false)));
    }
}
