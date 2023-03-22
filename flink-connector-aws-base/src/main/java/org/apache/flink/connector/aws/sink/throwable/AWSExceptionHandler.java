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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import java.util.function.Consumer;

/**
 * Class to provide improved semantics over {@link FatalExceptionClassifier#isFatal(Throwable,
 * Consumer)}. {@link FatalExceptionClassifier#isFatal(Throwable, Consumer)} returns `false` for a
 * fatal exception and `true` for a non-fatal exception.
 */
@Internal
public class AWSExceptionHandler {

    private final FatalExceptionClassifier classifier;

    private AWSExceptionHandler(FatalExceptionClassifier classifier) {
        this.classifier = classifier;
    }

    public static AWSExceptionHandler withClassifier(FatalExceptionClassifier classifier) {
        return new AWSExceptionHandler(classifier);
    }

    /**
     * Passes a given {@link Throwable} t to a given {@link Consumer} consumer if the throwable is
     * fatal. Returns `true` if the {@link Throwable} has been passed to the {@link Consumer} (i.e.
     * it is fatal) and `false` otherwise.
     *
     * @param t a {@link Throwable}
     * @param consumer a {@link Consumer} to call if the passed throwable t is fatal.
     * @return `true` if t is fatal, `false` otherwise.
     */
    public boolean consumeIfFatal(Throwable t, Consumer<Exception> consumer) {
        return !classifier.isFatal(t, consumer);
    }
}
