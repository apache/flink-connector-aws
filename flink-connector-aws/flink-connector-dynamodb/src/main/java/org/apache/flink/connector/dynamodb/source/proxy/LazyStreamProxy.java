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

package org.apache.flink.connector.dynamodb.source.proxy;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A stream proxy that the split readers of a source reader share. The proxy is created when the
 * first split reader needs it, so that a source reader that never gets a split opens no AWS SDK
 * clients.
 */
@Internal
public final class LazyStreamProxy implements Closeable {

    private final Supplier<StreamProxy> proxyFactory;

    @Nullable private StreamProxy proxy;
    private boolean closed;

    public LazyStreamProxy(Supplier<StreamProxy> proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    /** Returns the proxy, and creates it on the first call. */
    public synchronized StreamProxy get() {
        if (closed) {
            throw new IllegalStateException("The stream proxy is closed.");
        }
        if (proxy == null) {
            proxy = proxyFactory.get();
        }
        return proxy;
    }

    /** Closes the proxy if it was created. */
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (proxy != null) {
            proxy.close();
        }
    }
}
