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

import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.getTestStreamProxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LazyStreamProxy}. */
class LazyStreamProxyTest {

    @Test
    void testCreatesProxyOnceOnFirstGet() throws Exception {
        AtomicInteger createdProxies = new AtomicInteger();
        TestDynamoDbStreamsProxy proxy = getTestStreamProxy();
        LazyStreamProxy lazyStreamProxy =
                new LazyStreamProxy(
                        () -> {
                            createdProxies.incrementAndGet();
                            return proxy;
                        });
        assertThat(createdProxies.get()).isZero();

        assertThat(lazyStreamProxy.get()).isSameAs(proxy);
        assertThat(lazyStreamProxy.get()).isSameAs(proxy);
        assertThat(createdProxies.get()).isEqualTo(1);

        lazyStreamProxy.close();
        assertThat(proxy.isClosed()).isTrue();
    }

    @Test
    void testCloseWithoutGetCreatesNoProxy() throws Exception {
        AtomicInteger createdProxies = new AtomicInteger();
        LazyStreamProxy lazyStreamProxy =
                new LazyStreamProxy(
                        () -> {
                            createdProxies.incrementAndGet();
                            return getTestStreamProxy();
                        });

        lazyStreamProxy.close();

        assertThat(createdProxies.get()).isZero();
        assertThatThrownBy(lazyStreamProxy::get).isInstanceOf(IllegalStateException.class);
    }
}
