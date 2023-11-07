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

package org.apache.flink.streaming.connectors.kinesis.config;

import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RecoverableErrorsConfig}. */
public class RecoverableErrorsConfigTest {

    @Test
    public void testParseConfigFromProperties() {
        Properties config = new Properties();
        config.setProperty(
                "flink.shard.consumer.error.recoverable[0].exception",
                "java.net.UnknownHostException");
        config.setProperty(
                "flink.shard.consumer.error.recoverable[1].exception",
                "java.lang.IllegalArgumentException");
        Optional<RecoverableErrorsConfig> recoverableErrorsConfigOptional =
                RecoverableErrorsConfig.createConfigFromPropertiesOrThrow(config);
        assertTrue(recoverableErrorsConfigOptional.isPresent());
        RecoverableErrorsConfig recoverableErrorsConfig = recoverableErrorsConfigOptional.get();
        assertFalse(recoverableErrorsConfig.hasNoConfig());
        assertThat(recoverableErrorsConfig.getExceptionConfigs().size()).isEqualTo(2);
        assertThat(recoverableErrorsConfig.getExceptionConfigs().get(0).getExceptionClass())
                .isEqualTo(java.net.UnknownHostException.class);
        assertThat(recoverableErrorsConfig.getExceptionConfigs().get(1).getExceptionClass())
                .isEqualTo(java.lang.IllegalArgumentException.class);
    }

    @Test
    public void testReturnEmptyWhenConfigNotFound() {
        Optional<RecoverableErrorsConfig> recoverableErrorsConfigOptional =
                RecoverableErrorsConfig.createConfigFromPropertiesOrThrow(new Properties());
        assertFalse(recoverableErrorsConfigOptional.isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsExceptionWhenProvidedClassIsNotThrowable() {
        Properties config = new Properties();
        config.setProperty(
                "flink.shard.consumer.error.recoverable[0].exception", "java.util.Properties");
        RecoverableErrorsConfig.createConfigFromPropertiesOrThrow(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsExceptionWhenProvidedClassCanNotBeFound() {
        Properties config = new Properties();
        config.setProperty(
                "flink.shard.consumer.error.recoverable[0].exception", "made.up.TestClass");
        RecoverableErrorsConfig.createConfigFromPropertiesOrThrow(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsExceptionWhenProvidedConfigSuffixIsNotValid() {
        Properties config = new Properties();
        config.setProperty(
                "flink.shard.consumer.error.recoverable[0].exceptionnm", "java.lang.Exception");
        RecoverableErrorsConfig.createConfigFromPropertiesOrThrow(config);
    }
}
