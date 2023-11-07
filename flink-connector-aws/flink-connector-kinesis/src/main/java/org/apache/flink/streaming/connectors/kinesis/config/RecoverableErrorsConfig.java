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

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Hosts the recoverable exception configuration. Recoverable exceptions are retried indefinitely.
 */
public class RecoverableErrorsConfig {
    public static final String INVALID_CONFIG_MESSAGE =
            "Invalid config for recoverable consumer exceptions. "
                    + "Valid config example: "
                    + "`flink.shard.consumer.error.recoverable[0].exception=net.java.UnknownHostException`. "
                    + "Your config array must use zero-based indexing as shown in the example.";

    /**
     * Parses the array of recoverable error configs.
     *
     * @param config connector configuration
     * @return an Optional of RecoverableErrorsConfig
     */
    public static Optional<RecoverableErrorsConfig> createConfigFromPropertiesOrThrow(
            final Properties config) {
        List<ExceptionConfig> exConfs = new ArrayList<>();
        int idx = 0;
        String exceptionConfigKey =
                String.format(
                        "%s[%d].exception",
                        ConsumerConfigConstants.RECOVERABLE_EXCEPTIONS_PREFIX, idx);
        while (config.containsKey(exceptionConfigKey)) {
            String exPath = config.getProperty(exceptionConfigKey);
            try {
                Class<?> aClass = Class.forName(exPath);
                if (!Throwable.class.isAssignableFrom(aClass)) {
                    throw new ClassCastException();
                }
                exConfs.add(new ExceptionConfig(aClass));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        "Provided recoverable exception class is not a Throwable: " + exPath);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                        "Provided recoverable exception class could not be found: " + exPath);
            }
            exceptionConfigKey =
                    String.format(
                            "%s[%d].exception",
                            ConsumerConfigConstants.RECOVERABLE_EXCEPTIONS_PREFIX, ++idx);
        }
        if (idx > 0) {
            // We processed configs successfully
            return Optional.of(new RecoverableErrorsConfig(exConfs));
        }

        // Check if user provided wrong config suffix, so they fail faster
        for (Object key : config.keySet()) {
            if (((String) key).startsWith(ConsumerConfigConstants.RECOVERABLE_EXCEPTIONS_PREFIX)) {
                throw new IllegalArgumentException(RecoverableErrorsConfig.INVALID_CONFIG_MESSAGE);
            }
        }

        return Optional.empty();
    }

    private final List<ExceptionConfig> exceptionConfigs;

    public RecoverableErrorsConfig(List<ExceptionConfig> exceptionConfigs) {
        this.exceptionConfigs = exceptionConfigs;
    }

    public boolean hasNoConfig() {
        return CollectionUtils.isEmpty(exceptionConfigs);
    }

    public List<ExceptionConfig> getExceptionConfigs() {
        return exceptionConfigs;
    }
}
