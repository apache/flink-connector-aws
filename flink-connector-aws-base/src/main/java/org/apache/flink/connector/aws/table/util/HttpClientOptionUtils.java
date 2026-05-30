/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.aws.table.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;
import org.apache.flink.connector.base.table.options.TableOptionsUtils;
import org.apache.flink.connector.base.table.util.ConfigurationValidatorUtil;

import software.amazon.awssdk.http.Protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Class for handling AWS HTTP Client config options. */
@PublicEvolving
public class HttpClientOptionUtils implements TableOptionsUtils, ConfigurationValidator {
    public static final String CLIENT_PREFIX = "http-client.";
    private static final String CLIENT_HTTP_PROTOCOL_VERSION_OPTION = "protocol.version";
    private static final String CLIENT_HTTP_MAX_CONNECTION_TIMEOUT_MS = "connection-timeout-ms";
    private static final String CLIENT_HTTP_MAX_SOCKET_TIMEOUT_MS = "socket-timeout-ms";
    private static final String APACHE_MAX_CONNECTIONS = "apache.max-connections";

    private final List<String> allowedClientTypes;
    private final Map<String, String> resolvedOptions;

    public HttpClientOptionUtils(String[] allowedClientTypes, Map<String, String> resolvedOptions) {
        this.allowedClientTypes = Arrays.asList(allowedClientTypes);
        this.resolvedOptions = resolvedOptions;
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties clientConfigurations = new Properties();
        clientConfigurations.putAll(getProcessedResolvedOptions());
        validateClientType(clientConfigurations);
        validateConfigurations(clientConfigurations);
        return clientConfigurations;
    }

    @Override
    public Map<String, String> getProcessedResolvedOptions() {
        Map<String, String> mappedResolvedOptions = new HashMap<>();
        for (String key : resolvedOptions.keySet()) {
            if (key.startsWith(CLIENT_PREFIX)) {
                mappedResolvedOptions.put(translateClientKeys(key), resolvedOptions.get(key));
            }
        }
        return mappedResolvedOptions;
    }

    @Override
    public List<String> getNonValidatedPrefixes() {
        return Collections.singletonList(CLIENT_PREFIX);
    }

    private static String translateClientKeys(String key) {
        String truncatedKey = key.substring(CLIENT_PREFIX.length());
        switch (truncatedKey) {
            case CLIENT_HTTP_PROTOCOL_VERSION_OPTION:
                return AWSConfigConstants.HTTP_PROTOCOL_VERSION;
            case CLIENT_HTTP_MAX_CONNECTION_TIMEOUT_MS:
                return AWSConfigConstants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS;
            case CLIENT_HTTP_MAX_SOCKET_TIMEOUT_MS:
                return AWSConfigConstants.HTTP_CLIENT_SOCKET_TIMEOUT_MS;
            case APACHE_MAX_CONNECTIONS:
                return AWSConfigConstants.HTTP_CLIENT_APACHE_MAX_CONNECTIONS;
            default:
                return "aws.http-client." + truncatedKey;
        }
    }

    private void validateConfigurations(Properties config) {
        ConfigurationValidatorUtil.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS,
                "Invalid value given for HTTP connection timeout. Must be positive integer.");
        ConfigurationValidatorUtil.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_SOCKET_TIMEOUT_MS,
                "Invalid value given for HTTP socket read timeout. Must be positive integer.");
        ConfigurationValidatorUtil.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_APACHE_MAX_CONNECTIONS,
                "Invalid value for max number of Connection. Must be positive integer.");
        ConfigurationValidatorUtil.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY,
                "Invalid value given for HTTP client max concurrency. Must be positive integer.");
        validateOptionalHttpProtocolProperty(config);
    }

    private void validateClientType(Properties config) {
        if (config.containsKey(AWSConfigConstants.HTTP_CLIENT_TYPE)
                && !allowedClientTypes.contains(
                        config.getProperty(AWSConfigConstants.HTTP_CLIENT_TYPE))) {
            throw new IllegalArgumentException("Invalid Http Client Type.");
        }
    }

    private void validateOptionalHttpProtocolProperty(Properties config) {
        if (config.containsKey(AWSConfigConstants.HTTP_PROTOCOL_VERSION)) {
            try {
                Protocol.valueOf(config.getProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid value given for HTTP protocol. Must be HTTP1_1 or HTTP2.");
            }
        }
    }
}
