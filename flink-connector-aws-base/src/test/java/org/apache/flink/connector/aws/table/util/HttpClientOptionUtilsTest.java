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

package org.apache.flink.connector.aws.table.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class HttpClientOptionUtilsTest {

    private static final String[] ALLOWED_GLUE_HTTP_CLIENTS =
            new String[] {
                AWSConfigConstants.CLIENT_TYPE_URLCONNECTION, AWSConfigConstants.CLIENT_TYPE_APACHE
            };

    @Test
    public void testGoodHttpClientOptionsMapping() {
        HttpClientOptionUtils httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, getDefaultClientOptions());

        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                httpClientOptionUtils.getProcessedResolvedOptions();

        Assertions.assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    void testHttpClientOptionsUtilsFilteringNonPrefixedOptions() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("aws.not.http-client.dummy.option", "someValue");

        HttpClientOptionUtils httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, defaultClientOptions);

        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                httpClientOptionUtils.getProcessedResolvedOptions();

        Assertions.assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    void testHttpClientOptionsUtilsExtractingCorrectConfiguration() {
        HttpClientOptionUtils httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, getDefaultClientOptions());

        Properties expectedConfigurations = getDefaultExpectedClientConfigs();
        Properties actualConfigurations = httpClientOptionUtils.getValidatedConfigurations();

        Assertions.assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    void testHttpClientOptionsUtilsFailOnInvalidMaxConcurrency() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("http-client.max-concurrency", "invalid-integer");

        HttpClientOptionUtils httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, defaultClientOptions);

        Assertions.assertThrows(
                IllegalArgumentException.class, httpClientOptionUtils::getValidatedConfigurations);
    }

    @Test
    void testHttpClientOptionsUtilsFailOnInvalidHttpProtocol() {
        Map<String, String> defaultProperties = getDefaultClientOptions();
        defaultProperties.put("http-client.protocol.version", "invalid-http-protocol");

        HttpClientOptionUtils httpClientOptionUtils =
                new HttpClientOptionUtils(ALLOWED_GLUE_HTTP_CLIENTS, defaultProperties);

        Assertions.assertThrows(
                IllegalArgumentException.class, httpClientOptionUtils::getValidatedConfigurations);
    }

    private static Map<String, String> getDefaultClientOptions() {
        Map<String, String> defaultGlueClientOptions = new HashMap<String, String>();
        defaultGlueClientOptions.put("region", "us-east-1");
        defaultGlueClientOptions.put("http-client.max-concurrency", "10000");
        defaultGlueClientOptions.put("http-client.protocol.version", "HTTP2");
        return defaultGlueClientOptions;
    }

    private static Map<String, String> getDefaultExpectedClientOptions() {
        Map<String, String> defaultExpectedGlueClientConfigurations = new HashMap<String, String>();
        defaultExpectedGlueClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, "10000");
        defaultExpectedGlueClientConfigurations.put(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP2");
        return defaultExpectedGlueClientConfigurations;
    }

    private static Properties getDefaultExpectedClientConfigs() {
        Properties defaultExpectedGlueClientConfigurations = new Properties();
        defaultExpectedGlueClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, "10000");
        defaultExpectedGlueClientConfigurations.put(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP2");
        return defaultExpectedGlueClientConfigurations;
    }
}
