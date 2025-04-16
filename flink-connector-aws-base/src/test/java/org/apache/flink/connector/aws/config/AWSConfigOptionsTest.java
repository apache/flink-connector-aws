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

package org.apache.flink.connector.aws.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AWSConfigOptions}. */
class AWSConfigOptionsTest {

    @Test
    void testConfigOptionKeys() {
        // Test AWS_REGION_OPTION
        assertThat(AWSConfigOptions.AWS_REGION_OPTION.key())
                .isEqualTo(AWSConfigConstants.AWS_REGION);

        // Test AWS_CREDENTIALS_PROVIDER_OPTION
        assertThat(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION.key())
                .isEqualTo(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);

        // Test AWS_ACCESS_KEY_ID_OPTION
        assertThat(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.accessKeyId(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_SECRET_ACCESS_KEY_OPTION
        assertThat(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_PROFILE_PATH_OPTION
        assertThat(AWSConfigOptions.AWS_PROFILE_PATH_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.profilePath(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_PROFILE_NAME_OPTION
        assertThat(AWSConfigOptions.AWS_PROFILE_NAME_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.profileName(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ROLE_STS_ENDPOINT_OPTION
        assertThat(AWSConfigOptions.AWS_ROLE_STS_ENDPOINT_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.roleStsEndpoint(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test CUSTOM_CREDENTIALS_PROVIDER_CLASS_OPTION
        assertThat(AWSConfigOptions.CUSTOM_CREDENTIALS_PROVIDER_CLASS_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.customCredentialsProviderClass(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ROLE_ARN_OPTION
        assertThat(AWSConfigOptions.AWS_ROLE_ARN_OPTION.key())
                .isEqualTo(AWSConfigConstants.roleArn(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ROLE_SESSION_NAME
        assertThat(AWSConfigOptions.AWS_ROLE_SESSION_NAME.key())
                .isEqualTo(
                        AWSConfigConstants.roleSessionName(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ROLE_EXTERNAL_ID_OPTION
        assertThat(AWSConfigOptions.AWS_ROLE_EXTERNAL_ID_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.externalId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_WEB_IDENTITY_TOKEN_FILE
        assertThat(AWSConfigOptions.AWS_WEB_IDENTITY_TOKEN_FILE.key())
                .isEqualTo(
                        AWSConfigConstants.webIdentityTokenFile(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ROLE_CREDENTIALS_PROVIDER_OPTION - this was the one with the bug
        assertThat(AWSConfigOptions.AWS_ROLE_CREDENTIALS_PROVIDER_OPTION.key())
                .isEqualTo(
                        AWSConfigConstants.roleCredentialsProvider(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER))
                .isNotEqualTo(
                        AWSConfigConstants.webIdentityTokenFile(
                                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));

        // Test AWS_ENDPOINT_OPTION
        assertThat(AWSConfigOptions.AWS_ENDPOINT_OPTION.key())
                .isEqualTo(AWSConfigConstants.AWS_ENDPOINT);

        // Test TRUST_ALL_CERTIFICATES_OPTION
        assertThat(AWSConfigOptions.TRUST_ALL_CERTIFICATES_OPTION.key())
                .isEqualTo(AWSConfigConstants.TRUST_ALL_CERTIFICATES);

        // Test HTTP_PROTOCOL_VERSION_OPTION
        assertThat(AWSConfigOptions.HTTP_PROTOCOL_VERSION_OPTION.key())
                .isEqualTo(AWSConfigConstants.HTTP_PROTOCOL_VERSION);

        // Test HTTP_CLIENT_MAX_CONCURRENCY_OPTION
        assertThat(AWSConfigOptions.HTTP_CLIENT_MAX_CONCURRENCY_OPTION.key())
                .isEqualTo(AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY);

        // Test HTTP_CLIENT_READ_TIMEOUT_MILLIS_OPTION
        assertThat(AWSConfigOptions.HTTP_CLIENT_READ_TIMEOUT_MILLIS_OPTION.key())
                .isEqualTo(AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS);
    }
}
