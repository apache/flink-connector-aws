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

package org.apache.flink.connector.aws.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.accessKeyId;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.customCredentialsProviderClass;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.externalId;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.profileName;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.profilePath;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.roleArn;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.roleSessionName;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.roleStsEndpoint;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.secretKey;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.webIdentityTokenFile;

/** Configuration options for AWS service usage. */
@PublicEvolving
public class AWSConfigOptions {
    public static final ConfigOption<String> AWS_REGION_OPTION =
            ConfigOptions.key(AWSConfigConstants.AWS_REGION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS region of the service (\"us-east-1\" is used if not set).");

    public static final ConfigOption<AWSConfigConstants.CredentialProvider>
            AWS_CREDENTIALS_PROVIDER_OPTION =
                    ConfigOptions.key(AWS_CREDENTIALS_PROVIDER)
                            .enumType(AWSConfigConstants.CredentialProvider.class)
                            .defaultValue(AWSConfigConstants.CredentialProvider.BASIC)
                            .withDescription(
                                    "The credential provider type to use when AWS credentials are required (BASIC is used if not set");

    public static final ConfigOption<String> AWS_ACCESS_KEY_ID_OPTION =
            ConfigOptions.key(accessKeyId(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS access key ID to use when setting credentials provider type to BASIC.");

    public static final ConfigOption<String> AWS_SECRET_ACCESS_KEY_OPTION =
            ConfigOptions.key(secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS secret key to use when setting credentials provider type to BASIC.");

    public static final ConfigOption<String> AWS_PROFILE_PATH_OPTION =
            ConfigOptions.key(profilePath(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional configuration for profile path if credential provider type is set to be PROFILE.");

    public static final ConfigOption<String> AWS_PROFILE_NAME_OPTION =
            ConfigOptions.key(profileName(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional configuration for profile name if credential provider type is set to be PROFILE.");

    public static final ConfigOption<String> AWS_ROLE_STS_ENDPOINT_OPTION =
            ConfigOptions.key(roleStsEndpoint(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS endpoint for the STS (derived from the AWS region setting if not set) "
                                    + "to use if credential provider type is set to be ASSUME_ROLE.");

    public static final ConfigOption<String> CUSTOM_CREDENTIALS_PROVIDER_CLASS_OPTION =
            ConfigOptions.key(customCredentialsProviderClass(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The full path (e.g. org.user_company.auth.CustomAwsCredentialsProvider) to the user provided"
                                    + "class to use if credential provider type is set to be CUSTOM.");

    public static final ConfigOption<String> AWS_ROLE_ARN_OPTION =
            ConfigOptions.key(roleArn(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The role ARN to use when credential provider type is set to ASSUME_ROLE or"
                                    + "WEB_IDENTITY_TOKEN");

    public static final ConfigOption<String> AWS_ROLE_SESSION_NAME =
            ConfigOptions.key(roleSessionName(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The role session name to use when credential provider type is set to ASSUME_ROLE or"
                                    + "WEB_IDENTITY_TOKEN");

    public static final ConfigOption<String> AWS_ROLE_EXTERNAL_ID_OPTION =
            ConfigOptions.key(externalId(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The external ID to use when credential provider type is set to ASSUME_ROLE.");

    public static final ConfigOption<String> AWS_WEB_IDENTITY_TOKEN_FILE =
            ConfigOptions.key(webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The absolute path to the web identity token file that should be used if provider"
                                    + " type is set to WEB_IDENTITY_TOKEN.");

    public static final ConfigOption<String> AWS_ROLE_CREDENTIALS_PROVIDER_OPTION =
            ConfigOptions.key(webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The credentials provider that provides credentials for assuming the role when"
                                    + " credential provider type is set to ASSUME_ROLE. Roles can be nested, so"
                                    + " AWS_ROLE_CREDENTIALS_PROVIDER can again be set to ASSUME_ROLE");

    public static final ConfigOption<String> AWS_ENDPOINT_OPTION =
            ConfigOptions.key(AWSConfigConstants.AWS_ENDPOINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS endpoint for the service (derived from the AWS region setting if not set).");

    public static final ConfigOption<String> TRUST_ALL_CERTIFICATES_OPTION =
            ConfigOptions.key(AWSConfigConstants.TRUST_ALL_CERTIFICATES)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Whether to trust all SSL certificates.");

    public static final ConfigOption<String> HTTP_PROTOCOL_VERSION_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_PROTOCOL_VERSION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HTTP protocol version to use.");

    public static final ConfigOption<String> HTTP_CLIENT_MAX_CONCURRENCY_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Maximum request concurrency for SdkAsyncHttpClient.");

    public static final ConfigOption<String> HTTP_CLIENT_READ_TIMEOUT_MILLIS_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Read Request timeout for SdkAsyncHttpClient.");

    public static final ConfigOption<Duration> RETRY_STRATEGY_MIN_DELAY_OPTION =
            ConfigOptions.key("retry-strategy.delay.min")
                    .durationType()
                    .defaultValue(Duration.ofMillis(300))
                    .withDescription("Base delay for the exponential backoff retry strategy");

    public static final ConfigOption<Duration> RETRY_STRATEGY_MAX_DELAY_OPTION =
            ConfigOptions.key("retry-strategy.delay.max")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription("Max delay for the exponential backoff retry strategy");

    public static final ConfigOption<Integer> RETRY_STRATEGY_MAX_ATTEMPTS_OPTION =
            ConfigOptions.key("retry-strategy.attempts.max")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Maximum number of attempts for the exponential backoff retry strategy");
}
