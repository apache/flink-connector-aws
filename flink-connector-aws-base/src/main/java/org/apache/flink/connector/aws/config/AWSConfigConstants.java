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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/** Configuration keys for AWS service usage. */
@PublicEvolving
public class AWSConfigConstants {

    /**
     * Possible configuration values for the type of credential provider to use when accessing AWS.
     * Internally, a corresponding implementation of {@link AwsCredentialsProvider} will be used.
     */
    public enum CredentialProvider {

        /**
         * Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create
         * AWS credentials.
         */
        ENV_VAR,

        /**
         * Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS
         * credentials.
         */
        SYS_PROP,

        /** Use a AWS credentials profile file to create the AWS credentials. */
        PROFILE,

        /**
         * Simply create AWS credentials by supplying the AWS access key ID and AWS secret key in
         * the configuration properties.
         */
        BASIC,

        /**
         * Create AWS credentials by assuming a role. The credentials for assuming the role must be
         * supplied. *
         */
        ASSUME_ROLE,

        /**
         * Use AWS WebIdentityToken in order to assume a role. A token file and role details can be
         * supplied as configuration or environment variables. *
         */
        WEB_IDENTITY_TOKEN,

        /** Use a custom class specified by the user in connector config. */
        CUSTOM,

        /**
         * A credentials provider chain will be used that searches for credentials in this order:
         * ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE in the AWS instance metadata. *
         */
        AUTO,
    }

    /** The AWS region of the service ("us-east-1" is used if not set). */
    public static final String AWS_REGION = "aws.region";

    /**
     * The credential provider type to use when AWS credentials are required (BASIC is used if not
     * set).
     */
    public static final String AWS_CREDENTIALS_PROVIDER = "aws.credentials.provider";

    /** The AWS access key ID to use when setting credentials provider type to BASIC. */
    public static final String AWS_ACCESS_KEY_ID = accessKeyId(AWS_CREDENTIALS_PROVIDER);

    /** The AWS secret key to use when setting credentials provider type to BASIC. */
    public static final String AWS_SECRET_ACCESS_KEY = secretKey(AWS_CREDENTIALS_PROVIDER);

    /** Optional configuration for profile path if credential provider type is set to be PROFILE. */
    public static final String AWS_PROFILE_PATH = profilePath(AWS_CREDENTIALS_PROVIDER);

    /** Optional configuration for profile name if credential provider type is set to be PROFILE. */
    public static final String AWS_PROFILE_NAME = profileName(AWS_CREDENTIALS_PROVIDER);

    /**
     * The AWS endpoint for the STS (derived from the AWS region setting if not set) to use if
     * credential provider type is set to be ASSUME_ROLE.
     */
    public static final String AWS_ROLE_STS_ENDPOINT = roleStsEndpoint(AWS_CREDENTIALS_PROVIDER);

    /**
     * The full path (e.g. org.user_company.auth.CustomAwsCredentialsProvider) to the user provided
     * class to use if credential provider type is set to be CUSTOM.
     */
    public static final String CUSTOM_CREDENTIALS_PROVIDER_CLASS =
            customCredentialsProviderClass(AWS_CREDENTIALS_PROVIDER);

    /**
     * The role ARN to use when credential provider type is set to ASSUME_ROLE or
     * WEB_IDENTITY_TOKEN.
     */
    public static final String AWS_ROLE_ARN = roleArn(AWS_CREDENTIALS_PROVIDER);

    /**
     * The role session name to use when credential provider type is set to ASSUME_ROLE or
     * WEB_IDENTITY_TOKEN.
     */
    public static final String AWS_ROLE_SESSION_NAME = roleSessionName(AWS_CREDENTIALS_PROVIDER);

    /** The external ID to use when credential provider type is set to ASSUME_ROLE. */
    public static final String AWS_ROLE_EXTERNAL_ID = externalId(AWS_CREDENTIALS_PROVIDER);

    /**
     * The absolute path to the web identity token file that should be used if provider type is set
     * to WEB_IDENTITY_TOKEN.
     */
    public static final String AWS_WEB_IDENTITY_TOKEN_FILE =
            webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER);

    /**
     * The credentials provider that provides credentials for assuming the role when credential
     * provider type is set to ASSUME_ROLE. Roles can be nested, so AWS_ROLE_CREDENTIALS_PROVIDER
     * can again be set to "ASSUME_ROLE"
     */
    public static final String AWS_ROLE_CREDENTIALS_PROVIDER =
            roleCredentialsProvider(AWS_CREDENTIALS_PROVIDER);

    /** The AWS endpoint for the service (derived from the AWS region setting if not set). */
    public static final String AWS_ENDPOINT = "aws.endpoint";

    /** Whether to trust all SSL certificates. */
    public static final String TRUST_ALL_CERTIFICATES = "aws.trust.all.certificates";

    /** The HTTP protocol version to use. */
    public static final String HTTP_PROTOCOL_VERSION = "aws.http.protocol.version";

    /** Maximum request concurrency for {@link SdkAsyncHttpClient}. */
    public static final String HTTP_CLIENT_MAX_CONCURRENCY = "aws.http-client.max-concurrency";

    /** Read Request timeout for {@link SdkAsyncHttpClient}. */
    public static final String HTTP_CLIENT_READ_TIMEOUT_MILLIS = "aws.http-client.read-timeout";

    /**
     * The type of {@link software.amazon.awssdk.http.SdkHttpClient}. If set, all AWS clients will
     * use this specified HTTP client. If not set, HTTP_CLIENT_TYPE_DEFAULT will be used. For
     * specific types supported, see HTTP_CLIENT_TYPE_* defined below.
     */
    public static final String HTTP_CLIENT_TYPE = "http-client.type";

    // ---- glue configs

    /**
     * Used to configure the connection timeout in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to HTTP_CLIENT_TYPE_APACHE
     *
     * <p>For more details, see <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html">...</a>
     */
    public static final String HTTP_CLIENT_CONNECTION_TIMEOUT_MS =
            "http-client.connection-timeout-ms";

    /**
     * Used to configure the max connections number for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to HTTP_CLIENT_TYPE_APACHE
     *
     * <p>For more details, see <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html">...</a>
     */
    public static final String HTTP_CLIENT_APACHE_MAX_CONNECTIONS =
            "http-client.apache.max-connections";

    /**
     * Used to configure the socket timeout in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to HTTP_CLIENT_TYPE_APACHE
     *
     * <p>For more details, see <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html">...</a>
     */
    public static final String HTTP_CLIENT_SOCKET_TIMEOUT_MS = "http-client.socket-timeout-ms";

    public static final String CLIENT_TYPE_URLCONNECTION = "urlconnection";

    /**
     * {@link software.amazon.awssdk.http.apache.ApacheHttpClient} will be used as the HTTP Client.
     */
    public static final String CLIENT_TYPE_APACHE = "apache";

    public static String accessKeyId(String prefix) {
        return prefix + ".basic.accesskeyid";
    }

    public static String secretKey(String prefix) {
        return prefix + ".basic.secretkey";
    }

    public static String profilePath(String prefix) {
        return prefix + ".profile.path";
    }

    public static String profileName(String prefix) {
        return prefix + ".profile.name";
    }

    public static String roleArn(String prefix) {
        return prefix + ".role.arn";
    }

    public static String roleSessionName(String prefix) {
        return prefix + ".role.sessionName";
    }

    public static String externalId(String prefix) {
        return prefix + ".role.externalId";
    }

    public static String roleCredentialsProvider(String prefix) {
        return prefix + ".role.provider";
    }

    public static String roleStsEndpoint(String prefix) {
        return prefix + ".role.stsEndpoint";
    }

    public static String customCredentialsProviderClass(String prefix) {
        return prefix + ".custom.class";
    }

    public static String webIdentityTokenFile(String prefix) {
        return prefix + ".webIdentityToken.file";
    }
}
