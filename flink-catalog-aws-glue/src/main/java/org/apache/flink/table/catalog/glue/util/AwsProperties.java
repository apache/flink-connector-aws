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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/** Aws properties for glue and other clients. */
public class AwsProperties {

    /**
     * The type of {@link software.amazon.awssdk.http.SdkHttpClient} implementation used by {@link
     * AwsClientFactory} If set, all AWS clients will use this specified HTTP client. If not set,
     * {@link #HTTP_CLIENT_TYPE_DEFAULT} will be used. For specific types supported, see
     * HTTP_CLIENT_TYPE_* defined below.
     */
    public static final String HTTP_CLIENT_TYPE = "http-client.type";

    private Long httpClientUrlConnectionConnectionTimeoutMs;

    private Long httpClientUrlConnectionSocketTimeoutMs;

    private Long httpClientApacheConnectionAcquisitionTimeoutMs;

    private Long httpClientApacheConnectionMaxIdleTimeMs;

    private Long httpClientApacheConnectionTimeToLiveMs;

    private Long httpClientApacheConnectionTimeoutMs;

    private Boolean httpClientApacheExpectContinueEnabled;

    private Integer httpClientApacheMaxConnections;

    private Long httpClientApacheSocketTimeoutMs;

    private Boolean httpClientApacheTcpKeepAliveEnabled;

    private Boolean httpClientApacheUseIdleConnectionReaperEnabled;

    private String glueEndpoint;

    private String glueCatalogId;

    private Boolean glueCatalogSkipArchive;

    private Boolean glueCatalogSkipNameValidation;

    /** http client. */
    private String httpClientType;

    /**
     * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
     * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} will be used as the HTTP.
     * Client in {@link AwsClientFactory}
     */
    public static final String HTTP_CLIENT_TYPE_URLCONNECTION = "urlconnection";

    /**
     * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient} will be used as the HTTP Client in
     * {@linkAwsClientFactory}.
     */
    public static final String HTTP_CLIENT_TYPE_APACHE = "apache";

    /**
     * Used to configure the connection timeout in milliseconds for {@link
     * UrlConnectionHttpClient.Builder}. This flag only works when {@link #HTTP_CLIENT_TYPE} is set
     * to {@link #HTTP_CLIENT_TYPE_URLCONNECTION}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS =
            "http-client.urlconnection.connection-timeout-ms";

    public static final String HTTP_CLIENT_TYPE_DEFAULT = HTTP_CLIENT_TYPE_URLCONNECTION;

    /**
     * The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
     * automatically uses the caller's AWS account ID by default.
     *
     * <p>For more details, see
     * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
     */
    public static final String GLUE_CATALOG_ID = "glue.id";

    /**
     * The account ID used in a Glue resource ARN, e.g.
     * arn:aws:glue:us-east-1:1000000000000:table/db1/table1
     */
    public static final String GLUE_ACCOUNT_ID = "glue.account-id";

    /**
     * If Glue should skip archiving an old table version when creating a new version in a commit.
     * By default Glue archives all old table versions after an UpdateTable call, but Glue has a
     * default max number of archived table versions (can be increased). So for streaming use case
     * with lots of commits, it is recommended to set this value to true.
     */
    public static final String GLUE_CATALOG_SKIP_ARCHIVE = "glue.skip-archive";

    public static final String GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = "false";

    /**
     * Used to configure the socket timeout in milliseconds for {@link
     * UrlConnectionHttpClient.Builder}. This flag only works when {@link #HTTP_CLIENT_TYPE} is set
     * to {@link #HTTP_CLIENT_TYPE_URLCONNECTION}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS =
            "http-client.urlconnection.socket-timeout-ms";

    /**
     * Used to configure the connection acquisition timeout in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS =
            "http-client.apache.connection-acquisition-timeout-ms";

    /**
     * Used to configure the connection max idle time in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS =
            "http-client.apache.connection-max-idle-time-ms";

    /**
     * Used to configure the connection time to live in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS =
            "http-client.apache.connection-time-to-live-ms";

    /**
     * Used to configure whether to enable the expect continue setting for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>In default, this is disabled.
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED =
            "http-client.apache.expect-continue-enabled";

    /**
     * Used to configure the max connections number for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_MAX_CONNECTIONS =
            "http-client.apache.max-connections";

    /**
     * Used to configure whether to enable the tcp keep alive setting for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}.
     *
     * <p>In default, this is disabled.
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED =
            "http-client.apache.tcp-keep-alive-enabled";

    /**
     * Used to configure the connection timeout in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS =
            "http-client.apache.connection-timeout-ms";

    /**
     * Used to configure whether to use idle connection reaper for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}.
     *
     * <p>In default, this is enabled.
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED =
            "http-client.apache.use-idle-connection-reaper-enabled";

    /**
     * Used to configure the socket timeout in milliseconds for {@link
     * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when
     * {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
     *
     * <p>For more details, see
     * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
     */
    public static final String HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS =
            "http-client.apache.socket-timeout-ms";

    /**
     * Configure an alternative endpoint of the Glue service for GlueCatalog to access.
     *
     * <p>This could be used to use GlueCatalog with any glue-compatible metastore service that has
     * a different endpoint
     */
    public static final String GLUE_CATALOG_ENDPOINT = "glue.endpoint";

    /**
     * If Glue should skip name validations It is recommended to stick to Glue best practice in
     * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html to make sure operations
     * are Hive compatible. This is only added for users that have existing conventions using
     * non-standard characters. When database name and table name validation are skipped, there is
     * no guarantee that downstream systems would all support the names.
     */
    public static final String GLUE_CATALOG_SKIP_NAME_VALIDATION = "glue.skip-name-validation";

    public static final String GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT = "false";

    public AwsProperties(Map<String, String> properties) {
        this.httpClientType = properties.getOrDefault(HTTP_CLIENT_TYPE, HTTP_CLIENT_TYPE_DEFAULT);
        this.httpClientUrlConnectionConnectionTimeoutMs =
                Long.parseLong(
                        properties.getOrDefault(
                                HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS, "0"));
        this.httpClientUrlConnectionSocketTimeoutMs =
                Long.parseLong(
                        properties.getOrDefault(HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS, "0"));

        this.httpClientApacheConnectionAcquisitionTimeoutMs =
                Long.parseLong(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS, "0"));
        this.httpClientApacheConnectionMaxIdleTimeMs =
                Long.parseLong(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS, "0"));
        this.httpClientApacheConnectionTimeToLiveMs =
                Long.parseLong(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS, "0"));
        this.httpClientApacheConnectionTimeoutMs =
                Long.parseLong(
                        properties.getOrDefault(HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, "0"));
        this.httpClientApacheExpectContinueEnabled =
                Boolean.parseBoolean(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED, "false"));
        this.httpClientApacheMaxConnections =
                Integer.parseInt(properties.getOrDefault(HTTP_CLIENT_APACHE_MAX_CONNECTIONS, "1"));
        this.httpClientApacheSocketTimeoutMs =
                Long.parseLong(properties.getOrDefault(HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, "0"));
        this.httpClientApacheTcpKeepAliveEnabled =
                Boolean.parseBoolean(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED, "false"));
        this.httpClientApacheUseIdleConnectionReaperEnabled =
                Boolean.parseBoolean(
                        properties.getOrDefault(
                                HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED, "false"));

        this.glueEndpoint = properties.get(GLUE_CATALOG_ENDPOINT);
        this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
        this.glueCatalogSkipArchive =
                Boolean.parseBoolean(
                        properties.getOrDefault(
                                GLUE_CATALOG_SKIP_ARCHIVE, GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT));
        this.glueCatalogSkipNameValidation =
                Boolean.parseBoolean(
                        properties.getOrDefault(
                                GLUE_CATALOG_SKIP_NAME_VALIDATION,
                                GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT));
    }

    //    public AwsProperties() {
    //        this.httpClientType =
    //    }

    /**
     * Configure the httpClient for a client according to the HttpClientType. The two supported
     * HttpClientTypes are urlconnection and apache
     *
     * <p>Sample usage:
     *
     * <pre>
     *     S3Client.builder().applyMutation(awsProperties::applyHttpClientConfigurations)
     * </pre>
     */
    public <T extends AwsSyncClientBuilder> void applyHttpClientConfigurations(T builder) {
        if (Strings.isNullOrEmpty(httpClientType)) {
            httpClientType = HTTP_CLIENT_TYPE_DEFAULT;
        }
        switch (httpClientType) {
            case HTTP_CLIENT_TYPE_URLCONNECTION:
                builder.httpClientBuilder(
                        UrlConnectionHttpClient.builder()
                                .applyMutation(this::configureUrlConnectionHttpClientBuilder));
                break;
            case HTTP_CLIENT_TYPE_APACHE:
                builder.httpClientBuilder(
                        ApacheHttpClient.builder()
                                .applyMutation(this::configureApacheHttpClientBuilder));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized HTTP client type " + httpClientType);
        }
    }

    @VisibleForTesting
    <T extends UrlConnectionHttpClient.Builder> void configureUrlConnectionHttpClientBuilder(
            T builder) {
        if (httpClientUrlConnectionConnectionTimeoutMs != null) {
            builder.connectionTimeout(
                    Duration.ofMillis(httpClientUrlConnectionConnectionTimeoutMs));
        }

        if (httpClientUrlConnectionSocketTimeoutMs != null) {
            builder.socketTimeout(Duration.ofMillis(httpClientUrlConnectionSocketTimeoutMs));
        }
    }

    @VisibleForTesting
    <T extends ApacheHttpClient.Builder> void configureApacheHttpClientBuilder(T builder) {
        if (httpClientApacheConnectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(httpClientApacheConnectionTimeoutMs));
        }

        if (httpClientApacheSocketTimeoutMs != null) {
            builder.socketTimeout(Duration.ofMillis(httpClientApacheSocketTimeoutMs));
        }

        if (httpClientApacheConnectionAcquisitionTimeoutMs != null) {
            builder.connectionAcquisitionTimeout(
                    Duration.ofMillis(httpClientApacheConnectionAcquisitionTimeoutMs));
        }

        if (httpClientApacheConnectionMaxIdleTimeMs != null) {
            builder.connectionMaxIdleTime(
                    Duration.ofMillis(httpClientApacheConnectionMaxIdleTimeMs));
        }

        if (httpClientApacheConnectionTimeToLiveMs != null) {
            builder.connectionTimeToLive(Duration.ofMillis(httpClientApacheConnectionTimeToLiveMs));
        }

        if (httpClientApacheExpectContinueEnabled != null) {
            builder.expectContinueEnabled(httpClientApacheExpectContinueEnabled);
        }

        if (httpClientApacheMaxConnections != null) {
            builder.maxConnections(httpClientApacheMaxConnections);
        }

        if (httpClientApacheTcpKeepAliveEnabled != null) {
            builder.tcpKeepAlive(httpClientApacheTcpKeepAliveEnabled);
        }

        if (httpClientApacheUseIdleConnectionReaperEnabled != null) {
            builder.useIdleConnectionReaper(httpClientApacheUseIdleConnectionReaperEnabled);
        }
    }

    /**
     * Override the endpoint for a glue client.
     *
     * <p>Sample usage:
     *
     * <pre>
     *     GlueClient.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
     * </pre>
     */
    public <T extends GlueClientBuilder> void applyGlueEndpointConfigurations(T builder) {
        configureEndpoint(builder, glueEndpoint);
    }

    private <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint));
        }
    }

    /*
     * Getter for glue catalogId.
     */
    public String getGlueCatalogId() {
        return glueCatalogId;
    }
}

