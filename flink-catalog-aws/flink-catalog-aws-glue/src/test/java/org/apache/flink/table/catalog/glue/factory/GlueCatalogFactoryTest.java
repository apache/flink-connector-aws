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

package org.apache.flink.table.catalog.glue.factory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.glue.GlueCatalog;
import org.apache.flink.table.factories.CatalogFactory;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Unit tests for {@link GlueCatalogFactory}. */
class GlueCatalogFactoryTest {

    @Test
    void testFactoryIdentifier() {
        assertThat(new GlueCatalogFactory().factoryIdentifier()).isEqualTo("glue");
    }

    @Test
    void testCreateCatalogWithMinimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(catalog).isInstanceOf(GlueCatalog.class);
        assertThat(((GlueCatalog) catalog).getName()).isEqualTo("my_glue");
        assertThat(((GlueCatalog) catalog).getDefaultDatabase()).isEqualTo("default");
    }

    @Test
    void testCreateCatalogWithDefaultDatabase() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("default-database", "analytics");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(((GlueCatalog) catalog).getDefaultDatabase()).isEqualTo("analytics");
    }

    @Test
    void testMissingRegionIsRejected() {
        assertThatThrownBy(() -> createCatalog("my_glue", new HashMap<>()))
                .hasMessageContaining("region");
    }

    @Test
    void testUnknownOptionIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("unknown-option", "value");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .hasMessageContaining("unknown-option");
    }

    /**
     * Every {@code CREATE CATALOG ... WITH (...)} example in the documentation must be accepted by
     * the factory. The factory validates options strictly, so a stray key in a documented example
     * fails for users the moment they paste it into a SQL client.
     */
    @Test
    void testDocumentedCreateCatalogExamplesAreAcceptedByTheFactory() throws IOException {
        List<Path> docs = documentationPages();
        assumeThat(docs).as("documentation pages found relative to the module").isNotEmpty();

        int examples = 0;
        for (Path doc : docs) {
            for (Map<String, String> options : documentedCatalogOptions(doc)) {
                examples++;
                assertThat(options.remove("type")).as("%s: 'type' option", doc).isEqualTo("glue");
                assertThat(createCatalog("glue_catalog", options))
                        .as("%s: %s", doc, options)
                        .isInstanceOf(GlueCatalog.class);
            }
        }
        assertThat(examples).as("CREATE CATALOG examples found in the docs").isGreaterThan(0);
    }

    private static List<Path> documentationPages() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path docsRoot = dir.resolve("docs");
            if (Files.isDirectory(docsRoot)) {
                List<Path> pages = new ArrayList<>();
                for (String lang : new String[] {"content", "content.zh"}) {
                    Path page = docsRoot.resolve(lang + "/docs/connectors/table/glue.md");
                    if (Files.exists(page)) {
                        pages.add(page);
                    }
                }
                return pages;
            }
            dir = dir.getParent();
        }
        return new ArrayList<>();
    }

    private static final Pattern CREATE_CATALOG_BLOCK =
            Pattern.compile("CREATE CATALOG\\s+\\w+\\s+WITH\\s*\\((.*?)\\)\\s*;", Pattern.DOTALL);
    private static final Pattern OPTION_PAIR = Pattern.compile("'([^']+)'\\s*=\\s*'([^']*)'");

    private static List<Map<String, String>> documentedCatalogOptions(Path page)
            throws IOException {
        String text = new String(Files.readAllBytes(page), StandardCharsets.UTF_8);
        List<Map<String, String>> result = new ArrayList<>();
        Matcher block = CREATE_CATALOG_BLOCK.matcher(text);
        while (block.find()) {
            Map<String, String> options = new HashMap<>();
            Matcher pair = OPTION_PAIR.matcher(block.group(1));
            while (pair.find()) {
                options.put(pair.group(1), pair.group(2));
            }
            result.add(options);
        }
        return result;
    }

    @Test
    void testAwsAndHttpClientOptionsArePassedThrough() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("aws.endpoint", "http://localhost:5000");
        options.put("aws.credentials.provider", "BASIC");
        options.put("aws.credentials.basic.accesskeyid", "testAccessKey");
        options.put("aws.credentials.basic.secretkey", "testSecret");
        options.put("http-client.connection-timeout-ms", "5000");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(catalog).isInstanceOf(GlueCatalog.class);
    }

    @Test
    void testInvalidCredentialConfigurationIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        // BASIC requires access key id and secret key to be configured.
        options.put("aws.credentials.provider", "BASIC");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidHttpClientOptionIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("http-client.connection-timeout-ms", "not-a-number");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Proves the {@code aws.*} options are not merely accepted but reach the Glue client: the
     * catalog must send its requests to the configured endpoint, signed with the configured static
     * credentials. A loopback HTTP stub stands in for Glue and answers {@code GetDatabases}.
     */
    @Test
    void testAwsOptionsReachTheGlueClient() throws Exception {
        HttpServer glueStub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        List<String> receivedTargets = new ArrayList<>();
        List<String> receivedAuthorizations = new ArrayList<>();
        glueStub.createContext(
                "/",
                exchange -> {
                    receivedTargets.add(exchange.getRequestHeaders().getFirst("X-Amz-Target"));
                    receivedAuthorizations.add(
                            exchange.getRequestHeaders().getFirst("Authorization"));
                    byte[] body =
                            "{\"DatabaseList\":[{\"Name\":\"from_stub\"}]}"
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.1");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                    exchange.close();
                });
        glueStub.start();
        try {
            Map<String, String> options = new HashMap<>();
            options.put("region", "us-east-1");
            options.put("aws.endpoint", "http://127.0.0.1:" + glueStub.getAddress().getPort());
            options.put("aws.credentials.provider", "BASIC");
            options.put("aws.credentials.basic.accesskeyid", "AKIASTUBACCESSKEY");
            options.put("aws.credentials.basic.secretkey", "stubSecret");
            options.put("http-client.connection-timeout-ms", "2000");
            options.put("http-client.socket-timeout-ms", "2000");
            options.put("http-client.apache.max-connections", "2");

            Catalog catalog = createCatalog("my_glue", options);
            try {
                catalog.open();
                assertThat(catalog.listDatabases()).containsExactly("from_stub");
            } finally {
                catalog.close();
            }

            assertThat(receivedTargets).containsExactly("AWSGlue.GetDatabases");
            assertThat(receivedAuthorizations)
                    .singleElement()
                    .asString()
                    .startsWith("AWS4-HMAC-SHA256 Credential=AKIASTUBACCESSKEY/")
                    .contains("/us-east-1/glue/aws4_request");
        } finally {
            glueStub.stop(0);
        }
    }

    @Test
    void testUnsupportedHttpClientTypeIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("http-client.type", "urlconnection");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("http-client.type")
                .hasMessageContaining("apache");
    }

    @Test
    void testHttpClientTypeIsCaseInsensitive() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("http-client.type", "APACHE");

        assertThat(createCatalog("my_glue", options)).isInstanceOf(GlueCatalog.class);
    }

    private static Catalog createCatalog(String name, Map<String, String> options) {
        return new GlueCatalogFactory()
                .createCatalog(
                        new CatalogFactory.Context() {
                            @Override
                            public String getName() {
                                return name;
                            }

                            @Override
                            public Map<String, String> getOptions() {
                                return options;
                            }

                            @Override
                            public Configuration getConfiguration() {
                                return new Configuration();
                            }

                            @Override
                            public ClassLoader getClassLoader() {
                                return GlueCatalogFactoryTest.class.getClassLoader();
                            }
                        });
    }
}
