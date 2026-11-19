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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Keeps the {@code avro-glue} documentation page honest.
 *
 * <p>Every {@code CREATE TABLE} example on the page is executed against a real {@link
 * TableEnvironment} with the connector it names, and then planned as a source or a sink, so a
 * documented option the factory does not declare, or a documented statement the planner rejects,
 * fails this build instead of the reader's first {@code SHOW CREATE TABLE}. The "Format Options"
 * table is compared, key by key, against the factory's declared options, required flags, forwarded
 * flags and defaults.
 *
 * <p>The English and Chinese pages are both checked. The test is skipped when the docs tree cannot
 * be located (for example when the module is built from a source jar).
 */
class GlueSchemaRegistryAvroFormatDocsTest {

    private static final String PAGE = "docs/connectors/table/formats/avro-glue.md";
    private static final String FORMAT = GlueSchemaRegistryAvroFormatFactory.IDENTIFIER;

    private static final Pattern SQL_BLOCK = Pattern.compile("```sql\\s*(.*?)```", Pattern.DOTALL);
    private static final Pattern CREATE_TABLE =
            Pattern.compile("CREATE\\s+TABLE\\s+`?(\\w+)`?", Pattern.CASE_INSENSITIVE);
    private static final Pattern OPTION_PAIR = Pattern.compile("'([^']+)'\\s*=\\s*'([^']*)'");
    private static final Pattern OPTION_ROW =
            Pattern.compile(
                    "<tr>\\s*<td><h5>([^<]+)</h5></td>\\s*<td>(required|optional)</td>\\s*"
                            + "<td>(yes|no)</td>\\s*<td[^>]*>([^<]*)</td>\\s*<td>([^<]*)</td>",
                    Pattern.DOTALL);

    private static String previousGlueEndpoint;

    @BeforeAll
    static void isolateFromTheNetwork() {
        // The fetchFromRegistry example builds a real GlueClient at planning time. Point it at a
        // closed loopback port so the (swallowed) lookup fails immediately instead of walking the
        // default credential chain and the network.
        previousGlueEndpoint = System.setProperty("aws.endpointUrlGlue", "http://127.0.0.1:1");
    }

    @AfterAll
    static void restoreSystemProperties() {
        if (previousGlueEndpoint == null) {
            System.clearProperty("aws.endpointUrlGlue");
        } else {
            System.setProperty("aws.endpointUrlGlue", previousGlueEndpoint);
        }
    }

    static Stream<Path> docPages() {
        return locateDocPages().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("docPages")
    void documentedCreateTableExamplesArePlannable(Path page) throws IOException {
        List<String> statements = createTableStatements(page);
        assertThat(statements).as("CREATE TABLE examples on %s", page).isNotEmpty();

        for (String ddl : statements) {
            Map<String, String> options = options(ddl);
            assertThat(options)
                    .as("every example on the page must use this format:%n%s", ddl)
                    .containsEntry("format", FORMAT);
            String connector = options.get("connector");
            assertThat(connector).as("connector option in%n%s", ddl).isNotNull();

            TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            tEnv.executeSql(ddl);

            String table = tableName(ddl);
            boolean isSource = factoryExists(DynamicTableSourceFactory.class, connector);
            boolean isSink = factoryExists(DynamicTableSinkFactory.class, connector);
            boolean documentedAsSink = table.toLowerCase().endsWith("sink");
            assertThat(isSource || isSink).as("connector '%s' is unknown", connector).isTrue();

            if (isSource && !documentedAsSink) {
                tEnv.explainSql("SELECT * FROM `" + table + "`");
            }
            if (isSink && (documentedAsSink || !isSource)) {
                tEnv.explainSql("INSERT INTO `" + table + "` " + nullRowFor(tEnv, table));
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("docPages")
    void documentedFormatOptionsMatchTheFactory(Path page) throws IOException {
        String text = new String(Files.readAllBytes(page), StandardCharsets.UTF_8);
        GlueSchemaRegistryAvroFormatFactory factory = new GlueSchemaRegistryAvroFormatFactory();
        Map<String, ConfigOption<?>> declared = new LinkedHashMap<>();
        factory.requiredOptions().forEach(o -> declared.put(FORMAT + "." + o.key(), o));
        factory.optionalOptions().forEach(o -> declared.put(FORMAT + "." + o.key(), o));
        Set<String> forwarded =
                factory.forwardOptions().stream()
                        .map(o -> FORMAT + "." + o.key())
                        .collect(Collectors.toSet());

        Map<String, String[]> documented = new LinkedHashMap<>();
        Matcher m = OPTION_ROW.matcher(text);
        while (m.find()) {
            if (!"format".equals(m.group(1))) {
                documented.put(
                        m.group(1), new String[] {m.group(2), m.group(3), m.group(4).trim()});
            }
        }

        assertThat(documented.keySet())
                .as("options documented on %s vs options the factory declares", page)
                .containsExactlyInAnyOrderElementsOf(declared.keySet());

        for (Map.Entry<String, String[]> e : documented.entrySet()) {
            ConfigOption<?> option = declared.get(e.getKey());
            boolean required = factory.requiredOptions().contains(option);
            assertThat(e.getValue()[0])
                    .as("required flag of %s", e.getKey())
                    .isEqualTo(required ? "required" : "optional");
            assertThat(e.getValue()[1])
                    .as("forwarded flag of %s", e.getKey())
                    .isEqualTo(forwarded.contains(e.getKey()) ? "yes" : "no");
            String expectedDefault =
                    option.hasDefaultValue() ? String.valueOf(option.defaultValue()) : "(none)";
            assertThat(e.getValue()[2])
                    .as("documented default of %s", e.getKey())
                    .isEqualTo(expectedDefault);
        }
    }

    @Test
    void documentationPagesAreFound() {
        assumeThat(locateDocPages())
                .as("docs tree not found; skipping documentation checks")
                .isNotEmpty();
        assertThat(locateDocPages()).hasSize(2);
    }

    // ---------------------------------------------------------------------------------------------

    private static List<Path> locateDocPages() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path docsRoot = dir.resolve("docs");
            if (Files.isDirectory(docsRoot)) {
                List<Path> pages = new ArrayList<>();
                for (String content : new String[] {"content", "content.zh"}) {
                    Path page = docsRoot.resolve(content).resolve(PAGE);
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

    private static List<String> createTableStatements(Path page) throws IOException {
        String text = new String(Files.readAllBytes(page), StandardCharsets.UTF_8);
        List<String> statements = new ArrayList<>();
        Matcher blocks = SQL_BLOCK.matcher(text);
        while (blocks.find()) {
            String sql = blocks.group(1).trim();
            if (CREATE_TABLE.matcher(sql).find()) {
                statements.add(sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql);
            }
        }
        return statements;
    }

    private static String tableName(String ddl) {
        Matcher m = CREATE_TABLE.matcher(ddl);
        assertThat(m.find()).isTrue();
        return m.group(1);
    }

    private static Map<String, String> options(String ddl) {
        Map<String, String> options = new LinkedHashMap<>();
        Matcher m = OPTION_PAIR.matcher(ddl);
        while (m.find()) {
            options.put(m.group(1), m.group(2));
        }
        return options;
    }

    private static boolean factoryExists(Class<? extends Factory> type, String identifier) {
        try {
            FactoryUtil.discoverFactory(
                    GlueSchemaRegistryAvroFormatDocsTest.class.getClassLoader(), type, identifier);
            return true;
        } catch (ValidationException e) {
            return false;
        }
    }

    /** A {@code SELECT} producing one all-NULL row shaped exactly like the table. */
    private static String nullRowFor(TableEnvironment tEnv, String table) {
        List<Column> columns = tEnv.from("`" + table + "`").getResolvedSchema().getColumns();
        String select =
                columns.stream()
                        .map(c -> "CAST(NULL AS " + c.getDataType() + ") AS `" + c.getName() + "`")
                        .collect(Collectors.joining(", "));
        return "SELECT " + select + " FROM (VALUES (1)) AS T(x)";
    }
}
