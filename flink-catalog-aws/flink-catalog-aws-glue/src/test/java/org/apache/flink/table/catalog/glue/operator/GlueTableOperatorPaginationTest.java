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

package org.apache.flink.table.catalog.glue.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the table listing and lookup operations handle paginated Glue responses. The {@link
 * FakeGlueClient} pages its list responses at {@link FakeGlueClient#PAGE_SIZE} entries, so listing
 * more tables than one page proves the nextToken loop is exercised.
 */
class GlueTableOperatorPaginationTest {

    private static final String DATABASE_NAME = "paginationdb";
    private static final int TABLE_COUNT = FakeGlueClient.PAGE_SIZE * 2 + 20;

    private FakeGlueClient fakeGlueClient;
    private GlueTableOperator glueTableOperator;

    @BeforeEach
    void setUp() throws Exception {
        FakeGlueClient.reset();
        fakeGlueClient = new FakeGlueClient();
        glueTableOperator = new GlueTableOperator(fakeGlueClient, "test_catalog");
        fakeGlueClient.createDatabase(
                builder -> builder.databaseInput(db -> db.name(DATABASE_NAME)));
        for (int i = 0; i < TABLE_COUNT; i++) {
            String tableName = String.format("table_%03d", i);
            glueTableOperator.createTable(
                    DATABASE_NAME,
                    TableInput.builder()
                            .name(tableName)
                            .tableType("TABLE")
                            .storageDescriptor(
                                    StorageDescriptor.builder()
                                            .columns(
                                                    Column.builder()
                                                            .name("id")
                                                            .type("string")
                                                            .build())
                                            .build())
                            .build());
        }
    }

    @AfterEach
    void tearDown() {
        FakeGlueClient.reset();
    }

    @Test
    void testListTablesReturnsAllPages() {
        assertThat(glueTableOperator.listTables(DATABASE_NAME))
                .hasSize(TABLE_COUNT)
                .containsAll(
                        IntStream.range(0, TABLE_COUNT)
                                .mapToObj(i -> String.format("table_%03d", i))
                                .collect(Collectors.toList()));
    }

    @Test
    void testGetAllGlueTablesReturnsAllPages() {
        assertThat(glueTableOperator.getAllGlueTables(DATABASE_NAME)).hasSize(TABLE_COUNT);
    }

    @Test
    void testListTablesReturnsDeclaredNames() {
        // listTables returns the declared (case-preserved) names for every page.
        assertThat(glueTableOperator.listTables(DATABASE_NAME))
                .hasSize(TABLE_COUNT)
                .allMatch(name -> name.startsWith("table_"));
    }

    @Test
    void testGetGlueTableOrNullResolvesTablesBeyondFirstPage() {
        // Resolution is a single GetTable on the lowercase name, so a table sitting on the
        // last page is found without listing anything.
        String lastTable = String.format("table_%03d", TABLE_COUNT - 1);
        assertThat(glueTableOperator.getGlueTableOrNull(DATABASE_NAME, lastTable))
                .isNotNull()
                .extracting(Table::name)
                .isEqualTo(lastTable);
        assertThat(glueTableOperator.getGlueTableOrNull(DATABASE_NAME, lastTable.toUpperCase()))
                .isNotNull();
        assertThat(glueTableOperator.getGlueTableOrNull(DATABASE_NAME, "missing_table")).isNull();
    }
}
