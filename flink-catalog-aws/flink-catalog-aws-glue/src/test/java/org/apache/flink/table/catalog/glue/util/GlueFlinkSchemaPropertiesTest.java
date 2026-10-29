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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the column-name list encoding of {@link GlueFlinkSchemaProperties}. */
class GlueFlinkSchemaPropertiesTest {

    static Stream<List<String>> nameLists() {
        return Stream.of(
                Collections.emptyList(),
                Collections.singletonList("id"),
                Arrays.asList("id", "ts", "amount"),
                // The separator itself inside a name.
                Arrays.asList("first,second", "third"),
                Arrays.asList(",", ",,", "a,"),
                // The escape character inside a name, alone and next to a separator.
                Arrays.asList("back\\slash", "x"),
                Arrays.asList("ends\\", "starts"),
                Arrays.asList("a\\,b", "c"),
                // Empty names are not legal identifiers, but the encoding must not corrupt them.
                Arrays.asList("", "x", ""));
    }

    @ParameterizedTest
    @MethodSource("nameLists")
    void testJoinAndSplitRoundTrip(List<String> names) {
        assertThat(GlueFlinkSchemaProperties.splitNames(GlueFlinkSchemaProperties.joinNames(names)))
                .containsExactlyElementsOf(names);
    }

    /**
     * Tables written before escaping existed hold plain comma-joined names (no name then could
     * contain a comma or a backslash); the decoder must read them exactly as before.
     */
    @Test
    void testPlainCommaJoinedValuesDecodeUnchanged() {
        assertThat(GlueFlinkSchemaProperties.splitNames("id,ts,amount"))
                .containsExactly("id", "ts", "amount");
        assertThat(GlueFlinkSchemaProperties.splitNames("id")).containsExactly("id");
        assertThat(GlueFlinkSchemaProperties.joinNames(Arrays.asList("id", "ts", "amount")))
                .isEqualTo("id,ts,amount");
    }

    @Test
    void testSeparatorsInsideNamesAreEscaped() {
        assertThat(
                        GlueFlinkSchemaProperties.joinNames(
                                Arrays.asList("first,second", "back\\slash")))
                .isEqualTo("first\\,second,back\\\\slash");
    }
}
