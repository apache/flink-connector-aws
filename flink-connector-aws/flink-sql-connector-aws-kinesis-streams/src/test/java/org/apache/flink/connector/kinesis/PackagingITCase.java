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

package org.apache.flink.connector.kinesis;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class PackagingITCase {

    @Test
    void testPackaging() throws Exception {
        final Path jar =
                ResourceTestUtils.getResource(
                        ".*/flink-sql-connector-aws-kinesis-streams[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar,
                Arrays.asList(
                        "org/apache/flink/",
                        "org/apache/commons/",
                        "META-INF/",
                        "mozilla/",
                        "mime.types",
                        "VersionInfo.java"));
        PackagingTestUtils.assertJarContainsServiceEntry(jar, Factory.class);
    }

    @Test
    void testNotice() throws Exception {
        final Path jarPath =
                ResourceTestUtils.getResource(
                        ".*/flink-sql-connector-aws-kinesis-streams[^/]*\\.jar");

        final String notice =
                IOUtils.toString(
                        new URL(String.format("jar:file:%s!/META-INF/NOTICE", jarPath))
                                .openStream(),
                        StandardCharsets.UTF_8);

        final String dependencies =
                IOUtils.toString(
                        new URL(String.format("jar:file:%s!/META-INF/DEPENDENCIES", jarPath))
                                .openStream(),
                        StandardCharsets.UTF_8);

        Pattern pattern =
                Pattern.compile(
                        "\\b[a-zA-Z0-9_.-]+:[a-zA-Z0-9_.-]+:[a-zA-Z0-9_.-]+:[a-zA-Z0-9_.-]+\\b");
        Matcher matcher = pattern.matcher(dependencies);

        List<String> packageMissingFromNotice = new ArrayList<>();
        while (matcher.find()) {
            String packageId = matcher.group().replace(":jar:", ":").replace(":bundle:", ":");
            if (!notice.contains(packageId)) {
                packageMissingFromNotice.add(packageId);
            }
        }

        assertThat(packageMissingFromNotice)
                .withFailMessage(
                        "The following packages were missing from META-INF/NOTICE: %s",
                        packageMissingFromNotice)
                .isEmpty();
    }
}
