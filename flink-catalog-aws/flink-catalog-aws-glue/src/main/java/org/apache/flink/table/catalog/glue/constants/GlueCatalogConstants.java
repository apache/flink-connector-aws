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

package org.apache.flink.table.catalog.glue.constants;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.glue.GlueCatalog;

import java.util.regex.Pattern;

/** Constants and Defined Values used for {@link GlueCatalog}. */
@Internal
public class GlueCatalogConstants {
    public static final String COMMENT = "comment";
    public static final String DEFAULT_SEPARATOR = ":";
    public static final String LOCATION_SEPARATOR = "/";
    public static final String LOCATION_URI = "path";
    public static final String AND = "and";
    public static final String NEXT_LINE = "\n";
    public static final String SPACE = " ";

    public static final String TABLE_OWNER = "owner";
    public static final String TABLE_INPUT_FORMAT = "table.input.format";
    public static final String TABLE_OUTPUT_FORMAT = "table.output.format";

    public static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";
    public static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";
    public static final String FLINK_JAVA_FUNCTION_PREFIX = "flink:java:";

    public static final String FLINK_CATALOG = "FLINK_CATALOG";

    public static final Pattern GLUE_DB_PATTERN = Pattern.compile("^[a-z0-9_]{1,255}$");
    public static final String GLUE_EXCEPTION_MSG_IDENTIFIER = "GLUE EXCEPTION";
    public static final String TABLE_NOT_EXISTS_IDENTIFIER = "TABLE DOESN'T EXIST";
    public static final String DEFAULT_PARTITION_NAME = "__GLUE_DEFAULT_PARTITION__";

    public static final int UDF_CLASS_NAME_SIZE = 3;

    public static final String BASE_GLUE_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) Glue Catalog";

    /** Glue Catalog identifier for user agent prefix. */
    public static final String GLUE_CLIENT_USER_AGENT_PREFIX = "aws.glue.client.user-agent-prefix";

    public static final String IS_PERSISTED = "isPersisted";
    public static final String EXPLAIN_EXTRAS = "explainExtras";
    public static final String IS_PHYSICAL = "isPhysical";
}
