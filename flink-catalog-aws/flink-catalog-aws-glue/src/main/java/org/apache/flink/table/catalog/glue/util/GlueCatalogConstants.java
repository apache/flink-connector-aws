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

import org.apache.flink.annotation.Internal;

/** Constants used throughout the Glue catalog implementation. */
@Internal
public final class GlueCatalogConstants {

    private GlueCatalogConstants() {}

    /**
     * Partition property carrying the partition's storage location (the Glue storage descriptor
     * location), mirroring HiveCatalog's {@code hive.location-uri}. Exposed by {@code getPartition}
     * and consumed by {@code createPartition}/{@code alterPartition}; when absent on create, the
     * Hive-style default {@code [table location]/key=value/...} is used. A location that spells a
     * different partition of the same table is rejected, so copying one partition's properties into
     * another never points it at the first partition's data.
     */
    public static final String PARTITION_LOCATION = "location";

    // ---- Case preservation ------------------------------------------------------------------
    // AWS Glue stores database, table and column names in lowercase. The declared names are kept
    // in these Glue parameters so Flink can present identifiers exactly as they were declared.

    /** Database parameter carrying the declared database name. */
    public static final String ORIGINAL_DATABASE_NAME = "flink.original-database-name";

    /** Table parameter carrying the declared table name. */
    public static final String ORIGINAL_TABLE_NAME = "flink.original-table-name";

    /** Column parameter carrying the declared column name. */
    public static final String ORIGINAL_COLUMN_NAME = "flink.original-column-name";

    /**
     * Column parameter used for the declared column name by earlier builds of this connector; still
     * honoured on read so tables they created keep their declared column names.
     */
    public static final String LEGACY_ORIGINAL_COLUMN_NAME = "originalName";

    /**
     * Table-level parameter carrying the comma-separated, order-preserving declared names of the
     * partition keys. Needed because AWS Glue rejects column-level parameters on partition columns
     * ("Parameters not supported for partition columns"), so {@link #ORIGINAL_COLUMN_NAME} cannot
     * be used for them.
     */
    public static final String ORIGINAL_PARTITION_KEYS = "flink.original-partition-keys";

    // ---- Functions ----------------------------------------------------------------------------

    /** Prefix of the Glue function class name for Scala functions. */
    public static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";

    /** Prefix of the Glue function class name for Python functions. */
    public static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";

    /** Prefix of the Glue function class name for Java functions. */
    public static final String FLINK_JAVA_FUNCTION_PREFIX = "flink:java:";

    /** Owner name recorded on Glue functions created by this catalog. */
    public static final String FLINK_CATALOG = "FLINK_CATALOG";

    // ---- Client -------------------------------------------------------------------------------

    /** Format of the user agent prefix sent with every Glue request. */
    public static final String BASE_GLUE_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) Glue Catalog";

    /** Client configuration key for the user agent prefix. */
    public static final String GLUE_CLIENT_USER_AGENT_PREFIX = "aws.glue.client.user-agent-prefix";
}
