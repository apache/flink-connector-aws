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

package org.apache.flink.connector.cloudwatch.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Constants used for testing. */
public class TestConstants {
    public static final String TEST_REGION = "us-east-1";

    public static final String TEST_NAMESPACE = "test_namespace";
    public static final String TEST_METRIC_NAME = "test_metric_name";
    public static final String TEST_DIMENSION_KEY_1 = "test_dim_key_1";
    public static final String TEST_DIMENSION_KEY_2 = "test_dim_key_2";
    public static final String TEST_DIMENSION_KEYS =
            String.format("[%s,%s]", TEST_DIMENSION_KEY_1, TEST_DIMENSION_KEY_2);
    public static final String TEST_DIMENSION_VALUE_1 = "test_dim_val_1";
    public static final String TEST_DIMENSION_VALUE_2 = "test_dim_val_2";
    public static final String TEST_VALUE_KEY = "test_value_key";
    public static final String TEST_COUNT_KEY = "test_count_key";
    public static final double TEST_SAMPLE_VALUE = 123d;
    public static final double TEST_SAMPLE_COUNT = 12d;
    public static final String TEST_TIMESTAMP_KEY = "test_sample_ts_key";
    public static final long TEST_SAMPLE_TS_VALUE = 234L;
    public static final String TEST_UNIT_KEY = "test_unit_key";
    public static final String TEST_UNIT_VALUE = "Second";
    public static final String TEST_STORAGE_RESOLUTION_KEY = "test_storage_res_key";
    public static final int TEST_STORAGE_RESOLUTION_VALUE = 9;
    public static final String TEST_STATS_MAX_KEY = "test_stats_max_key";
    public static final double TEST_STATS_MAX_VALUE = 999d;
    public static final String TEST_STATS_MIN_KEY = "test_stats_min_key";
    public static final double TEST_STATS_MIN_VALUE = 1d;
    public static final String TEST_STATS_SUM_KEY = "test_stats_sum_key";
    public static final double TEST_STATS_SUM_VALUE = 10d;
    public static final String TEST_STATS_COUNT_KEY = "test_stats_count_key";
    public static final double TEST_STATS_COUNT_VALUE = 11d;

    public static final DataType DATA_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD(TEST_METRIC_NAME, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_DIMENSION_KEY_1, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_DIMENSION_KEY_2, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_VALUE_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_COUNT_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_TIMESTAMP_KEY, DataTypes.TIMESTAMP()),
                            DataTypes.FIELD(TEST_UNIT_KEY, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_STORAGE_RESOLUTION_KEY, DataTypes.INT()),
                            DataTypes.FIELD(TEST_STATS_MAX_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_STATS_MIN_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_STATS_SUM_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_STATS_COUNT_KEY, DataTypes.DOUBLE()))
                    .notNull();
}
