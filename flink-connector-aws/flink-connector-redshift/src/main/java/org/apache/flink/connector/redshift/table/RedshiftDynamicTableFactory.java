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

package org.apache.flink.connector.redshift.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redshift.internal.executor.RedshiftS3Util;
import org.apache.flink.connector.redshift.internal.options.RedshiftOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** Dynamic Table Factory. */
public class RedshiftDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "redshift";

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("the Redshift hostname.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5439)
                    .withDescription("the Redshift port.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the Redshift username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the Redshift password.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .defaultValue("dev")
                    .withDescription("the Redshift database name. Default to `dev`.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the Redshift table name.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key("sink.batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "the flush max size, over this number of records, will flush data. The default value is 1000.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1L))
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");

    public static final ConfigOption<Boolean> COPY_MODE =
            ConfigOptions.key("copy-mode")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("using Redshift COPY command to insert/upsert or not.");
    public static final ConfigOption<String> TEMP_S3_URI =
            ConfigOptions.key("copy-temp-s3-uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("using Redshift COPY command must provide a S3 URI.");
    public static final ConfigOption<String> IAM_ROLE_ARN =
            ConfigOptions.key("iam-role-arn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "using Redshift COPY function must provide a IAM Role which have attached to the Cluster.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        String[] primaryKeys =
                resolvedSchema
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(keys -> keys.toArray(new String[0]))
                        .orElse(new String[0]);
        String[] fieldNames = resolvedSchema.getColumnNames().toArray(new String[0]);
        DataType[] fieldDataTypes = resolvedSchema.getColumnDataTypes().toArray(new DataType[0]);
        return new RedshiftDynamicTableSink(
                getOptions(config), primaryKeys, fieldNames, fieldDataTypes);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTNAME);
        requiredOptions.add(PORT);
        requiredOptions.add(DATABASE_NAME);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(COPY_MODE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(TEMP_S3_URI);
        optionalOptions.add(IAM_ROLE_ARN);
        return optionalOptions;
    }

    private RedshiftOptions getOptions(ReadableConfig config) {
        return (new RedshiftOptions.Builder())
                .withHostname(config.get(HOSTNAME))
                .withPort(config.get(PORT))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withCopyMode(config.get(COPY_MODE))
                .withTempS3Uri(config.get(TEMP_S3_URI))
                .withIamRoleArn(config.get(IAM_ROLE_ARN))
                .build();
    }

    private void validateConfigOptions(ReadableConfig config) {
        if (config.get(COPY_MODE) && !config.getOptional(TEMP_S3_URI).isPresent()) {
            throw new IllegalArgumentException(
                    "A S3 URL must be provided as the COPY mode is True!");
        } else if (config.getOptional(TEMP_S3_URI).isPresent()) {
            String uri = config.get(TEMP_S3_URI);
            try {

                RedshiftS3Util.getS3Parts(uri);
            } catch (Exception e) {
                throw new IllegalArgumentException("A incorrect S3 URL provided! S3 URI = " + uri, e);
            }
        }

        if (config.get(COPY_MODE) && !config.getOptional(IAM_ROLE_ARN).isPresent()) {
            throw new IllegalArgumentException(
                    "A IAM Role ARN which attached to the Redshift cluster must be provided as the COPY mode is True!");
        }
    }
}
