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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorSinkOptionsUtils;
import org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorSourceOptionsUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.AWS_REGION;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_FAIL_ON_ERROR;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM_ARN;
import static org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorSinkOptionsUtils.KINESIS_CLIENT_PROPERTIES_KEY;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link KinesisDynamicSink}. */
@Internal
public class KinesisDynamicTableFactory extends AsyncDynamicTableSinkFactory
        implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kinesis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

        KinesisStreamsConnectorSinkOptionsUtils optionsUtils =
                new KinesisStreamsConnectorSinkOptionsUtils(
                        factoryContext.getResolvedOptions(),
                        factoryContext.getTableOptions(),
                        (RowType) factoryContext.getPhysicalDataType().getLogicalType(),
                        factoryContext.getPartitionKeys(),
                        context.getClassLoader());
        // validate the data types of the table options
        factoryContext
                .getFactoryHelper()
                .validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));

        // Validate option values
        validateKinesisPartitioner(
                factoryContext.getTableOptions(), factoryContext.isPartitioned());
        Properties properties = optionsUtils.getValidatedSinkConfigurations();

        KinesisDynamicSink.KinesisDynamicTableSinkBuilder builder =
                new KinesisDynamicSink.KinesisDynamicTableSinkBuilder();

        builder.setStreamArn((String) properties.get(STREAM_ARN.key()))
                .setKinesisClientProperties(
                        (Properties) properties.get(KINESIS_CLIENT_PROPERTIES_KEY))
                .setEncodingFormat(factoryContext.getEncodingFormat())
                .setConsumedDataType(factoryContext.getPhysicalDataType())
                .setPartitioner(
                        (PartitionKeyGenerator<RowData>) properties.get(SINK_PARTITIONER.key()));
        addAsyncOptionsToBuilder(properties, builder);
        Optional.ofNullable((Boolean) properties.get(SINK_FAIL_ON_ERROR.key()))
                .ifPresent(builder::setFailOnError);
        return builder.build();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        KinesisStreamsConnectorSourceOptionsUtils kinesisStreamsConnectorSourceOptionsUtil =
                new KinesisStreamsConnectorSourceOptionsUtils(
                        catalogTable.getOptions(), tableOptions.get(STREAM_ARN));

        helper.validateExcept(
                kinesisStreamsConnectorSourceOptionsUtil
                        .getNonValidatedPrefixes()
                        .toArray(new String[0]));

        Configuration sourceConfig =
                kinesisStreamsConnectorSourceOptionsUtil.getValidatedSourceConfigurations();

        KinesisDynamicSource.KinesisDynamicTableSourceBuilder builder =
                new KinesisDynamicSource.KinesisDynamicTableSourceBuilder();

        return builder.setStreamArn(tableOptions.get(STREAM_ARN))
                .setDecodingFormat(decodingFormat)
                .setConsumedDataType(context.getPhysicalRowDataType())
                .setSourceConfig(sourceConfig)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STREAM_ARN);
        options.add(FORMAT);
        options.add(AWS_REGION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(SINK_PARTITIONER);
        options.add(SINK_PARTITIONER_FIELD_DELIMITER);
        options.add(SINK_FAIL_ON_ERROR);
        return KinesisStreamsConnectorSinkOptionsUtils.KinesisProducerOptionsMapper
                .addDeprecatedKeys(options);
    }

    private static void validateKinesisPartitioner(
            ReadableConfig tableOptions, boolean isPartitioned) {
        tableOptions
                .getOptional(SINK_PARTITIONER)
                .ifPresent(
                        partitioner -> {
                            if (isPartitioned) {
                                throw new ValidationException(
                                        String.format(
                                                "Cannot set %s option for a table defined with a PARTITIONED BY clause",
                                                SINK_PARTITIONER.key()));
                            }
                        });
    }
}
