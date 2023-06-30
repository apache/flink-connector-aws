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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorSourceOptionsUtil;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM_ARN;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link KinesisDynamicSource}. */
public class KinesisDynamicTableSourceFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kinesis-source";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();

        KinesisStreamsConnectorSourceOptionsUtil kinesisStreamsConnectorSourceOptionsUtil =
                new KinesisStreamsConnectorSourceOptionsUtil(
                        catalogTable.getOptions(), tableOptions.get(STREAM_ARN));

        Configuration sourceConfig =
                kinesisStreamsConnectorSourceOptionsUtil.getValidatedSourceConfigurations();

        KinesisDynamicSource.KinesisDynamicTableSourceBuilder builder =
                new KinesisDynamicSource.KinesisDynamicTableSourceBuilder();

        builder.setStream(tableOptions.get(STREAM_ARN))
                .setDecodingFormat(decodingFormat)
                .setConsumedDataType(context.getPhysicalRowDataType())
                .setSourceConfig(sourceConfig);

        KinesisDynamicSource kinesisDynamicSource = builder.build();
        return kinesisDynamicSource;
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
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
