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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.connector.aws.config.AWSConfigOptions;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorState;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.kinesis.table.RowMetadata.ShardId;
import static org.apache.flink.connector.kinesis.table.RowMetadata.Timestamp;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KinesisDynamicSource} created by {@link KinesisDynamicTableFactory}. */
public class KinesisDynamicTableSourceFactoryTest extends TestLogger {

    @Test
    public void testGoodTableSource() {
        ResolvedSchema sourceSchema = defaultSourceSchema();
        Map<String, String> sourceOptions = defaultTableOptions().build();

        // Construct actual DynamicTableSource using FactoryUtil
        KinesisDynamicSource actualSource =
                (KinesisDynamicSource) createTableSource(sourceSchema, sourceOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSource expectedSource =
                new KinesisDynamicSource(
                        sourceSchema.toPhysicalRowDataType(),
                        STREAM_ARN,
                        defaultSourceConfig(),
                        new TestFormatFactory.DecodingFormatMock(",", true));

        // verify that the constructed DynamicTableSink is as expected
        assertThat(actualSource).isEqualTo(expectedSource);

        // verify that the copy of the constructed DynamicTableSink is as expected
        assertThat(actualSource.copy()).isEqualTo(expectedSource);

        // verify produced source
        ScanTableSource.ScanRuntimeProvider functionProvider =
                actualSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertKinesisStreamsSource(functionProvider);
    }

    @Test
    public void testGoodTableSourceWithMetadataFields() {
        ResolvedSchema sourceSchema = defaultSourceSchema();
        Map<String, String> sourceOptions = defaultTableOptions().build();

        RowMetadata[] requestedMetadata = new RowMetadata[] {ShardId, Timestamp};
        List<String> metadataKeys = Arrays.asList(ShardId.getFieldName(), Timestamp.getFieldName());
        DataType producedDataType = getProducedType(sourceSchema, requestedMetadata);

        // Construct actual DynamicTableSource using FactoryUtil
        KinesisDynamicSource actualSource =
                (KinesisDynamicSource) createTableSource(sourceSchema, sourceOptions);
        actualSource.applyReadableMetadata(metadataKeys, producedDataType);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSource expectedSource =
                new KinesisDynamicSource(
                        sourceSchema.toPhysicalRowDataType(),
                        STREAM_ARN,
                        defaultSourceConfig(),
                        new TestFormatFactory.DecodingFormatMock(",", true),
                        producedDataType,
                        Arrays.asList(requestedMetadata));

        // verify that the constructed DynamicTableSource is as expected
        assertThat(actualSource).isEqualTo(expectedSource);

        // verify that the copy of the constructed DynamicTableSource is as expected
        assertThat(actualSource.copy()).isEqualTo(expectedSource);
    }

    private ResolvedSchema defaultSourceSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("curr_id", DataTypes.BIGINT()),
                        Column.physical("time", DataTypes.TIMESTAMP(3)),
                        Column.computed(
                                "next_id",
                                ResolvedExpressionMock.of(DataTypes.BIGINT(), "curr_id + 1"))),
                Collections.singletonList(
                        WatermarkSpec.of(
                                "time",
                                ResolvedExpressionMock.of(
                                        DataTypes.TIMESTAMP(3), "time - INTERVAL '5' SECOND"))),
                null);
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = "kinesis";
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(KinesisConnectorOptions.STREAM_ARN, STREAM_ARN)
                .withTableOption(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2")
                .withTableOption(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, "BASIC")
                .withTableOption(AWSConfigOptions.TRUST_ALL_CERTIFICATES_OPTION, "true")
                .withTableOption("aws.credentials.basic.accesskeyid", "ververicka")
                .withTableOption("aws.credentials.basic.secretkey", "SuperSecretSecretSquirrel")
                .withTableOption(
                        KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP, "AT_TIMESTAMP")
                .withTableOption(
                        KinesisSourceConfigOptions.STREAM_TIMESTAMP_DATE_FORMAT,
                        "yyyy-MM-dd'T'HH:mm:ss")
                .withTableOption(
                        KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP, "2014-10-22T12:00:00")

                // default format options
                .withFormatOption(TestFormatFactory.DELIMITER, ",")
                .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
    }

    private Configuration defaultSourceConfig() {
        return ConfigurationUtils.createConfiguration(
                new Properties() {
                    {
                        setProperty(AWSConfigOptions.AWS_REGION_OPTION.key(), "us-west-2");
                        setProperty(
                                AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION.key(), "BASIC");
                        setProperty("aws.credentials.provider.basic.accesskeyid", "ververicka");
                        setProperty(
                                "aws.credentials.provider.basic.secretkey",
                                "SuperSecretSecretSquirrel");
                        setProperty(AWSConfigOptions.TRUST_ALL_CERTIFICATES_OPTION.key(), "true");
                        setProperty(
                                KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP.key(),
                                "AT_TIMESTAMP");
                        setProperty(
                                KinesisSourceConfigOptions.STREAM_TIMESTAMP_DATE_FORMAT.key(),
                                "yyyy-MM-dd'T'HH:mm:ss");
                        setProperty(
                                KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP.key(),
                                "2014-10-22T12:00:00");
                    }
                });
    }

    private KinesisStreamsSource<?> assertKinesisStreamsSource(
            ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
        final DataStreamScanProvider dataStreamScanProvider = (DataStreamScanProvider) provider;
        final Transformation<RowData> transformation =
                dataStreamScanProvider
                        .produceDataStream(
                                n -> Optional.empty(),
                                StreamExecutionEnvironment.createLocalEnvironment())
                        .getTransformation();
        assertThat(transformation).isInstanceOf(SourceTransformation.class);
        SourceTransformation<RowData, KinesisShardSplit, KinesisStreamsSourceEnumeratorState>
                sourceTransformation =
                        (SourceTransformation<
                                        RowData,
                                        KinesisShardSplit,
                                        KinesisStreamsSourceEnumeratorState>)
                                transformation;
        assertThat(sourceTransformation.getSource()).isInstanceOf(KinesisStreamsSource.class);
        return (KinesisStreamsSource<?>) sourceTransformation.getSource();
    }

    private DataType getProducedType(ResolvedSchema schema, RowMetadata... requestedMetadata) {
        Stream<DataTypes.Field> physicalFields =
                IntStream.range(0, schema.getColumnCount())
                        .mapToObj(
                                i ->
                                        DataTypes.FIELD(
                                                schema.getColumnNames().get(i),
                                                schema.getColumnDataTypes().get(i)));
        Stream<DataTypes.Field> metadataFields =
                Arrays.stream(requestedMetadata)
                        .map(m -> DataTypes.FIELD(m.name(), m.getDataType()));
        Stream<DataTypes.Field> allFields = Stream.concat(physicalFields, metadataFields);

        return DataTypes.ROW(allFields.toArray(DataTypes.Field[]::new));
    }
}
