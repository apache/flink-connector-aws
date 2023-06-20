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
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorState;
import org.apache.flink.connector.kinesis.source.model.Metadata;
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.model.Metadata.ShardId;
import static org.apache.flink.connector.kinesis.source.model.Metadata.Timestamp;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KinesisDynamicSource} created by {@link KinesisDynamicTableSourceFactory}. */
public class KinesisDynamicTableSourceFactoryTest extends TestLogger {

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789123:stream/myStream";

    @Rule public ExpectedException thrown = ExpectedException.none();

    // --------------------------------------------------------------------------------------------
    // Positive tests
    // --------------------------------------------------------------------------------------------

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

        Metadata[] requestedMetadata = new Metadata[] {ShardId, Timestamp};
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

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

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
        String connector = KinesisDynamicTableSourceFactory.IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(KinesisConnectorOptions.STREAM_ARN, STREAM_ARN)
                .withTableOption("aws.region", "us-west-2")
                .withTableOption("aws.credentials.provider", "BASIC")
                .withTableOption("aws.credentials.basic.accesskeyid", "ververicka")
                .withTableOption("aws.credentials.basic.secretkey", "SuperSecretSecretSquirrel")
                .withTableOption("scan.stream.initpos", "AT_TIMESTAMP")
                .withTableOption("scan.stream.initpos-timestamp-format", "yyyy-MM-dd'T'HH:mm:ss")
                .withTableOption("scan.stream.initpos-timestamp", "2014-10-22T12:00:00")

                // default format options
                .withFormatOption(TestFormatFactory.DELIMITER, ",")
                .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
    }

    private Configuration defaultSourceConfig() {
        return ConfigurationUtils.createConfiguration(
                new Properties() {
                    {
                        setProperty("aws.region", "us-west-2");
                        setProperty("aws.credentials.provider", "BASIC");
                        setProperty("aws.credentials.provider.basic.accesskeyid", "ververicka");
                        setProperty(
                                "aws.credentials.provider.basic.secretkey",
                                "SuperSecretSecretSquirrel");
                        setProperty("flink.stream.initpos", "AT_TIMESTAMP");
                        setProperty(
                                "flink.stream.initpos.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss");
                        setProperty("flink.stream.initpos.timestamp", "2014-10-22T12:00:00");
                    }
                });
    }

    private DataType getProducedType(ResolvedSchema schema, Metadata... requestedMetadata) {
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

    private <T> T as(Object object, Class<T> clazz) {
        assertThat(object).isInstanceOf(clazz);
        return clazz.cast(object);
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
}
