package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.READER_EMPTY_RECORDS_FETCH_INTERVAL;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_MAX;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

class KinesisShardSplitReaderBaseTest {

    private static Configuration newConfigurationForTest() {
        return new Configuration().set(SHARD_GET_RECORDS_MAX, 50);
    }

    private static Stream<Arguments> readerTypeAndInterval() {
        return Stream.of(
                Arguments.of(NullReturningReader.class, 0L),
                Arguments.of(NullReturningReader.class, 250L),
                Arguments.of(NullReturningReader.class, 1000L),
                Arguments.of(EmptyRecordReturningReader.class, 0L),
                Arguments.of(EmptyRecordReturningReader.class, 250L),
                Arguments.of(EmptyRecordReturningReader.class, 1000L));
    }

    @MethodSource("readerTypeAndInterval")
    @ParameterizedTest
    public void testGetRecordsIntervalForIdleSource(
            Class<? extends CountingReader> readerClass, long interval) {
        Configuration configuration = newConfigurationForTest();
        configuration.set(READER_EMPTY_RECORDS_FETCH_INTERVAL, Duration.ofMillis(interval));

        // Given reader with custom interval
        List<KinesisShardSplit> shardSplits = createShardSplits(8);
        Map<String, KinesisShardMetrics> metrics = getShardMetrics(shardSplits);
        CountingReader reader = buildReader(readerClass, configuration, metrics);

        reader.handleSplitsChanges(new SplitsAddition<>(shardSplits));

        // When (empty) records are fetched continuously
        await().pollInSameThread()
                .pollInterval(Duration.ofMillis(1))
                .atMost(interval + 1000L, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            reader.fetch();

                            // Then call fetch record at intervals
                            for (List<Long> fetchRecordsCallTimes :
                                    reader.getFetchRecordsCallTimestamps().values()) {
                                assertThat(fetchRecordsCallTimes.size()).isEqualTo(2);

                                // Ensure interval between fetchRecord calls is between configured
                                // interval and interval + 250 milliseconds (allowing 250
                                // milliseconds of leeway)
                                assertThat(
                                                fetchRecordsCallTimes.get(1)
                                                        - fetchRecordsCallTimes.get(0))
                                        .isBetween(interval, interval + 250L);
                            }

                            assertThat(reader.getFetchRecordsCallTimestamps().size()).isEqualTo(8);
                        });
    }

    private static Stream<Arguments> readerTypeAndShardCount() {
        return Stream.of(
                Arguments.of(NullReturningReader.class, 1),
                Arguments.of(NullReturningReader.class, 8),
                Arguments.of(NullReturningReader.class, 64),
                Arguments.of(EmptyRecordReturningReader.class, 1),
                Arguments.of(EmptyRecordReturningReader.class, 8),
                Arguments.of(EmptyRecordReturningReader.class, 64));
    }

    @MethodSource("readerTypeAndShardCount")
    @ParameterizedTest
    public void testFetchRecordsIntervalForMultipleIdleSource(
            Class<? extends CountingReader> readerClass, int shardCount) {
        // Given reader with shard count
        List<KinesisShardSplit> shardSplits = createShardSplits(shardCount);
        Map<String, KinesisShardMetrics> metrics = getShardMetrics(shardSplits);
        CountingReader reader = buildReader(readerClass, newConfigurationForTest(), metrics);

        reader.handleSplitsChanges(new SplitsAddition<>(shardSplits));

        // When (empty) records are fetched continuously
        await().pollInSameThread()
                .pollInterval(Duration.ofMillis(1))
                .atMost(250L + 1000L, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            reader.fetch();

                            // Then call fetch record at intervals
                            for (List<Long> fetchRecordCallTime :
                                    reader.getFetchRecordsCallTimestamps().values()) {
                                assertThat(fetchRecordCallTime.size()).isEqualTo(2);

                                // Default READER_EMPTY_RECORDS_FETCH_INTERVAL value at 250
                                // Ensure interval between fetchRecord calls is between 250 and 500
                                // milliseconds (allowing 250 milliseconds of leeway)
                                assertThat(fetchRecordCallTime.get(1) - fetchRecordCallTime.get(0))
                                        .isBetween(250L, 250L + 250L);
                            }

                            assertThat(reader.getFetchRecordsCallTimestamps().size())
                                    .isEqualTo(shardCount);
                        });
    }

    private static CountingReader buildReader(
            Class<? extends CountingReader> readerClass,
            Configuration configuration,
            Map<String, KinesisShardMetrics> metrics) {
        if (readerClass == NullReturningReader.class) {
            return new NullReturningReader(metrics, configuration);
        } else if (readerClass == EmptyRecordReturningReader.class) {
            return new EmptyRecordReturningReader(metrics, configuration);
        }

        throw new RuntimeException(
                "No test implementation found for " + readerClass.getCanonicalName());
    }

    abstract static class CountingReader extends KinesisShardSplitReaderBase {

        private Map<KinesisShardSplitState, List<Long>> fetchRecordsCallTimestamps;

        protected CountingReader(
                Map<String, KinesisShardMetrics> shardMetricGroupMap, Configuration configuration) {
            super(shardMetricGroupMap, configuration);
            fetchRecordsCallTimestamps = new HashMap<>();
        }

        @Override
        protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
            recordFetchTimestamp(splitState);

            return null;
        }

        private void recordFetchTimestamp(KinesisShardSplitState splitState) {
            if (fetchRecordsCallTimestamps.containsKey(splitState)) {
                fetchRecordsCallTimestamps.get(splitState).add(System.currentTimeMillis());
            } else {
                ArrayList<Long> fetchRecordCallTime = new ArrayList<>();
                fetchRecordCallTime.add(System.currentTimeMillis());
                fetchRecordsCallTimestamps.put(splitState, fetchRecordCallTime);
            }
        }

        @Override
        public void close() throws Exception {}

        public Map<KinesisShardSplitState, List<Long>> getFetchRecordsCallTimestamps() {
            return fetchRecordsCallTimestamps;
        }
    }

    static class NullReturningReader extends CountingReader {
        public NullReturningReader(
                Map<String, KinesisShardMetrics> shardMetricGroupMap, Configuration configuration) {
            super(shardMetricGroupMap, configuration);
        }

        @Override
        protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
            super.fetchRecords(splitState);
            return null;
        }
    }

    static class EmptyRecordReturningReader extends CountingReader {
        public EmptyRecordReturningReader(
                Map<String, KinesisShardMetrics> shardMetricGroupMap, Configuration configuration) {
            super(shardMetricGroupMap, configuration);
        }

        @Override
        protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
            super.fetchRecords(splitState);
            return new RecordBatch(
                    Collections.emptyList(), splitState.getKinesisShardSplit(), 0L, false);
        }
    }

    private static List<KinesisShardSplit> createShardSplits(int shardCount) {
        return IntStream.range(0, shardCount)
                .mapToObj(shardId -> getTestSplit(generateShardId(shardId)))
                .collect(Collectors.toList());
    }

    private static Map<String, KinesisShardMetrics> getShardMetrics(
            List<KinesisShardSplit> shardSplits) {
        Map<String, KinesisShardMetrics> metrics = new HashMap<>();
        MetricListener metricListener = new MetricListener();

        shardSplits.forEach(
                shardSplit ->
                        metrics.put(
                                shardSplit.splitId(),
                                new KinesisShardMetrics(
                                        shardSplit, metricListener.getMetricGroup())));

        return metrics;
    }
}
