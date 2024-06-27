package org.apache.flink.connector.kinesis.source.split;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisShardSplitTest {

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:290038087681:stream/keeneses_stream";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final StartingPosition STARTING_POSITION = StartingPosition.fromStart();
    private static final Set<String> PARENT_SHARD_IDS = Collections.emptySet();

    @Test
    void testStreamArnNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new KinesisShardSplit(
                                        null, SHARD_ID, STARTING_POSITION, PARENT_SHARD_IDS))
                .withMessageContaining("streamArn cannot be null");
    }

    @Test
    void testShardIdNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new KinesisShardSplit(
                                        STREAM_ARN, null, STARTING_POSITION, PARENT_SHARD_IDS))
                .withMessageContaining("shardId cannot be null");
    }

    @Test
    void testStartingPositionNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () -> new KinesisShardSplit(STREAM_ARN, SHARD_ID, null, PARENT_SHARD_IDS))
                .withMessageContaining("startingPosition cannot be null");
    }

    @Test
    void testParentShardIdsNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () -> new KinesisShardSplit(STREAM_ARN, SHARD_ID, STARTING_POSITION, null))
                .withMessageContaining("parentShardIds cannot be null");
    }

    @Test
    void testEquals() {
        EqualsVerifier.forClass(KinesisShardSplit.class).verify();
    }
}
