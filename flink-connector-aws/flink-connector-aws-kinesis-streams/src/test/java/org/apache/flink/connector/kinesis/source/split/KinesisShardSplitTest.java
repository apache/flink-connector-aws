package org.apache.flink.connector.kinesis.source.split;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisShardSplitTest {

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:290038087681:stream/keeneses_stream";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final StartingPosition STARTING_POSITION = StartingPosition.fromStart();

    @Test
    void testStreamArnNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new KinesisShardSplit(null, SHARD_ID, STARTING_POSITION))
                .withMessageContaining("streamArn cannot be null");
    }

    @Test
    void testShardIdNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new KinesisShardSplit(STREAM_ARN, null, STARTING_POSITION))
                .withMessageContaining("shardId cannot be null");
    }

    @Test
    void testStartingPositionNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new KinesisShardSplit(STREAM_ARN, SHARD_ID, null))
                .withMessageContaining("startingPosition cannot be null");
    }

    @Test
    void testEquals() {
        EqualsVerifier.forClass(KinesisShardSplit.class).verify();
    }
}
