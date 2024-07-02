package org.apache.flink.connector.dynamodb.source.split;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamoDbStreamsShardSplitTest {

    private static final String STREAM_ARN =  "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final StartingPosition STARTING_POSITION = StartingPosition.fromStart();

    @Test
    void testStreamArnNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new DynamoDbStreamsShardSplit(null, SHARD_ID, STARTING_POSITION))
                .withMessageContaining("streamArn cannot be null");
    }

    @Test
    void testShardIdNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new DynamoDbStreamsShardSplit(STREAM_ARN, null, STARTING_POSITION))
                .withMessageContaining("shardId cannot be null");
    }

    @Test
    void testStartingPositionNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, null))
                .withMessageContaining("startingPosition cannot be null");
    }

    @Test
    void testEquals() {
        DynamoDbStreamsShardSplit dynamoDbStreamsShardSplit1 = new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, STARTING_POSITION);
        DynamoDbStreamsShardSplit dynamoDbStreamsShardSplit2 = new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, STARTING_POSITION);
        DynamoDbStreamsShardSplit dynamoDbStreamsShardSplit3 = new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, StartingPosition.latest());

        assertTrue(dynamoDbStreamsShardSplit1.equals(dynamoDbStreamsShardSplit2));
        assertFalse(dynamoDbStreamsShardSplit1.equals(dynamoDbStreamsShardSplit3));

    }
}
