package org.apache.flink.connector.dynamodb.source.split;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StartingPositionTest {

    @Test
    void testEquals() {
        StartingPosition startingPosition1 = StartingPosition.fromStart();
        StartingPosition startingPosition2 = StartingPosition.fromStart();
        StartingPosition startingPosition3 = StartingPosition.latest();
        assertTrue(startingPosition1.equals(startingPosition2));
        assertFalse(startingPosition1.equals(startingPosition3));
    }
}
