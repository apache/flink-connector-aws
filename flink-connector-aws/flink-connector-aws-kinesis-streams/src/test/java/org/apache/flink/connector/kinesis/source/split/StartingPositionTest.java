package org.apache.flink.connector.kinesis.source.split;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class StartingPositionTest {

    @Test
    void testEquals() {
        EqualsVerifier.forClass(StartingPosition.class).verify();
    }
}
