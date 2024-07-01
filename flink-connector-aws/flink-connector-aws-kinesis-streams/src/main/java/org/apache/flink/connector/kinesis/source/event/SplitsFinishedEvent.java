package org.apache.flink.connector.kinesis.source.event;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Set;

/** Source event used by source reader to communicate that splits are finished to enumerator. */
@Internal
public class SplitsFinishedEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final Set<String> finishedSplitIds;

    public SplitsFinishedEvent(Set<String> finishedSplitIds) {
        this.finishedSplitIds = finishedSplitIds;
    }

    public Set<String> getFinishedSplitIds() {
        return finishedSplitIds;
    }

    @Override
    public String toString() {
        return "SplitsFinishedEvent{"
                + "finishedSplitIds=["
                + String.join(",", finishedSplitIds)
                + "]}";
    }
}
