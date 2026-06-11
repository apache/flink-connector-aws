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

package org.apache.flink.connector.kinesis.lineage;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Kinesis Streams lineage support. */
class KinesisLineageTest {

    private static final String TEST_STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/my-test-stream";

    @Test
    void testSourceLineageVertex() {
        KinesisStreamsSource<String> source =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(TEST_STREAM_ARN)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        assertThat(source).isInstanceOf(LineageVertexProvider.class);

        LineageVertex vertex = ((LineageVertexProvider) source).getLineageVertex();
        assertThat(vertex).isNotNull();

        List<LineageDataset> datasets = vertex.datasets();
        assertThat(datasets).hasSize(1);

        LineageDataset dataset = datasets.get(0);
        assertThat(dataset.name()).isEqualTo("my-test-stream");
        assertThat(dataset.namespace()).isEqualTo("arn:aws:kinesis:us-east-1:123456789012:stream");
    }

    @Test
    void testSourceLineageDatasetHasEmptyFacets() {
        KinesisStreamsSource<String> source =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(TEST_STREAM_ARN)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        LineageVertex vertex = ((LineageVertexProvider) source).getLineageVertex();
        LineageDataset dataset = vertex.datasets().get(0);

        // Facets are added by the table planner / OL module, not the connector
        assertThat(dataset.facets()).isEmpty();
    }

    @Test
    void testSinkLineageVertexWithArn() {
        KinesisStreamsSink<String> sink =
                KinesisStreamsSink.<String>builder()
                        .setStreamArn(TEST_STREAM_ARN)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setPartitionKeyGenerator(element -> "key")
                        .build();

        assertThat(sink).isInstanceOf(LineageVertexProvider.class);

        LineageVertex vertex = ((LineageVertexProvider) sink).getLineageVertex();
        assertThat(vertex).isNotNull();

        List<LineageDataset> datasets = vertex.datasets();
        assertThat(datasets).hasSize(1);

        LineageDataset dataset = datasets.get(0);
        assertThat(dataset.name()).isEqualTo("my-test-stream");
        assertThat(dataset.namespace()).isEqualTo("arn:aws:kinesis:us-east-1:123456789012:stream");
    }

    @Test
    void testSinkLineageVertexWithNameOnly() {
        KinesisStreamsSink<String> sink =
                KinesisStreamsSink.<String>builder()
                        .setStreamName("name-only-stream")
                        .setSerializationSchema(new SimpleStringSchema())
                        .setPartitionKeyGenerator(element -> "key")
                        .build();

        LineageVertex vertex = ((LineageVertexProvider) sink).getLineageVertex();
        List<LineageDataset> datasets = vertex.datasets();

        assertThat(datasets).hasSize(1);
        assertThat(datasets.get(0).name()).isEqualTo("name-only-stream");
        assertThat(datasets.get(0).namespace()).isEqualTo("kinesis://unknown-region");
    }

    @Test
    void testLineageUtilNamespaceExtraction() {
        String namespace = KinesisLineageUtil.namespaceOf(TEST_STREAM_ARN);
        assertThat(namespace).isEqualTo("arn:aws:kinesis:us-east-1:123456789012:stream");
    }

    @Test
    void testLineageUtilStreamNameExtraction() {
        String streamName = KinesisLineageUtil.streamNameOf(TEST_STREAM_ARN);
        assertThat(streamName).isEqualTo("my-test-stream");
    }

    @Test
    void testLineageUtilDifferentRegions() {
        String euArn = "arn:aws:kinesis:eu-west-1:999888777666:stream/orders";
        assertThat(KinesisLineageUtil.namespaceOf(euArn))
                .isEqualTo("arn:aws:kinesis:eu-west-1:999888777666:stream");
        assertThat(KinesisLineageUtil.streamNameOf(euArn)).isEqualTo("orders");
    }

    @Test
    void testKinesisDatasetFacetEquality() {
        KinesisDatasetFacet facet1 = new KinesisDatasetFacet(TEST_STREAM_ARN, "stream", "us-east-1");
        KinesisDatasetFacet facet2 = new KinesisDatasetFacet(TEST_STREAM_ARN, "stream", "us-east-1");
        KinesisDatasetFacet facet3 =
                new KinesisDatasetFacet(TEST_STREAM_ARN, "other", "us-west-2");

        assertThat(facet1).isEqualTo(facet2);
        assertThat(facet1).isNotEqualTo(facet3);
        assertThat(facet1.name()).isEqualTo(KinesisDatasetFacet.KINESIS_FACET_NAME);
    }
}
