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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.lineage.DefaultLineageDataset;
import org.apache.flink.streaming.api.lineage.DefaultLineageVertex;
import org.apache.flink.streaming.api.lineage.DefaultSourceLineageVertex;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import software.amazon.awssdk.arns.Arn;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for constructing lineage datasets and vertices for Kinesis Streams.
 *
 * <p>Dataset identity follows the OpenLineage naming convention for ARN-identified AWS resources
 * (matching the AWS Glue precedent): the namespace is {@code
 * arn:{partition}:kinesis:{region}:{account}} and the dataset name carries the resource type,
 * {@code stream/{streamName}}.
 */
@Internal
public class KinesisLineageUtil {

    private static final String DATASET_NAME_PREFIX = "stream/";
    private static final String FALLBACK_NAMESPACE = "arn:aws:kinesis";

    private KinesisLineageUtil() {}

    /**
     * Constructs the dataset namespace for a Kinesis stream from its ARN, e.g. {@code
     * arn:aws:kinesis:us-east-1:123456789012}.
     */
    public static String namespaceOf(String streamArn) {
        Arn arn = Arn.fromString(streamArn);
        return String.format(
                "arn:%s:kinesis:%s:%s",
                arn.partition(), arn.region().orElse(""), arn.accountId().orElse(""));
    }

    /**
     * Constructs a best-effort dataset namespace when only the stream name is known, using the
     * region from the client properties when available (e.g. {@code arn:aws:kinesis:eu-west-1})
     * so that datasets stay under the same scheme as ARN-derived namespaces.
     */
    public static String namespaceOf(@Nullable Properties clientProperties) {
        String region =
                clientProperties == null
                        ? null
                        : clientProperties.getProperty(AWSConfigConstants.AWS_REGION);
        return region == null ? FALLBACK_NAMESPACE : FALLBACK_NAMESPACE + ":" + region;
    }

    /** Constructs the dataset name for a Kinesis stream, e.g. {@code stream/my-stream}. */
    public static String nameOf(String streamName) {
        return DATASET_NAME_PREFIX + streamName;
    }

    /** Extracts the stream name from a Kinesis stream ARN. */
    public static String streamNameOf(String streamArn) {
        return Arn.fromString(streamArn).resource().resource();
    }

    /** Builds the lineage dataset for a stream identified by its ARN, without type facet. */
    public static LineageDataset datasetOf(String streamArn) {
        return datasetOf(streamArn, null);
    }

    /**
     * Builds the lineage dataset for a stream identified by its ARN, attaching the Kinesis facet
     * and, when available, the type facet carrying the record schema.
     */
    public static LineageDataset datasetOf(
            String streamArn, @Nullable TypeInformation<?> typeInformation) {
        Arn arn = Arn.fromString(streamArn);
        String streamName = arn.resource().resource();
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(
                KinesisDatasetFacet.KINESIS_FACET_NAME,
                new KinesisDatasetFacet(streamArn, streamName, arn.region().orElse(null)));
        if (typeInformation != null) {
            facets.put(TypeDatasetFacet.TYPE_FACET_NAME, new TypeDatasetFacet(typeInformation));
        }
        return new DefaultLineageDataset(nameOf(streamName), namespaceOf(streamArn), facets);
    }

    /**
     * Builds the lineage dataset for a stream known only by name (no ARN), deriving the namespace
     * region from the client properties when present.
     */
    public static LineageDataset datasetOfStreamName(
            String streamName, @Nullable Properties clientProperties) {
        String region =
                clientProperties == null
                        ? null
                        : clientProperties.getProperty(AWSConfigConstants.AWS_REGION);
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(
                KinesisDatasetFacet.KINESIS_FACET_NAME,
                new KinesisDatasetFacet(null, streamName, region));
        return new DefaultLineageDataset(
                nameOf(streamName), namespaceOf(clientProperties), facets);
    }

    /** Wraps datasets in a source lineage vertex. */
    public static SourceLineageVertex sourceLineageVertexOf(Collection<LineageDataset> datasets) {
        DefaultSourceLineageVertex vertex =
                new DefaultSourceLineageVertex(Boundedness.CONTINUOUS_UNBOUNDED);
        datasets.forEach(vertex::addDataset);
        return vertex;
    }

    /** Wraps datasets in a sink lineage vertex. */
    public static LineageVertex sinkLineageVertexOf(Collection<LineageDataset> datasets) {
        DefaultLineageVertex vertex = new DefaultLineageVertex();
        datasets.forEach(vertex::addLineageDataset);
        return vertex;
    }
}
