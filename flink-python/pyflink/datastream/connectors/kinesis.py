################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from typing import Dict, Union, List

from pyflink.common import SerializationSchema, DeserializationSchema
from pyflink.datastream.connectors import Sink, Source
from pyflink.java_gateway import get_gateway

__all__ = [
    'KinesisShardAssigner',
    'KinesisDeserializationSchema',
    'PartitionKeyGenerator',
    'KinesisStreamsSource',
    'KinesisStreamsSourceBuilder',
    'KinesisStreamsSink',
    'KinesisStreamsSinkBuilder',
    'KinesisFirehoseSink',
    'KinesisFirehoseSinkBuilder'
]


# ---- KinesisSource ----


class KinesisShardAssigner(object):
    """
    Utility to map Kinesis shards to Flink subtask indices. Users can provide a Java
    KinesisShardAssigner in Python if they want to provide custom shared assigner.
    """
    def __init__(self, j_kinesis_shard_assigner):
        self._j_kinesis_shard_assigner = j_kinesis_shard_assigner

    @staticmethod
    def uniform_shard_assigner() -> 'KinesisShardAssigner':
        """
        A KinesisShardAssigner that maps Kinesis shard hash-key ranges to Flink subtasks.
        It creates a more uniform distribution of shards across subtasks when the
        Kinesis records in the stream have hash keys that are uniformly distributed over all
        possible hash keys, which is the case if records have randomly-generated partition keys.
        (This is the same assumption made if you use the Kinesis UpdateShardCount operation with
        UNIFORM_SCALING.)
        """
        return KinesisShardAssigner(get_gateway().jvm.org.apache.flink.connector.kinesis.source.
                                    enumerator.assigner.ShardAssignerFactory.uniformShardAssigner())


class KinesisDeserializationSchema(object):
    """
    This is a deserialization schema specific for the Flink Kinesis Consumer. Different from the
    basic DeserializationSchema, this schema offers additional Kinesis-specific information about
    the record that may be useful to the user application.
    """

    def __init__(self, j_kinesis_deserialization_schema):
        self._j_kinesis_deserialization_schema = j_kinesis_deserialization_schema


class KinesisStreamsSource(Source):
    """
    A Kinesis Data Streams (KDS) Source that reads from a single Kinesis stream using the
    FLIP-27 Source API.

    The source is an exactly-once parallel streaming data source that subscribes to a single
    AWS Kinesis Data stream. It handles resharding transparently and maintains order within
    a specific Kinesis partitionId.

    The source will discover shards and start reading from each eligible shard in parallel,
    depending on the parallelism of the operator. It also transparently handles discovery of
    new shards if resharding occurs while the job is running.
    """

    def __init__(self, j_kinesis_streams_source):
        super().__init__(j_kinesis_streams_source)

    @staticmethod
    def builder() -> 'KinesisStreamsSourceBuilder':
        """
        Get a KinesisStreamsSourceBuilder to build a :class:`KinesisStreamsSource`.

        :return: a Kinesis source builder.
        """
        return KinesisStreamsSourceBuilder()


class KinesisStreamsSourceBuilder(object):
    """
    Builder to construct KinesisStreamsSource.

    The following example shows the minimum setup to create a KinesisStreamsSource that reads
    String values from a Kinesis Data Streams stream.

    Example:
    ::

        >>> from pyflink.common.serialization import SimpleStringSchema
        >>> from pyflink.common import Configuration
        >>> source_config = Configuration()
        >>> source = KinesisStreamsSource.builder() \\
        ...     .set_stream_arn("arn:aws:kinesis:us-east-1:123456789012:stream/test-stream") \\
        ...     .set_deserialization_schema(SimpleStringSchema()) \\
        ...     .set_source_config(source_config) \\
        ...     .set_kinesis_shard_assigner(KinesisShardAssigner.uniform_shard_assigner()) \\
        ...     .build()

    If the following parameters are not set in this builder, the following defaults will be used:

    - kinesisShardAssigner will be UniformShardAssigner
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm.org.apache.flink.connector.kinesis.source. \
            KinesisStreamsSource.builder()

    def set_stream_arn(self, stream_arn: str) -> 'KinesisStreamsSourceBuilder':
        """
        Sets the ARN of the Kinesis Data Streams stream to read from. There is no default for
        this parameter, therefore, this must be provided at source creation time otherwise the
        build will fail.
        """
        self._j_builder.setStreamArn(stream_arn)
        return self

    def set_deserialization_schema(
            self, deserialization_schema: DeserializationSchema) -> 'KinesisStreamsSourceBuilder':
        """
        Sets the DeserializationSchema for deserializing records read from Kinesis.
        """
        self._j_builder.setDeserializationSchema(deserialization_schema._j_deserialization_schema)
        return self

    def set_kinesis_shard_assigner(
            self, shard_assigner: KinesisShardAssigner) -> 'KinesisStreamsSourceBuilder':
        """
        Sets the KinesisShardAssigner that controls the mapping of Kinesis shards to Flink
        subtask indices. By default, UniformShardAssigner is used.
        """
        self._j_builder.setKinesisShardAssigner(shard_assigner._j_kinesis_shard_assigner)
        return self

    def set_source_config(self, source_config) -> 'KinesisStreamsSourceBuilder':
        """
        Sets the configuration for the KinesisStreamsSource. Configuration keys can be taken
        from AWSConfigOptions (AWS-specific configuration) and KinesisSourceConfigOptions
        (Kinesis Source configuration).
        """
        self._j_builder.setSourceConfig(source_config._j_configuration)
        return self

    def build(self) -> 'KinesisStreamsSource':
        """
        Build the KinesisStreamsSource.
        """
        return KinesisStreamsSource(self._j_builder.build())


# ---- KinesisSink ----

class PartitionKeyGenerator(object):
    """
    This is a generator convert from an input element to the partition key, a string.
    """
    def __init__(self, j_partition_key_generator):
        self._j_partition_key_generator = j_partition_key_generator

    @staticmethod
    def fixed() -> 'PartitionKeyGenerator':
        """
        A partitioner ensuring that each internal Flink partition ends up in the same Kinesis
        partition. This is achieved by using the index of the producer task as a PartitionKey.
        """
        return PartitionKeyGenerator(get_gateway().jvm.org.apache.flink.connector.kinesis.table.
                                     FixedKinesisPartitionKeyGenerator())

    @staticmethod
    def random() -> 'PartitionKeyGenerator':
        """
        A PartitionKeyGenerator that maps an arbitrary input element to a random partition ID.
        """
        return PartitionKeyGenerator(get_gateway().jvm.org.apache.flink.connector.kinesis.table.
                                     RandomKinesisPartitionKeyGenerator())


class KinesisStreamsSink(Sink):
    """
    A Kinesis Data Streams (KDS) Sink that performs async requests against a destination stream
    using the buffering protocol.

    The sink internally uses a software.amazon.awssdk.services.kinesis.KinesisAsyncClient to
    communicate with the AWS endpoint.

    The behaviour of the buffering may be specified by providing configuration during the sink
    build time.

    - maxBatchSize: the maximum size of a batch of entries that may be sent to KDS
    - maxInFlightRequests: the maximum number of in flight requests that may exist, if any more in
        flight requests need to be initiated once the maximum has been reached, then it will be
        blocked until some have completed
    - maxBufferedRequests: the maximum number of elements held in the buffer, requests to add
        elements will be blocked while the number of elements in the buffer is at the maximum
    - maxBatchSizeInBytes: the maximum size of a batch of entries that may be sent to KDS
        measured in bytes
    - maxTimeInBufferMS: the maximum amount of time an entry is allowed to live in the buffer,
        if any element reaches this age, the entire buffer will be flushed immediately
    - maxRecordSizeInBytes: the maximum size of a record the sink will accept into the buffer,
        a record of size larger than this will be rejected when passed to the sink
    - failOnError: when an exception is encountered while persisting to Kinesis Data Streams,
        the job will fail immediately if failOnError is set
    """

    def __init__(self, j_kinesis_streams_sink):
        super(KinesisStreamsSink, self).__init__(sink=j_kinesis_streams_sink)

    @staticmethod
    def builder() -> 'KinesisStreamsSinkBuilder':
        return KinesisStreamsSinkBuilder()


class KinesisStreamsSinkBuilder(object):
    """
    Builder to construct KinesisStreamsSink.

    The following example shows the minimum setup to create a KinesisStreamsSink that writes String
    values to a Kinesis Data Streams stream named your_stream_here.

    Example:
    ::

        >>> from pyflink.common.serialization import SimpleStringSchema
        >>> sink_properties = {"aws.region": "eu-west-1"}
        >>> sink = KinesisStreamsSink.builder() \\
        ...     .set_kinesis_client_properties(sink_properties) \\
        ...     .set_stream_name("your_stream_name") \\
        ...     .set_serialization_schema(SimpleStringSchema()) \\
        ...     .set_partition_key_generator(PartitionKeyGenerator.random()) \\
        ...     .build()

    If the following parameters are not set in this builder, the following defaults will be used:

        - maxBatchSize will be 500
        - maxInFlightRequests will be 50
        - maxBufferedRequests will be 10000
        - maxBatchSizeInBytes will be 5 MB i.e. 5 * 1024 * 1024
        - maxTimeInBufferMS will be 5000ms
        - maxRecordSizeInBytes will be 1 MB i.e. 1 * 1024 * 1024
        - failOnError will be false
    """

    def __init__(self):
        JKinesisStreamsSink = get_gateway().jvm.org.apache.flink.connector.kinesis.sink.\
            KinesisStreamsSink
        self._j_kinesis_sink_builder = JKinesisStreamsSink.builder()

    def set_stream_name(self, stream_name: Union[str, List[str]]) -> 'KinesisStreamsSinkBuilder':
        """
        Sets the name of the KDS stream that the sink will connect to. There is no default for this
        parameter, therefore, this must be provided at sink creation time otherwise the build will
        fail.
        """
        self._j_kinesis_sink_builder.setStreamName(stream_name)
        return self

    def set_serialization_schema(self, serialization_schema: SerializationSchema) \
            -> 'KinesisStreamsSinkBuilder':
        """
        Sets the SerializationSchema of the KinesisSinkBuilder.
        """
        self._j_kinesis_sink_builder.setSerializationSchema(
            serialization_schema._j_serialization_schema)
        return self

    def set_partition_key_generator(self, partition_key_generator: PartitionKeyGenerator) \
            -> 'KinesisStreamsSinkBuilder':
        """
        Sets the PartitionKeyGenerator of the KinesisSinkBuilder.
        """
        self._j_kinesis_sink_builder.setPartitionKeyGenerator(
            partition_key_generator._j_partition_key_generator)
        return self

    def set_fail_on_error(self, fail_on_error: bool) -> 'KinesisStreamsSinkBuilder':
        """
        Sets the failOnError of the KinesisSinkBuilder. If failOnError is on, then a runtime
        exception will be raised. Otherwise, those records will be requested in the buffer for
        retry.
        """
        self._j_kinesis_sink_builder.setFailOnError(fail_on_error)
        return self

    def set_kinesis_client_properties(self, kinesis_client_properties: Dict) \
            -> 'KinesisStreamsSinkBuilder':
        """
        Sets the kinesisClientProperties of the KinesisSinkBuilder.
        """
        j_properties = get_gateway().jvm.java.util.Properties()
        for key, value in kinesis_client_properties.items():
            j_properties.setProperty(key, value)
        self._j_kinesis_sink_builder.setKinesisClientProperties(j_properties)
        return self

    def set_max_batch_size(self, max_batch_size: int) -> 'KinesisStreamsSinkBuilder':
        """
        Maximum number of elements that may be passed in a list to be written downstream.
        """
        self._j_kinesis_sink_builder.setMaxBatchSize(max_batch_size)
        return self

    def set_max_in_flight_requests(self, max_in_flight_requests: int) \
            -> 'KinesisStreamsSinkBuilder':
        """
        Maximum number of uncompleted calls to submitRequestEntries that the SinkWriter will allow
        at any given point. Once this point has reached, writes and callbacks to add elements to
        the buffer may block until one or more requests to submitRequestEntries completes.
        """
        self._j_kinesis_sink_builder.setMaxInFlightRequests(max_in_flight_requests)
        return self

    def set_max_buffered_requests(self, max_buffered_requests: int) -> 'KinesisStreamsSinkBuilder':
        """
        The maximum buffer length. Callbacks to add elements to the buffer and calls to write will
        block if this length has been reached and will only unblock if elements from the buffer have
        been removed for flushing.
        """
        self._j_kinesis_sink_builder.setMaxBufferedRequests(max_buffered_requests)
        return self

    def set_max_batch_size_in_bytes(self, max_batch_size_in_bytes: int) \
            -> 'KinesisStreamsSinkBuilder':
        """
        The flush will be attempted if the most recent call to write introduces an element to the
        buffer such that the total size of the buffer is greater than or equal to this threshold
        value. If this happens, the maximum number of elements from the head of the buffer will be
        selected, that is smaller than maxBatchSizeInBytes in size will be flushed.
        """
        self._j_kinesis_sink_builder.setMaxBatchSizeInBytes(max_batch_size_in_bytes)
        return self

    def set_max_time_in_buffer_ms(self, max_time_in_buffer_ms: int) -> 'KinesisStreamsSinkBuilder':
        """
        The maximum amount of time an element may remain in the buffer. In most cases elements are
        flushed as a result of the batch size (in bytes or number) being reached or during a
        snapshot. However, there are scenarios where an element may remain in the buffer forever or
        a long period of time. To mitigate this, a timer is constantly active in the buffer such
        that: while the buffer is not empty, it will flush every maxTimeInBufferMS milliseconds.
        """
        self._j_kinesis_sink_builder.setMaxTimeInBufferMS(max_time_in_buffer_ms)
        return self

    def set_max_record_size_in_bytes(self, max_record_size_in_bytes: int) \
            -> 'KinesisStreamsSinkBuilder':
        """
        The maximum size of each records in bytes. If a record larger than this is passed to the
        sink, it will throw an IllegalArgumentException.
        """
        self._j_kinesis_sink_builder.setMaxRecordSizeInBytes(max_record_size_in_bytes)
        return self

    def build(self) -> 'KinesisStreamsSink':
        """
        Build thd KinesisStreamsSink.
        """
        return KinesisStreamsSink(self._j_kinesis_sink_builder.build())


class KinesisFirehoseSink(Sink):
    """
    A Kinesis Data Firehose (KDF) Sink that performs async requests against a destination delivery
    stream using the buffering protocol.
    """
    def __init__(self, j_kinesis_firehose_sink):
        super(KinesisFirehoseSink, self).__init__(sink=j_kinesis_firehose_sink)

    @staticmethod
    def builder() -> 'KinesisFirehoseSinkBuilder':
        return KinesisFirehoseSinkBuilder()


class KinesisFirehoseSinkBuilder(object):
    """
    Builder to construct KinesisFirehoseSink.

    The following example shows the minimum setup to create a KinesisFirehoseSink that writes
    String values to a Kinesis Data Firehose delivery stream named delivery-stream-name.

    Example:
    ::

        >>> from pyflink.common.serialization import SimpleStringSchema
        >>> sink_properties = {"aws.region": "eu-west-1"}
        >>> sink = KinesisFirehoseSink.builder() \\
        ...     .set_firehose_client_properties(sink_properties) \\
        ...     .set_delivery_stream_name("delivery-stream-name") \\
        ...     .set_serialization_schema(SimpleStringSchema()) \\
        ...     .set_max_batch_size(20) \\
        ...     .build()

    If the following parameters are not set in this builder, the following defaults will be used:
    - maxBatchSize will be 500
    - maxInFlightRequests will be 50
    - maxBufferedRequests will be 10000
    - maxBatchSizeInBytes will be 4 MB i.e. 4 * 1024 * 1024
    - maxTimeInBufferMS will be 5000ms
    - maxRecordSizeInBytes will be 1000 KB i.e. 1000 * 1024
    - failOnError will be false
    """
    def __init__(self):
        JKinesisFirehoseSink = get_gateway().jvm.org.apache.flink.connector.firehose.sink. \
            KinesisFirehoseSink
        self._j_kinesis_sink_builder = JKinesisFirehoseSink.builder()

    def set_delivery_stream_name(self, delivery_stream_name: str) -> 'KinesisFirehoseSinkBuilder':
        """
        Sets the name of the KDF delivery stream that the sink will connect to. There is no default
        for this parameter, therefore, this must be provided at sink creation time otherwise the
        build will fail.
        """
        self._j_kinesis_sink_builder.setDeliveryStreamName(delivery_stream_name)
        return self

    def set_serialization_schema(self, serialization_schema: SerializationSchema) \
            -> 'KinesisFirehoseSinkBuilder':
        """
        Allows the user to specify a serialization schema to serialize each record to persist to
        Firehose.
        """
        self._j_kinesis_sink_builder.setSerializationSchema(
            serialization_schema._j_serialization_schema)
        return self

    def set_fail_on_error(self, fail_on_error: bool) -> 'KinesisFirehoseSinkBuilder':
        """
        If writing to Kinesis Data Firehose results in a partial or full failure being returned,
        the job will fail
        """
        self._j_kinesis_sink_builder.setFailOnError(fail_on_error)
        return self

    def set_firehose_client_properties(self, firehose_client_properties: Dict) \
            -> 'KinesisFirehoseSinkBuilder':
        """
        A set of properties used by the sink to create the firehose client. This may be used to set
        the aws region, credentials etc. See the docs for usage and syntax.
        """
        j_properties = get_gateway().jvm.java.util.Properties()
        for key, value in firehose_client_properties.items():
            j_properties.setProperty(key, value)
        self._j_kinesis_sink_builder.setFirehoseClientProperties(j_properties)
        return self

    def set_max_batch_size(self, max_batch_size: int) -> 'KinesisFirehoseSinkBuilder':
        """
        Maximum number of elements that may be passed in a list to be written downstream.
        """
        self._j_kinesis_sink_builder.setMaxBatchSize(max_batch_size)
        return self

    def set_max_in_flight_requests(self, max_in_flight_requests: int) \
            -> 'KinesisFirehoseSinkBuilder':
        """
        Maximum number of uncompleted calls to submitRequestEntries that the SinkWriter will allow
        at any given point. Once this point has reached, writes and callbacks to add elements to
        the buffer may block until one or more requests to submitRequestEntries completes.
        """
        self._j_kinesis_sink_builder.setMaxInFlightRequests(max_in_flight_requests)
        return self

    def set_max_buffered_requests(self, max_buffered_requests: int) -> 'KinesisFirehoseSinkBuilder':
        """
        The maximum buffer length. Callbacks to add elements to the buffer and calls to write will
        block if this length has been reached and will only unblock if elements from the buffer have
        been removed for flushing.
        """
        self._j_kinesis_sink_builder.setMaxBufferedRequests(max_buffered_requests)
        return self

    def set_max_batch_size_in_bytes(self, max_batch_size_in_bytes: int) \
            -> 'KinesisFirehoseSinkBuilder':
        """
        The flush will be attempted if the most recent call to write introduces an element to the
        buffer such that the total size of the buffer is greater than or equal to this threshold
        value. If this happens, the maximum number of elements from the head of the buffer will be
        selected, that is smaller than maxBatchSizeInBytes in size will be flushed.
        """
        self._j_kinesis_sink_builder.setMaxBatchSizeInBytes(max_batch_size_in_bytes)
        return self

    def set_max_time_in_buffer_ms(self, max_time_in_buffer_ms: int) -> 'KinesisFirehoseSinkBuilder':
        """
        The maximum amount of time an element may remain in the buffer. In most cases elements are
        flushed as a result of the batch size (in bytes or number) being reached or during a
        snapshot. However, there are scenarios where an element may remain in the buffer forever or
        a long period of time. To mitigate this, a timer is constantly active in the buffer such
        that: while the buffer is not empty, it will flush every maxTimeInBufferMS milliseconds.
        """
        self._j_kinesis_sink_builder.setMaxTimeInBufferMS(max_time_in_buffer_ms)
        return self

    def set_max_record_size_in_bytes(self, max_record_size_in_bytes: int) \
            -> 'KinesisFirehoseSinkBuilder':
        """
        The maximum size of each records in bytes. If a record larger than this is passed to the
        sink, it will throw an IllegalArgumentException.
        """
        self._j_kinesis_sink_builder.setMaxRecordSizeInBytes(max_record_size_in_bytes)
        return self

    def build(self) -> 'KinesisFirehoseSink':
        """
        Build thd KinesisFirehoseSink.
        """
        return KinesisFirehoseSink(self._j_kinesis_sink_builder.build())
