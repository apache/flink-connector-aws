package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import com.amazonaws.arn.Arn;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * This util class exists to convert Stream name into Stream ARN and vice versa.
 */
public class StreamConverterUtil {

    public static String getStreamName(final Arn streamArn) {
        return streamArn.getResource().getResource();
    }

    public static String getStreamName(final String streamArn) {
        return Arn.fromString(streamArn).getResource().getResource();
    }

    public static String getStreamArn(
        final String streamNameOrArn, final Properties configProps)
        throws ExecutionException, InterruptedException {
        try {
            return Arn.fromString(streamNameOrArn).toString();
        } catch (IllegalArgumentException e) {
            KinesisProxyInterface kinesis = KinesisProxy.create(configProps);
            return kinesis
                .describeStream(streamNameOrArn, null)
                .getStreamDescription()
                .getStreamARN();
        }
    }
}
