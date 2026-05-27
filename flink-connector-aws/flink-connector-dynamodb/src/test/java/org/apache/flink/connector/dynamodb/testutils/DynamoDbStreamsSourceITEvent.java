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

package org.apache.flink.connector.dynamodb.testutils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

/** Helper POJO used for DDB Streams Source IT Case. */
public class DynamoDbStreamsSourceITEvent {

    private String shardId;
    private String eventType;
    private String sequenceNumber;
    private Map<String, AttributeValue> oldImage;
    private Map<String, AttributeValue> newImage;

    public DynamoDbStreamsSourceITEvent() {}

    public DynamoDbStreamsSourceITEvent(
            String shardId,
            String eventType,
            String sequenceNumber,
            Map<String, AttributeValue> oldImage,
            Map<String, AttributeValue> newImage) {
        this.shardId = shardId;
        this.eventType = eventType;
        this.sequenceNumber = sequenceNumber;
        this.oldImage = oldImage;
        this.newImage = newImage;
    }

    public String getShardId() {
        return shardId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public Map<String, AttributeValue> getOldImage() {
        return oldImage;
    }

    public Map<String, AttributeValue> getNewImage() {
        return newImage;
    }
}
