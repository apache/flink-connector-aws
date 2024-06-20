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

package org.apache.flink.connector.dynamodb.source.serialization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** Deserializer required for decrypting a DynamoDB object to JSON format. */
public class RecordObjectMapper extends ObjectMapper {
    public static final String L = "L";
    public static final String M = "M";
    public static final String BS = "BS";
    public static final String NS = "NS";
    public static final String SS = "SS";
    public static final String BOOL = "BOOL";
    public static final String NULL = "NULL";
    public static final String B = "B";
    public static final String N = "N";
    public static final String S = "S";
    public static final String OLD_IMAGE = "OldImage";
    public static final String NEW_IMAGE = "NewImage";
    public static final String STREAM_VIEW_TYPE = "StreamViewType";
    public static final String OPERATION_TYPE = "OperationType";
    public static final String SEQUENCE_NUMBER = "SequenceNumber";
    public static final String SIZE_BYTES = "SizeBytes";
    public static final String KEYS = "Keys";
    public static final String AWS_REGION = "awsRegion";
    public static final String DYNAMODB = "dynamodb";
    public static final String USER_IDENTITY = "userIdentity";
    public static final String EVENT_ID = "eventID";
    public static final String EVENT_NAME = "eventName";
    public static final String EVENT_SOURCE = "eventSource";
    public static final String EVENT_VERSION = "eventVersion";
    public static final String APPROXIMATE_CREATION_DATE_TIME = "ApproximateCreationDateTime";

    private static final String MODULE = "custom";

    public RecordObjectMapper() {
        super();
        SimpleModule module = new SimpleModule(MODULE, Version.unknownVersion());

        // Deal with (de)serializing of byte[].
        module.addSerializer(ByteBuffer.class, new ByteBufferSerializer());
        module.addDeserializer(ByteBuffer.class, new ByteBufferDeserializer());

        // Deal with (de)serializing of Date
        module.addSerializer(Date.class, DateSerializer.instance);
        module.addDeserializer(Date.class, new DateDeserializer());

        // Deal with (de)serializing of Instant
        module.addSerializer(Instant.class, InstantSerializer.INSTANCE);
        module.addDeserializer(Instant.class, InstantDeserializer.INSTANT);

        // Don't serialize things that are null
        this.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        this.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);

        this.addMixIn(Record.class, DynamoRecordMixIn.class);
        this.addMixIn(StreamRecord.class, StreamRecordMixIn.class);
        this.addMixIn(AttributeValue.class, AttributeValueMixIn.class);

        this.registerModule(module);
    }

    /*
     * Serializers and Deserializer classes
     */
    private static class ByteBufferSerializer extends JsonSerializer<ByteBuffer> {
        @Override
        public void serialize(ByteBuffer value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            // value is never null, according to JsonSerializer contract
            jgen.writeBinary(value.array());
        }
    }

    private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
        @Override
        public ByteBuffer deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            // never called for null literal, according to JsonDeserializer contract
            return ByteBuffer.wrap(jp.getBinaryValue());
        }
    }

    @JsonDeserialize(builder = Record.Builder.class)
    private abstract static class DynamoRecordMixIn {
        @JsonProperty(EVENT_ID)
        public abstract String getEventID();

        @JsonProperty(EVENT_ID)
        public abstract void setEventID(String eventID);

        @JsonProperty(EVENT_NAME)
        public abstract String getEventName();

        @JsonProperty(EVENT_NAME)
        public abstract void setEventName(String eventName);

        @JsonProperty(EVENT_VERSION)
        public abstract String getEventVersion();

        @JsonProperty(EVENT_VERSION)
        public abstract void setEventVersion(String eventVersion);

        @JsonProperty(EVENT_SOURCE)
        public abstract String getEventSource();

        @JsonProperty(EVENT_SOURCE)
        public abstract void setEventSource(String eventSource);

        @JsonProperty(AWS_REGION)
        public abstract String getAwsRegion();

        @JsonProperty(AWS_REGION)
        public abstract void setAwsRegion(String awsRegion);

        @JsonProperty(DYNAMODB)
        public abstract StreamRecord getDynamodb();

        @JsonProperty(DYNAMODB)
        public abstract void setDynamodb(StreamRecord dynamodb);

        @JsonIgnore
        public abstract Identity getUserIdentity();

        //@JsonProperty(USER_IDENTITY)
        @JsonIgnore
        public abstract void setUserIdentity(Identity userIdentity);
    }

    private abstract static class StreamRecordMixIn {
        @JsonProperty(SIZE_BYTES)
        public abstract Long getSizeBytes();

        @JsonProperty(SIZE_BYTES)
        public abstract void setSizeBytes(Long sizeBytes);

        @JsonProperty(SEQUENCE_NUMBER)
        public abstract String getSequenceNumber();

        @JsonProperty(SEQUENCE_NUMBER)
        public abstract void setSequenceNumber(String sequenceNumber);

        @JsonProperty(STREAM_VIEW_TYPE)
        public abstract StreamViewType getStreamViewTypeEnum();

        @JsonProperty(STREAM_VIEW_TYPE)
        public abstract void setStreamViewType(StreamViewType streamViewType);

        @JsonProperty(KEYS)
        public abstract Map<String, AttributeValue> getKeys();

        @JsonProperty(KEYS)
        public abstract void setKeys(Map<String, AttributeValue> keys);

        @JsonProperty(NEW_IMAGE)
        public abstract Map<String, AttributeValue> getNewImage();

        @JsonProperty(NEW_IMAGE)
        public abstract void setNewImage(Map<String, AttributeValue> newImage);

        @JsonProperty(OLD_IMAGE)
        public abstract Map<String, AttributeValue> getOldImage();

        @JsonProperty(OLD_IMAGE)
        public abstract void setOldImage(Map<String, AttributeValue> oldImage);

        @JsonProperty(APPROXIMATE_CREATION_DATE_TIME)
        public abstract Instant getApproximateCreationDateTime();

        @JsonProperty(APPROXIMATE_CREATION_DATE_TIME)
        public abstract void setApproximateCreationDateTime(Instant approximateCreationDateTime);
    }

    private abstract static class AttributeValueMixIn {
        @JsonProperty(S)
        public abstract String getS();

        @JsonProperty(S)
        public abstract void setS(String s);

        @JsonProperty(N)
        public abstract String getN();

        @JsonProperty(N)
        public abstract void setN(String n);

        @JsonProperty(B)
        public abstract ByteBuffer getB();

        @JsonProperty(B)
        public abstract void setB(ByteBuffer b);

        @JsonProperty(NULL)
        public abstract Boolean isNULL();

        @JsonProperty(NULL)
        public abstract void setNULL(Boolean nU);

        @JsonProperty(BOOL)
        public abstract Boolean getBOOL();

        @JsonProperty(BOOL)
        public abstract void setBOOL(Boolean bO);

        @JsonProperty(SS)
        public abstract List<String> getSS();

        @JsonProperty(SS)
        public abstract void setSS(List<String> sS);

        @JsonProperty(NS)
        public abstract List<String> getNS();

        @JsonProperty(NS)
        public abstract void setNS(List<String> nS);

        @JsonProperty(BS)
        public abstract List<String> getBS();

        @JsonProperty(BS)
        public abstract void setBS(List<String> bS);

        @JsonProperty(M)
        public abstract Map<String, AttributeValue> getM();

        @JsonProperty(M)
        public abstract void setM(Map<String, AttributeValue> val);

        @JsonProperty(L)
        public abstract List<AttributeValue> getL();

        @JsonProperty(L)
        public abstract void setL(List<AttributeValue> val);
    }
}
