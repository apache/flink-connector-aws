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

package org.apache.flink.connector.dynamodb.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization Utils for DynamoDb {@link AttributeValue}. This class is currently not
 * serializable, see <a href="https://github.com/aws/aws-sdk-java-v2/issues/3143">open issue</a>
 */
@Internal
public class DynamoDbSerializationUtil {

    public static void serializeWriteRequest(
            DynamoDbWriteRequest dynamoDbWriteRequest, DataOutputStream out) throws IOException {
        out.writeByte(dynamoDbWriteRequest.getType().toByteValue());
        serializeItem(dynamoDbWriteRequest.getItem(), out);
        serializeNullableString(dynamoDbWriteRequest.getUpdateExpression(), out);
        serializeNullableString(dynamoDbWriteRequest.getConditionExpression(), out);
        serializeNullableStringMap(dynamoDbWriteRequest.getExpressionAttributeNames(), out);
        serializeNullableAttributeValueMap(
                dynamoDbWriteRequest.getExpressionAttributeValues(), out);
    }

    public static DynamoDbWriteRequest deserializeWriteRequest(DataInputStream in)
            throws IOException {
        return deserializeWriteRequest(in, 2);
    }

    public static DynamoDbWriteRequest deserializeWriteRequest(DataInputStream in, int version)
            throws IOException {
        int writeRequestType = in.read();
        DynamoDbWriteRequestType dynamoDbWriteRequestType =
                DynamoDbWriteRequestType.fromByteValue((byte) writeRequestType);
        Map<String, AttributeValue> item = deserializeItem(in);

        DynamoDbWriteRequest.Builder builder =
                DynamoDbWriteRequest.builder()
                        .setType(dynamoDbWriteRequestType)
                        .setItem(item);

        // Version 2 added expression fields for conditional and update writes
        if (version >= 2) {
            String updateExpression = deserializeNullableString(in);
            String conditionExpression = deserializeNullableString(in);
            Map<String, String> expressionAttributeNames = deserializeNullableStringMap(in);
            Map<String, AttributeValue> expressionAttributeValues =
                    deserializeNullableAttributeValueMap(in);
            if (updateExpression != null) {
                builder.setUpdateExpression(updateExpression);
            }
            if (conditionExpression != null) {
                builder.setConditionExpression(conditionExpression);
            }
            if (expressionAttributeNames != null) {
                builder.setExpressionAttributeNames(expressionAttributeNames);
            }
            if (expressionAttributeValues != null) {
                builder.setExpressionAttributeValues(expressionAttributeValues);
            }
        }

        return builder.build();
    }

    private static void serializeNullableString(String value, DataOutputStream out)
            throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    private static String deserializeNullableString(DataInputStream in) throws IOException {
        boolean present = in.readBoolean();
        return present ? in.readUTF() : null;
    }

    private static void serializeNullableStringMap(Map<String, String> map, DataOutputStream out)
            throws IOException {
        if (map == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }

    private static Map<String, String> deserializeNullableStringMap(DataInputStream in)
            throws IOException {
        boolean present = in.readBoolean();
        if (!present) {
            return null;
        }
        int size = in.readInt();
        Map<String, String> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(in.readUTF(), in.readUTF());
        }
        return map;
    }

    private static void serializeNullableAttributeValueMap(
            Map<String, AttributeValue> map, DataOutputStream out) throws IOException {
        if (map == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            serializeItem(map, out);
        }
    }

    private static Map<String, AttributeValue> deserializeNullableAttributeValueMap(
            DataInputStream in) throws IOException {
        boolean present = in.readBoolean();
        return present ? deserializeItem(in) : null;
    }

    private static void serializeItem(Map<String, AttributeValue> item, DataOutputStream out)
            throws IOException {
        out.writeInt(item.size());
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            out.writeUTF(entry.getKey());
            AttributeValue value = entry.getValue();
            serializeAttributeValue(value, out);
        }
    }

    private static void serializeAttributeValue(AttributeValue value, DataOutputStream out)
            throws IOException {
        if (value.nul() != null) {
            out.writeByte(DynamoDbType.NULL.toByteValue());
        } else if (value.bool() != null) {
            out.writeByte(DynamoDbType.BOOLEAN.toByteValue());
            out.writeBoolean(value.bool());
        } else if (value.s() != null) {
            out.writeByte(DynamoDbType.STRING.toByteValue());
            out.writeUTF(value.s());
        } else if (value.n() != null) {
            out.writeByte(DynamoDbType.NUMBER.toByteValue());
            out.writeUTF(value.n());
        } else if (value.b() != null) {
            byte[] bytes = value.b().asByteArrayUnsafe();
            out.writeByte(DynamoDbType.BINARY.toByteValue());
            out.writeInt(bytes.length);
            out.write(bytes);
        } else if (value.hasSs()) {
            out.writeByte(DynamoDbType.STRING_SET.toByteValue());
            out.writeInt(value.ss().size());
            for (String s : value.ss()) {
                out.writeUTF(s);
            }
        } else if (value.hasNs()) {
            out.writeByte(DynamoDbType.NUMBER_SET.toByteValue());
            out.writeInt(value.ns().size());
            for (String s : value.ns()) {
                out.writeUTF(s);
            }
        } else if (value.hasBs()) {
            out.writeByte(DynamoDbType.BINARY_SET.toByteValue());
            out.writeInt(value.bs().size());
            for (SdkBytes sdkBytes : value.bs()) {
                byte[] bytes = sdkBytes.asByteArrayUnsafe();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        } else if (value.hasL()) {
            out.writeByte(DynamoDbType.LIST.toByteValue());
            List<AttributeValue> l = value.l();
            out.writeInt(l.size());
            for (AttributeValue attributeValue : l) {
                serializeAttributeValue(attributeValue, out);
            }
        } else if (value.hasM()) {
            out.writeByte(DynamoDbType.MAP.toByteValue());
            Map<String, AttributeValue> m = value.m();
            serializeItem(m, out);
        } else {
            throw new IllegalArgumentException("Attribute value must not be empty: " + value);
        }
    }

    private static Map<String, AttributeValue> deserializeItem(DataInputStream in)
            throws IOException {
        int size = in.readInt();
        Map<String, AttributeValue> item = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            AttributeValue attributeValue = deserializeAttributeValue(in);
            item.put(key, attributeValue);
        }
        return item;
    }

    private static AttributeValue deserializeAttributeValue(DataInputStream in) throws IOException {
        int type = in.read();
        DynamoDbType dynamoDbType = DynamoDbType.fromByteValue((byte) type);
        return deserializeAttributeValue(dynamoDbType, in);
    }

    private static AttributeValue deserializeAttributeValue(
            DynamoDbType dynamoDbType, DataInputStream in) throws IOException {
        switch (dynamoDbType) {
            case NULL:
                return AttributeValue.builder().nul(true).build();
            case STRING:
                return AttributeValue.builder().s(in.readUTF()).build();
            case NUMBER:
                return AttributeValue.builder().n(in.readUTF()).build();
            case BOOLEAN:
                return AttributeValue.builder().bool(in.readBoolean()).build();
            case BINARY:
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.read(bytes);
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            case STRING_SET:
                int stringSetSize = in.readInt();
                List<String> stringSet = new ArrayList<>(stringSetSize);
                for (int i = 0; i < stringSetSize; i++) {
                    stringSet.add(in.readUTF());
                }
                return AttributeValue.builder().ss(stringSet).build();
            case NUMBER_SET:
                int numberSetSize = in.readInt();
                List<String> numberSet = new ArrayList<>(numberSetSize);
                for (int i = 0; i < numberSetSize; i++) {
                    numberSet.add(in.readUTF());
                }
                return AttributeValue.builder().ns(numberSet).build();
            case BINARY_SET:
                int binarySetSize = in.readInt();
                List<SdkBytes> byteSet = new ArrayList<>(binarySetSize);
                for (int i = 0; i < binarySetSize; i++) {
                    int byteLength = in.readInt();
                    byte[] bs = new byte[byteLength];
                    in.read(bs);
                    byteSet.add(SdkBytes.fromByteArray(bs));
                }
                return AttributeValue.builder().bs(byteSet).build();
            case LIST:
                int listSize = in.readInt();
                List<AttributeValue> list = new ArrayList<>(listSize);
                for (int i = 0; i < listSize; i++) {
                    list.add(deserializeAttributeValue(in));
                }
                return AttributeValue.builder().l(list).build();
            case MAP:
                return AttributeValue.builder().m(deserializeItem(in)).build();
            default:
                throw new IllegalArgumentException("Unknown DynamoDbType " + dynamoDbType);
        }
    }
}
