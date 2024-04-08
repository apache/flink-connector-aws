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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.dynamodb.sink.DynamoDbTypeInformedElementConverter;

import java.io.Serializable;

/** A test POJO for use with {@link DynamoDbTypeInformedElementConverter}. */
public class ComplexPayload implements Serializable {
    private static final long serialVersionUID = 233624606545704853L;

    private String stringField;
    private String[] stringArrayField;
    private int[] intArrayField;
    private InnerPayload innerPayload;
    private Tuple2<Integer, String> tupleField;

    public ComplexPayload() {}

    public ComplexPayload(
            String stringField,
            String[] stringArrayField,
            int[] intArrayField,
            InnerPayload innerPayload,
            Tuple2<Integer, String> tupleField) {
        this.stringField = stringField;
        this.stringArrayField = stringArrayField;
        this.intArrayField = intArrayField;
        this.innerPayload = innerPayload;
        this.tupleField = tupleField;
    }

    public String getStringField() {
        return stringField;
    }

    public String[] getStringArrayField() {
        return stringArrayField;
    }

    public int[] getIntArrayField() {
        return intArrayField;
    }

    public InnerPayload getInnerPayload() {
        return innerPayload;
    }

    public Tuple2<Integer, String> getTupleField() {
        return tupleField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public void setStringArrayField(String[] stringArrayField) {
        this.stringArrayField = stringArrayField;
    }

    public void setIntArrayField(int[] intArrayField) {
        this.intArrayField = intArrayField;
    }

    public void setInnerPayload(InnerPayload innerPayload) {
        this.innerPayload = innerPayload;
    }

    public void setTupleField(Tuple2<Integer, String> tupleField) {
        this.tupleField = tupleField;
    }

    /** A test POJO for use as InnerPayload for {@link ComplexPayload}. */
    public static class InnerPayload implements Serializable {
        private static final long serialVersionUID = 3986298180012117883L;

        private boolean primitiveBooleanField;
        private byte[] byteArrayField;

        public InnerPayload() {}

        public InnerPayload(boolean primitiveBooleanField, byte[] byteArrayField) {
            this.primitiveBooleanField = primitiveBooleanField;
            this.byteArrayField = byteArrayField;
        }

        public boolean getPrimitiveBooleanField() {
            return primitiveBooleanField;
        }

        public byte[] getByteArrayField() {
            return byteArrayField;
        }

        public void setPrimitiveBooleanField(boolean primitiveBooleanField) {
            this.primitiveBooleanField = primitiveBooleanField;
        }

        public void setByteArrayField(byte[] byteArrayField) {
            this.byteArrayField = byteArrayField;
        }
    }
}
