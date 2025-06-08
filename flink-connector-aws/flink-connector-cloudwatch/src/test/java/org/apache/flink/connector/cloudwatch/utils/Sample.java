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

package org.apache.flink.connector.cloudwatch.utils;

import org.apache.flink.connector.cloudwatch.sink.MetricWriteRequestElementConverter;

import java.util.Map;

/** A test POJO for use with {@link MetricWriteRequestElementConverter}. */
public class Sample {

    private final String name;
    private final Map<String, String> label;
    private final double value;

    public String getName() {
        return name;
    }

    public Map<String, String> getLabel() {
        return label;
    }

    public double getValue() {
        return value;
    }

    public Sample(String name, Map<String, String> label, double value) {
        this.name = name;
        this.label = label;
        this.value = value;
    }
}
