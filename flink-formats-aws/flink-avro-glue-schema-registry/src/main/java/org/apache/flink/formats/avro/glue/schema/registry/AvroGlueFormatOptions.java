/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.amazonaws.services.schemaregistry.utils.AvroRecordType;

/**
 * Avro-specific configuration options for the AWS Glue Schema Registry Avro format factory.
 *
 * <p>Shared options (aws.region, registry.name, schema.name, etc.) are defined in {@link
 * GlueFormatOptions}.
 */
@PublicEvolving
public class AvroGlueFormatOptions extends GlueFormatOptions {

    public static final ConfigOption<AvroRecordType> SCHEMA_TYPE =
            ConfigOptions.key("schema.type")
                    .enumType(AvroRecordType.class)
                    .defaultValue(AvroRecordType.GENERIC_RECORD)
                    .withDescription("Avro record type. Defaults to GENERIC_RECORD.");

    public static final ConfigOption<String> AVRO_NAMESPACE =
            ConfigOptions.key("avro.namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Override the namespace in the auto-generated Avro schema. "
                                    + "Use this to match schemas already registered in GSR with a different namespace.");

    public static final ConfigOption<String> AVRO_RECORD_NAME =
            ConfigOptions.key("avro.record-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Override the record name in the auto-generated Avro schema. "
                                    + "Use this to match schemas already registered in GSR with a different record name.");

    public static final ConfigOption<Boolean> SCHEMA_FETCH_FROM_REGISTRY =
            ConfigOptions.key("schema.fetchFromRegistry")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to fetch the schema from GSR instead of using the auto-generated one. "
                                    + "Defaults to false.");

    private AvroGlueFormatOptions() {}
}
