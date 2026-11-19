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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.formats.avro.glue.schema.registry.GlueFormatOptions;

/**
 * Protobuf-specific configuration options for the AWS Glue Schema Registry Protobuf format factory.
 *
 * <p>Shared options (aws.region, registry.name, schema.name, etc.) are inherited from {@link
 * GlueFormatOptions}. Currently no additional Protobuf-specific options are needed.
 */
@PublicEvolving
public class ProtobufGlueFormatOptions extends GlueFormatOptions {

    // No Protobuf-specific options needed at this time.
    // All shared GSR options are inherited from GlueFormatOptions.
}
