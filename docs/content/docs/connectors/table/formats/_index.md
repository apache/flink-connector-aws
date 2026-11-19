---
title: "Formats"
weight: 10
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Formats

This section describes the format factories available for use with the AWS connectors (Kinesis, Firehose, etc.) in Flink SQL.

## AWS Glue Schema Registry Formats

The following formats integrate with [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) for schema management:

- [Avro (avro-glue)]({{< ref "docs/connectors/table/formats/avro-glue" >}})
- [JSON (json-glue)]({{< ref "docs/connectors/table/formats/json-glue" >}})
- [Protobuf (protobuf-glue)]({{< ref "docs/connectors/table/formats/protobuf-glue" >}})
