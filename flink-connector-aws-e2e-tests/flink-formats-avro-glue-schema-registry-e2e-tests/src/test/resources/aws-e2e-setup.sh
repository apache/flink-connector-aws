#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Setup and teardown AWS resources for avro-glue E2E tests.
#
# Usage:
#   ./aws-e2e-setup.sh setup   # Create Kinesis stream + GSR registry
#   ./aws-e2e-setup.sh teardown # Delete Kinesis stream + GSR registry
#
# Environment variables (set before running):
#   AWS_REGION          - AWS region (default: us-east-1)
#   KINESIS_STREAM      - Kinesis stream name (default: flink-avro-glue-e2e-test)
#   GSR_REGISTRY_NAME   - GSR registry name (default: flink-avro-glue-e2e-registry)
#   GSR_SCHEMA_NAME     - Schema name prefix (default: flink-avro-glue-e2e-schema)

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
KINESIS_STREAM="${KINESIS_STREAM:-flink-avro-glue-e2e-test}"
GSR_REGISTRY_NAME="${GSR_REGISTRY_NAME:-flink-avro-glue-e2e-registry}"
GSR_SCHEMA_NAME="${GSR_SCHEMA_NAME:-flink-avro-glue-e2e-schema}"

setup() {
    echo "=== Creating Kinesis stream: ${KINESIS_STREAM} ==="
    aws kinesis create-stream \
        --stream-name "${KINESIS_STREAM}" \
        --shard-count 1 \
        --region "${AWS_REGION}" 2>/dev/null || echo "Stream may already exist"

    echo "Waiting for stream to become ACTIVE..."
    aws kinesis wait stream-exists \
        --stream-name "${KINESIS_STREAM}" \
        --region "${AWS_REGION}"

    STREAM_ARN=$(aws kinesis describe-stream-summary \
        --stream-name "${KINESIS_STREAM}" \
        --region "${AWS_REGION}" \
        --query 'StreamDescriptionSummary.StreamARN' \
        --output text)

    echo "=== Creating GSR registry: ${GSR_REGISTRY_NAME} ==="
    aws glue create-registry \
        --registry-name "${GSR_REGISTRY_NAME}" \
        --region "${AWS_REGION}" 2>/dev/null || echo "Registry may already exist"

    echo ""
    echo "=== Setup complete ==="
    echo "Export these before running the E2E test:"
    echo "  export AWS_REGION=${AWS_REGION}"
    echo "  export KINESIS_STREAM_ARN=${STREAM_ARN}"
    echo "  export GSR_REGISTRY_NAME=${GSR_REGISTRY_NAME}"
    echo "  export GSR_SCHEMA_NAME=${GSR_SCHEMA_NAME}"
}

teardown() {
    echo "=== Deleting Kinesis stream: ${KINESIS_STREAM} ==="
    aws kinesis delete-stream \
        --stream-name "${KINESIS_STREAM}" \
        --enforce-consumer-deletion \
        --region "${AWS_REGION}" 2>/dev/null || echo "Stream may not exist"

    echo "=== Deleting GSR schemas in registry: ${GSR_REGISTRY_NAME} ==="
    # Delete all schemas in the registry before deleting the registry
    SCHEMAS=$(aws glue list-schemas \
        --registry-id RegistryName="${GSR_REGISTRY_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Schemas[].SchemaName' \
        --output text 2>/dev/null || echo "")

    for schema in ${SCHEMAS}; do
        echo "  Deleting schema: ${schema}"
        aws glue delete-schema \
            --schema-id SchemaName="${schema}",RegistryName="${GSR_REGISTRY_NAME}" \
            --region "${AWS_REGION}" 2>/dev/null || true
    done

    echo "=== Deleting GSR registry: ${GSR_REGISTRY_NAME} ==="
    aws glue delete-registry \
        --registry-id RegistryName="${GSR_REGISTRY_NAME}" \
        --region "${AWS_REGION}" 2>/dev/null || echo "Registry may not exist"

    echo "=== Teardown complete ==="
}

case "${1:-}" in
    setup)
        setup
        ;;
    teardown)
        teardown
        ;;
    *)
        echo "Usage: $0 {setup|teardown}"
        exit 1
        ;;
esac
