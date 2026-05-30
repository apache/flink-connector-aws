/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.constants;

import org.apache.flink.annotation.PublicEvolving;

/** Configuration keys for AWS Glue Data Catalog service usage. */
@PublicEvolving
public class AWSGlueConfigConstants {

    /**
     * Configure an alternative endpoint of the Glue service for GlueCatalog to access.
     *
     * <p>This could be used to use GlueCatalog with any glue-compatible metastore service that has
     * a different endpoint
     */
    public static final String GLUE_CATALOG_ENDPOINT = "aws.glue.endpoint";

    /**
     * The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
     * automatically uses the caller's AWS account ID by default.
     *
     * <p>For more details, see <a
     * href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html">...</a>
     */
    public static final String GLUE_CATALOG_ID = "aws.glue.id";

    /**
     * The account ID used in a Glue resource ARN, e.g.
     * arn:aws:glue:us-east-1:1000000000000:table/db1/table1
     */
    public static final String GLUE_ACCOUNT_ID = "aws.glue.account-id";
}
