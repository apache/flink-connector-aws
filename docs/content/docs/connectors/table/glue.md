---
title: "AWS Glue Catalog"
weight: 11
type: docs
aliases:
  - /dev/table/connectors/glue.html
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

# AWS Glue Catalog

The AWS Glue Catalog provides a way to use [AWS Glue](https://aws.amazon.com/glue) as a catalog for Apache Flink. 
This allows users to access Glue's metadata store directly from Flink SQL and Table API.

## Dependencies

{{< sql_download_table "glue" >}}

## How to create a Glue Catalog

### SQL

```sql
CREATE CATALOG glue_catalog WITH (
    'type' = 'glue'
    [, 'catalog-name' = '...']
    [, 'default-database' = '...']
    [, 'region' = '...']
    [, 'access-key' = '...']
    [, 'secret-key' = '...'] 
    [, 'session-token' = '...']
    [, 'role-arn' = '...']
    [, 'role-session-name' = '...']
    [, 'endpoint-url' = '...']
    [, 'parameters' = '...'] 
);
```

### Java/Scala

```java
TableEnvironment tableEnv = TableEnvironment.create(...);

String name = "glue_catalog";
String defaultDatabase = "default";
String region = "us-east-1";

Map<String, String> options = new HashMap<>();
options.put("type", "glue");
options.put("default-database", defaultDatabase);
options.put("region", region);

Catalog catalog = CatalogUtils.createCatalog(name, options);
tableEnv.registerCatalog(name, catalog);

// Set the catalog as current catalog
tableEnv.useCatalog(name);
```

### Python

```python
from pyflink.table import *

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

name = "glue_catalog"
default_database = "default"
region = "us-east-1"

options = {
    "type": "glue",
    "default-database": default_database,
    "region": region
}

t_env.register_catalog(name, options)

# Set the GlueCatalog as the current catalog
t_env.use_catalog(name)
```

## Catalog Configuration Options

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Option</th>
        <th class="text-left" style="width: 15%">Required</th>
        <th class="text-left" style="width: 10%">Default</th>
        <th class="text-left" style="width: 55%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>type</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Catalog type. Must be set to <code>glue</code>.</td>
    </tr>
    <tr>
      <td><h5>catalog-name</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">glue-catalog</td>
      <td>The name of the catalog.</td>
    </tr>
    <tr>
      <td><h5>default-database</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">default</td>
      <td>The default database to use if none is specified.</td>
    </tr>
    <tr>
      <td><h5>region</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS region of the Glue service. If not specified, it will be determined through the default AWS region provider chain.</td>
    </tr>
    <tr>
      <td><h5>access-key</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS access key. If not specified, it will be determined through the default AWS credentials provider chain.</td>
    </tr>
    <tr>
      <td><h5>secret-key</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS secret key. If not specified, it will be determined through the default AWS credentials provider chain.</td>
    </tr>
    <tr>
      <td><h5>session-token</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS session token. Only required if using temporary credentials.</td>
    </tr>
    <tr>
      <td><h5>role-arn</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>ARN of the IAM role to assume. Use this for cross-account access or when using temporary credentials.</td>
    </tr>
    <tr>
      <td><h5>role-session-name</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">flink-glue-catalog</td>
      <td>Session name for the assumed role.</td>
    </tr>
    <tr>
      <td><h5>endpoint-url</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Custom endpoint URL for the AWS Glue service (e.g., for testing with localstack).</td>
    </tr>
    <tr>
      <td><h5>parameters</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Additional parameters to pass to the catalog implementation.</td>
    </tr>
    </tbody>
</table>

## Data Type Mapping

AWS Glue data types are mapped to corresponding Flink SQL data types. The following table lists the type mapping:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">AWS Glue Data Type</th>
        <th class="text-left">Flink SQL Data Type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>char, varchar, string</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>boolean</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>smallint, int, tinyint</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>bigint</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>float, real</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>double</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>numeric, decimal</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>date</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>timestamp</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
      <td>binary</td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>array</td>
      <td>ARRAY</td>
    </tr>
    <tr>
      <td>map</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>struct</td>
      <td>ROW</td>
    </tr>
    </tbody>
</table>

## Features

Currently, the following features are supported:

* Databases: create, drop, alter, use, list
* Tables: create, drop, alter, list, describe
* Views: create, drop, alter, list, describe
* User-defined functions: list

## Examples

```sql
-- Create a Glue catalog
CREATE CATALOG glue WITH (
  'type' = 'glue',
  'region' = 'us-east-1'
);

-- Use the Glue catalog
USE CATALOG glue;

-- Create a database in Glue
CREATE DATABASE IF NOT EXISTS mydb
COMMENT 'A new database in AWS Glue';

-- Use the "mydb" database
USE mydb;

-- Create a table
CREATE TABLE mytable (
  user_id BIGINT,
  name STRING,
  date_joined TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://mybucket/path/to/data',
  'format' = 'parquet'
);

-- Query the table
SELECT * FROM mytable;

-- Drop the table
DROP TABLE mytable;

-- Drop the database
DROP DATABASE mydb;
```

## Limitations

* AWS Glue schema evolution is not fully supported.
* Some complex AWS Glue features like encryption options are not exposed.
* Functions created in Glue are visible in the catalog but not automatically registered for use in Flink SQL.

{{< top >}} 