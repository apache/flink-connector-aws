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

## Features

- Register AWS Glue as a catalog in Flink applications
- Access Glue databases and tables through Flink SQL
- Support for various AWS data sources (S3, Kinesis, MSK)
- Mapping between Flink and AWS Glue data types
- Compatibility with Flink's Table API and SQL interface

The Glue Catalog is registered through the Table API / SQL. DataStream applications can also
use it by converting between DataStreams and Tables with the
[DataStream API integration]({{< ref "docs/dev/table/data_stream_api" >}}), so tables backed by
Glue metadata are accessible from DataStream programs through a `StreamTableEnvironment`.

## Dependencies

{{< sql_connector_download_table "glue" >}}

## Prerequisites

Before getting started, ensure you have the following:

- **AWS account** with appropriate permissions for AWS Glue and other required services
- **AWS credentials** properly configured, see the
  [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)
  for the supported configuration options

## How to create a Glue Catalog

### SQL

```sql
CREATE CATALOG glue_catalog WITH (
    'type' = 'glue',
    'default-database' = 'default',
    'region' = 'us-east-1'
);
```

### Java/Scala

```java
// Java/Scala
import org.apache.flink.table.catalog.glue.GlueCatalog;
import org.apache.flink.table.catalog.Catalog;

// Create Glue catalog instance
Catalog glueCatalog = new GlueCatalog(
    "glue_catalog",      // Catalog name
    "default",           // Default database
    "us-east-1");         // AWS region


// Register with table environment
tableEnv.registerCatalog("glue_catalog", glueCatalog);
tableEnv.useCatalog("glue_catalog");
```

### Python

```python
# Python
from pyflink.table.catalog import GlueCatalog

# Create and register Glue catalog
glue_catalog = GlueCatalog(
    "glue_catalog",      // Catalog name
    "default",           // Default database
    "us-east-1")         // AWS region

t_env.register_catalog("glue_catalog", glue_catalog)
t_env.use_catalog("glue_catalog")
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
      <td><h5>default-database</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">default</td>
      <td>The default database to use if none is specified.</td>
    </tr>
    <tr>
      <td><h5>region</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS region of the Glue service.</td>
    </tr>
    <tr>
      <td><h5>aws.*</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>AWS client configuration shared with the other AWS connectors. For example, <code>aws.credentials.provider</code> selects the credential provider (<code>AUTO</code>, <code>BASIC</code>, <code>PROFILE</code>, <code>ASSUME_ROLE</code>, <code>WEB_IDENTITY_TOKEN</code>, ...), and <code>aws.endpoint</code> points the catalog at a Glue-compatible endpoint. See the <a href="https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/connectors/datastream/kinesis/#configuring-access-to-aws-with-iam">AWS connector documentation</a> for the full list.</td>
    </tr>
    <tr>
      <td><h5>http-client.*</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>HTTP client options for the underlying AWS SDK client: <code>http-client.connection-timeout-ms</code>, <code>http-client.socket-timeout-ms</code>, <code>http-client.apache.max-connections</code>, <code>http-client.max-concurrency</code>, <code>http-client.read-timeout</code>, <code>http-client.protocol.version</code> (<code>HTTP1_1</code> or <code>HTTP2</code>) and <code>http-client.type</code> (only <code>apache</code> is supported). Unknown or invalid values are rejected when the catalog is created.</td>
    </tr>
    </tbody>
</table>

## Data Type Mapping

The catalog maps Flink data types to the Glue (Hive-style) type strings that other AWS services read, so tables created from Flink are usable from Athena, Spark or crawlers, and tables they create are readable from Flink.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Type</th>
        <th class="text-left">AWS Glue Type</th>
        <th class="text-left">Notes</th>
      </tr>
    </thead>
    <tbody>
    <tr><td>CHAR(n), VARCHAR(n), STRING</td><td>string</td><td>Glue <code>char(n)</code> / <code>varchar(n)</code> written by other engines read back as <code>CHAR(n)</code> / <code>VARCHAR(n)</code>.</td></tr>
    <tr><td>BOOLEAN</td><td>boolean</td><td></td></tr>
    <tr><td>BINARY, VARBINARY, BYTES</td><td>binary</td><td></td></tr>
    <tr><td>DECIMAL(p, s)</td><td>decimal(p,s)</td><td>An unparameterized Glue <code>decimal</code> reads back as <code>DECIMAL(10, 0)</code>.</td></tr>
    <tr><td>TINYINT</td><td>tinyint</td><td></td></tr>
    <tr><td>SMALLINT</td><td>smallint</td><td></td></tr>
    <tr><td>INT</td><td>int</td><td>Glue <code>integer</code> is accepted as well.</td></tr>
    <tr><td>BIGINT</td><td>bigint</td><td></td></tr>
    <tr><td>FLOAT</td><td>float</td><td></td></tr>
    <tr><td>DOUBLE</td><td>double</td><td></td></tr>
    <tr><td>DATE</td><td>date</td><td></td></tr>
    <tr><td>TIME(p)</td><td>string</td><td>Glue has no time type.</td></tr>
    <tr><td>TIMESTAMP(p)</td><td>timestamp</td><td></td></tr>
    <tr><td>TIMESTAMP_LTZ(p)</td><td>timestamp</td><td></td></tr>
    <tr><td>ROW</td><td>struct&lt;name:type,...&gt;</td><td>Struct field names keep their case.</td></tr>
    <tr><td>ARRAY</td><td>array&lt;type&gt;</td><td></td></tr>
    <tr><td>MAP</td><td>map&lt;key,value&gt;</td><td></td></tr>
    </tbody>
</table>

`INTERVAL`, `MULTISET` and `TIMESTAMP WITH TIME ZONE` columns have no Glue representation and are rejected when a table is created. Glue `uniontype` columns on tables created by other engines cannot be read.

### Schema fidelity

The Glue type strings above cannot express everything a Flink schema can: `TIMESTAMP(3)` and `TIMESTAMP(6)` are both `timestamp`, `NOT NULL`, `TIMESTAMP_LTZ` and `TIME` are lost, and computed columns, metadata columns, watermarks, primary keys and column order relative to partition keys have no Glue counterpart at all. For tables and views created through Flink, the catalog therefore also records the declared schema in `flink.schema.*` table parameters and restores it on read, so `SHOW CREATE TABLE` returns exactly what was declared. Other engines see the plain Glue columns and ignore these parameters; table options starting with `flink.schema.` or `flink.original-` are reserved and rejected in `WITH (...)`.

Tables that were not created by Flink have no such metadata and are read from their Glue columns alone (physical columns only, nullable, with the lossy mappings above). Column comments are stored on the Glue column itself and are visible to every engine.

## Catalog Operations

The AWS Glue Catalog connector supports several catalog operations through SQL. Here's a list of the operations that are currently implemented:

### Database Operations

```sql
-- Create a new database
CREATE DATABASE sales_db;

-- Create a database with comment
CREATE DATABASE sales_db COMMENT 'Database for sales data';

-- Create a database if it doesn't exist
CREATE DATABASE IF NOT EXISTS sales_db;

-- Drop a database
DROP DATABASE sales_db;

-- Drop a database if it exists
DROP DATABASE IF EXISTS sales_db;

-- Use a specific database
USE sales_db;
```

### Table Operations

```sql
-- Create a table
CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP,
  amount DECIMAL(10, 2)
);

-- Create a table with comment and properties
CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP,
  amount DECIMAL(10, 2),
  PRIMARY KEY (order_id) NOT ENFORCED
) COMMENT 'Table storing order information'
WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'customer-stream',
  'aws.region' = 'us-east-1',
  'format' = 'json'
);

-- Create table if not exists
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT,
  customer_id BIGINT
);

-- Drop a table
DROP TABLE orders;

-- Drop a table if it exists
DROP TABLE IF EXISTS orders;

-- Show table details
DESCRIBE orders;
```

### Partition Operations

Partitioned tables store one Glue partition per distinct combination of partition key values.
Partitions are created, listed and dropped with the standard Flink SQL statements; the partition
spec must name every partition key of the table.

```sql
CREATE TABLE events (
  id BIGINT,
  payload STRING,
  region STRING
) PARTITIONED BY (region) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-bucket/events',
  'format' = 'json'
);

-- Add a partition; its location defaults to <table location>/region=eu-west-1
ALTER TABLE events ADD PARTITION (region = 'eu-west-1');

-- Add a partition stored somewhere else
ALTER TABLE events ADD IF NOT EXISTS PARTITION (region = 'us-east-1')
WITH ('location' = 's3://other-bucket/events-us');

SHOW PARTITIONS events;

ALTER TABLE events DROP IF EXISTS PARTITION (region = 'eu-west-1');
```

The `location` partition property is the partition's Glue storage location, in the same way
`hive.location-uri` is for the Hive catalog: `getPartition` exposes it and
`createPartition`/`alterPartition` consume it, so a partition round-trips between catalogs with its
location intact. When it is omitted on create, the catalog derives the Hive-style default
`<table location>/key1=value1/key2=value2` (path-escaped like Hive). Because the property is part of
what `getPartition` returns, copying one partition's properties into another partition would point
the second at the first partition's data; the catalog rejects a `location` that spells a different
partition of the same table, and accepts any other location as a deliberate choice. Every other
property is stored as a Glue partition parameter.

### View Operations

```sql
-- Create a view
CREATE VIEW order_summary AS
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount
FROM orders
GROUP BY customer_id;

-- Create a temporary view (only available in current session)
CREATE TEMPORARY VIEW temp_view AS
SELECT * FROM orders WHERE amount > 100;

-- Drop a view
DROP VIEW order_summary;

-- Drop a view if it exists
DROP VIEW IF EXISTS order_summary;
```

### Function Operations

```sql
-- Register a function
CREATE FUNCTION multiply_func AS 'com.example.functions.MultiplyFunction';

-- Register a temporary function
CREATE TEMPORARY FUNCTION temp_function AS 'com.example.functions.TempFunction';

-- Drop a function
DROP FUNCTION multiply_func;

-- Drop a temporary function
DROP TEMPORARY FUNCTION temp_function;
```

### Listing Resources

Query available catalogs, databases, and tables:

```sql
-- List all catalogs
SHOW CATALOGS;

-- List databases in the current catalog
SHOW DATABASES;

-- List tables in the current database
SHOW TABLES;

-- List tables in a specific database
SHOW TABLES FROM sales_db;

-- List views in the current database
SHOW VIEWS;

-- List functions
SHOW FUNCTIONS;
```

## Case Sensitivity in AWS Glue

### Understanding Case Handling

AWS Glue handles case sensitivity in a specific way:

1. **Top-level column names** are automatically lowercased in Glue (e.g., `UserProfile` becomes `userprofile`)
2. **Nested struct field names** preserve their original case in Glue (e.g., inside a struct, `FirstName` stays as `FirstName`)

However, when writing queries in Flink SQL, you should use the **original column names** as defined in your `CREATE TABLE` statement, not how they are stored in Glue.

### Example with Nested Fields

Consider this table definition:

```sql
CREATE TABLE nested_json_test (
  `Id` INT,
  `UserProfile` ROW<
     `FirstName` VARCHAR(255), 
     `lastName` VARCHAR(255)
  >,
  `event_data` ROW<
     `EventType` VARCHAR(50),
     `eventTimestamp` TIMESTAMP(3)
  >,
  `metadata` MAP<VARCHAR(100), VARCHAR(255)>
)
```

When stored in Glue, the schema looks like:

```json
{
  "userprofile": {  // Note: lowercased
    "FirstName": "string",  // Note: original case preserved
    "lastName": "string"    // Note: original case preserved
  }
}
```

### Querying Nested Fields

When querying, always use the original column names as defined in your `CREATE TABLE` statement:

```sql
-- CORRECT: Use the original column names from CREATE TABLE
SELECT UserProfile.FirstName FROM nested_json_test;

-- INCORRECT: This doesn't match your schema definition
SELECT `userprofile`.`FirstName` FROM nested_json_test;

-- For nested fields within nested fields, also use original case
SELECT event_data.EventType, event_data.eventTimestamp FROM nested_json_test;

-- Accessing map fields
SELECT metadata['source_system'] FROM nested_json_test;
```

## Limitations and Considerations

1. **Case Sensitivity**: As detailed above, always use the original column names from your schema definition when querying.
2. **AWS Service Limits**: The catalog is subject to the [AWS Glue service quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html) of the account. The most relevant ones are the number of databases per account, tables per database, partitions per table and per account, and the Glue API rate limits (which apply to catalog operations such as listing or resolving tables). Most of these are soft limits that can be raised through AWS Service Quotas.
3. **Authentication**: Ensure proper AWS credentials with appropriate permissions are available. The credential mode is configurable through the `aws.credentials.provider` catalog option.
4. **Region Selection**: The Glue catalog must be registered with the correct AWS region where your Glue resources exist.
5. **Tables created by other engines**: Tables and views written by Athena, Glue crawlers, Spark or Hive (Glue table types such as `EXTERNAL_TABLE` and `VIRTUAL_VIEW`) are listed, described and readable from Flink with their Glue columns. They carry no Flink connector options, so querying one directly reports a missing `connector` option; create a Flink table over the same data with `CREATE TABLE ... WITH ('connector' = ..., ...) LIKE <foreign table>`, which reuses the Glue schema without modifying the object the other engine owns. Avoid `ALTER TABLE` on such tables: Flink would rewrite their columns through the lossy type mapping. Their schema is read from the Glue columns alone, see [Schema fidelity](#schema-fidelity).
6. **Unsupported Operations**: The following operations throw `UnsupportedOperationException`:
   - `ALTER DATABASE` (modifying database properties)
   - `ALTER TABLE ... RENAME TO` (renaming a table)
   - `ALTER TABLE` on a view, or changing the partition keys of an existing table
   - Table, column and partition statistics (`ANALYZE TABLE`, `alterTableStatistics`, `alterTableColumnStatistics`, `alterPartitionStatistics`, `alterPartitionColumnStatistics`); reads return unknown statistics
   - Partition filter push-down: `listPartitionsByFilter` is not implemented, so the planner falls back to `listPartitions` and evaluates partition predicates on the Flink side. Queries over partitioned tables stay correct but list every partition from Glue, which counts against the Glue API quotas listed above.

## Troubleshooting

### Common Issues

1. **"Table not found"**: Verify the table exists in the specified Glue database and catalog.
2. **Authentication errors**: Check AWS credentials and permissions.
3. **Case sensitivity errors**: Ensure you're using the original column names as defined in your schema.
4. **Type conversion errors**: Verify that data types are compatible between Flink and Glue.

{{< top >}} 