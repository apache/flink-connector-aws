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

## Dependencies

{{< sql_download_table "glue" >}}

## Prerequisites

Before getting started, ensure you have the following:

- **AWS account** with appropriate permissions for AWS Glue and other required services
- **AWS credentials** properly configured

## How to create a Glue Catalog

### SQL

```sql
CREATE CATALOG glue_catalog WITH (
    'type' = 'glue',
    'catalog-name' = 'glue_catalog',
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
    </tbody>
</table>

## Data Type Mapping

The connector handles mapping between Flink data types and AWS Glue data types automatically. The following table shows the basic type mappings:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Type</th>
        <th class="text-left">AWS Glue Type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR</td>
      <td>string</td>
    </tr>
    <tr>
      <td>VARCHAR</td>
      <td>string</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>boolean</td>
    </tr>
    <tr>
      <td>BINARY</td>
      <td>binary</td>
    </tr>
    <tr>
      <td>VARBINARY</td>
      <td>binary</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>decimal</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>byte</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>short</td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>int</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>long</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>float</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>double</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>date</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>string</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>timestamp</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>struct</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>array</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>map</td>
    </tr>
    </tbody>
</table>

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
2. **AWS Service Limits**: Be aware of AWS Glue service limits that may affect your application.
3. **Authentication**: Ensure proper AWS credentials with appropriate permissions are available.
4. **Region Selection**: The Glue catalog must be registered with the correct AWS region where your Glue resources exist.
5. **Unsupported Operations**: The following operations are not currently supported:
   - ALTER DATABASE (modifying database properties)
   - ALTER TABLE (modifying table properties or schema)
   - RENAME TABLE
   - Partition management operations (ADD/DROP PARTITION)

## Troubleshooting

### Common Issues

1. **"Table not found"**: Verify the table exists in the specified Glue database and catalog.
2. **Authentication errors**: Check AWS credentials and permissions.
3. **Case sensitivity errors**: Ensure you're using the original column names as defined in your schema.
4. **Type conversion errors**: Verify that data types are compatible between Flink and Glue.

{{< top >}} 