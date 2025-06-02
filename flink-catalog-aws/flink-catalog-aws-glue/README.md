# Flink AWS Glue Catalog Connector

The Flink AWS Glue Catalog connector provides integration between Apache Flink and the AWS Glue Data Catalog. This connector enables Flink applications to use AWS Glue as a metadata catalog for tables, databases, and schemas, allowing seamless SQL queries against AWS resources.

## Features

- Register AWS Glue as a catalog in Flink applications
- Access Glue databases and tables through Flink SQL
- Support for various AWS data sources (S3, Kinesis, MSK)
- Mapping between Flink and AWS Glue data types
- Compatibility with Flink's Table API and SQL interface

## Prerequisites

Before getting started, ensure you have the following:

- **AWS account** with appropriate permissions for AWS Glue and other required services
- **AWS credentials** properly configured

## Getting Started

### 1. Add Dependency

Add the AWS Glue Catalog connector to your Flink project:

### 2. Configure AWS Credentials

Ensure AWS credentials are configured using one of these methods:

- Environment variables
- AWS credentials file
- IAM roles (for applications running on AWS)

### 3. Register the Glue Catalog

You can register the AWS Glue catalog using either the Table API or SQL:

#### Using Table API (Java/Scala)

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

#### Using Table API (Python)

```python
# Python
from pyflink.table.catalog import GlueCatalog

# Create and register Glue catalog
glue_catalog = GlueCatalog(
    "glue_catalog",      # Catalog name
    "default",           # Default database
    "us-east-1")         # AWS region

t_env.register_catalog("glue_catalog", glue_catalog)
t_env.use_catalog("glue_catalog")
```

#### Using SQL

In the Flink SQL Client, create and use the Glue catalog:

```sql
-- Create a catalog using Glue
CREATE CATALOG glue_catalog WITH (
    'type' = 'glue',
    'catalog-name' = 'glue_catalog',
    'default-database' = 'default',
    'region' = 'us-east-1'
);

-- Use the created catalog
USE CATALOG glue_catalog;

-- Use a specific database
USE default;
```

### 4. Create or Reference Glue Tables

Once the catalog is registered, you can create new tables or reference existing ones:

```sql
-- Create a new table in Glue
CREATE TABLE customer_table (
  id BIGINT,
  name STRING,
  region STRING
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'customer-stream',
  'aws.region' = 'us-east-1',
  'format' = 'json'
);

-- Query existing Glue table
SELECT * FROM glue_catalog.sales_db.order_table;
```

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

### Important Notes on Case Sensitivity

1. Always use the original column names as defined in your `CREATE TABLE` statement
2. Use backticks (`) when column names contain special characters or spaces
3. Remember that regardless of how Glue stores the data internally, your queries should match your schema definition
4. When creating tables, defining the schema with backticks is recommended for clarity

## Data Type Mapping

The connector handles mapping between Flink data types and AWS Glue data types automatically. The following table shows the basic type mappings:

| Flink Type | AWS Glue Type |
|------------|---------------|
| CHAR       | string        |
| VARCHAR    | string        |
| BOOLEAN    | boolean       |
| BINARY     | binary        |
| VARBINARY  | binary        |
| DECIMAL    | decimal       |
| TINYINT    | byte          |
| SMALLINT   | short         |
| INTEGER    | int           |
| BIGINT     | long          |
| FLOAT      | float         |
| DOUBLE     | double        |
| DATE       | date          |
| TIME       | string        |
| TIMESTAMP  | timestamp     |
| ROW        | struct        |
| ARRAY      | array         |
| MAP        | map           |

## Object Name Case Preservation

### Overview

AWS Glue stores object names in lowercase, but this Flink connector preserves the original case of your database, table, and column names by storing the original names in metadata parameters. This allows you to use any casing convention you prefer while ensuring compatibility with data formats that require specific casing (like JSON).

### How It Works

When you create objects with mixed case names, the connector:

1. **Stores the lowercase version** in Glue (as required by AWS Glue)
2. **Preserves the original case** in metadata parameters
3. **Returns the original case** when listing or retrieving objects
4. **Handles lookups** by original name, regardless of case variations

### Metadata Storage Details

The original names are stored using these parameter keys in AWS Glue:

| Object Type | Glue Storage Location | Parameter Key |
|-------------|----------------------|---------------|
| **Database** | `Database.parameters` | `flink.original-database-name` |
| **Table/View** | `Table.parameters` | `flink.original-table-name` |
| **Column** | `Column.parameters` | `originalName` |
| **Function** | ⚠️ *Not supported yet* | *No parameters field available* |

### Examples

#### Database Names
```sql
-- Create database with mixed case
CREATE DATABASE MyCompanyDB;

-- Stored in Glue as: "mycompanydb" 
-- With parameter: flink.original-database-name = "MyCompanyDB"

-- All these work equivalently:
USE MyCompanyDB;
USE mycompanydb;
USE MYCOMPANYDB;

-- But SHOW DATABASES returns: "MyCompanyDB"
```

#### Table Names
```sql
-- Create table with mixed case
CREATE TABLE MyCompanyDB.UserProfiles (
  UserId INT,
  UserName VARCHAR(100)
);

-- Stored in Glue as: "userprofiles"
-- With parameter: flink.original-table-name = "UserProfiles"

-- All these work equivalently:
SELECT * FROM MyCompanyDB.UserProfiles;
SELECT * FROM mycompanydb.userprofiles;
SELECT * FROM MYCOMPANYDB.USERPROFILES;

-- But SHOW TABLES returns: "UserProfiles"
```

#### Column Names
```sql
-- Columns preserve case automatically
SELECT UserId, UserName FROM UserProfiles;

-- JSON parsing works correctly with original case:
-- {"UserId": 123, "UserName": "John"}
```

### Benefits

1. **User-Friendly**: Use any casing convention you prefer
2. **JSON Compatibility**: Original case preserved for JSON field mapping
3. **External Tool Compatibility**: Other tools can access the same data using Glue's lowercase names
4. **Backward Compatibility**: Existing lowercase objects continue to work
5. **Case-Insensitive Operations**: Objects can be referenced with any case variation

### Limitations

1. **Functions**: Case preservation not yet implemented for user-defined functions due to AWS Glue API limitations
2. **External Tools**: Tools that bypass this connector may only see lowercase names in Glue
3. **Performance**: Initial lookup may require scanning when direct case match fails (rare)

### Migration from Lowercase-Only

Existing catalogs with lowercase-only names will continue to work seamlessly:

- Objects without original name metadata fall back to stored (lowercase) names
- No migration required for existing data
- New objects automatically get case preservation

### Best Practices

1. **Consistent Casing**: Use consistent casing conventions within your organization
2. **Documentation**: Document your naming conventions for team consistency
3. **Testing**: Test with your actual data formats (especially JSON) to ensure case handling meets your needs

## Limitations and Considerations

1. **Case Sensitivity**: As detailed above, database, table, and column names preserve original case, while function names currently use case-insensitive handling.
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

## Additional Resources

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)