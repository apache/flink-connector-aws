# Flink-AWS Glue Integration

This project demonstrates how to integrate **Apache Flink** with the **AWS Glue Data Catalog**. It enables Flink applications to query metadata from AWS Glue, register catalogs, and interact with AWS services such as Kinesis and Kafka through Flink SQL.

## Prerequisites

Before getting started, ensure you have the following:

- **Apache Flink 1.20** or later installed.
- An **AWS account** with appropriate permissions to access **AWS Glue** and **Kinesis** (if used).
- **Maven 3.8+** for building the project.
- **Java 11** or later installed.

## Project Setup

### 1. Clone the Repository

Clone this repository to your local machine:

```bash
git clone x
cd flink-catalog-aws-glue
```
### 2. Build the Project

Run the following Maven command to build the project:

```bash
mvn clean install
```

### 3. Configure AWS Credentials

Ensure you have AWS credentials set up on your local machine. You can configure them via the AWS CLI:

```bash
aws configure
```

### 4. Running the Application

Once you have everything set up, you can run the **StreamingJob** which integrates Flink with the Glue Data Catalog:

### Integration with Glue

```markdown
## Integration with Glue

The main integration points for AWS Glue are handled in the `GlueTableUtils` and `GlueTypeConverter` classes. They help map Glue-specific metadata (such as columns and types) to Flink-compatible formats.

### 1. Glue Catalog Setup

The application registers a **GlueCatalog** with the Flink environment to interact with Glue's metadata. This enables Flink to read Glue's schema and catalog information for its SQL operations.

```java
Catalog glueCatalog = new GlueCatalog("glue_catalog", "default", "us-east-1");
tEnv.registerCatalog("glue_catalog", glueCatalog);
tEnv.useCatalog("glue_catalog");
```
### 2. Creating and Interacting with Glue Tables
TBD

## Case Sensitivity with Nested Fields

### Understanding Case Handling in AWS Glue

AWS Glue handles case sensitivity in a specific way:

1. **Top-level column names** are automatically lowercased in Glue (e.g., `UserProfile` becomes `userprofile`).
2. **Nested struct field names** preserve their original case in Glue (e.g., inside the struct, `FirstName` stays as `FirstName`).

This difference in case handling can cause issues when querying nested fields using Flink SQL.

### Example

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

When stored in Glue, it looks like:

```json
{
  "userprofile": {  // Note: lowercased
    "FirstName": "string",  // Note: original case preserved
    "lastName": "string"    // Note: original case preserved
  }
}
```

### Handling Case Sensitivity in Queries

When querying this data with Flink SQL, a direct query like:

```sql
SELECT UserProfile.FirstName FROM nested_json_test
```

Will fail with an error like:

```
Column 'UserProfile.FirstName' not found in table 'nested_json_test'; did you mean 'firstname'?
```

To properly query the data, you need to use backticks to handle case sensitivity:

```sql
SELECT `userprofile`.`FirstName` FROM nested_json_test
```

### Utility Method for SQL Generation

To help generate properly quoted SQL expressions for nested fields, use the `generateSafeNestedFieldAccess` utility method:

```java
// Import the utility class
import com.amazonaws.services.msf.util.GlueTableUtils;

// Generate a SQL-safe field access expression
String fieldExpression = GlueTableUtils.generateSafeNestedFieldAccess("userprofile", "FirstName");
// Result: "`userprofile`.`FirstName`"

// Use it in a SQL query
String sql = "SELECT " + fieldExpression + " FROM nested_json_test";
// Result: "SELECT `userprofile`.`FirstName` FROM nested_json_test"
```

This approach ensures that your SQL queries will work correctly with the case-sensitive nested fields in AWS Glue.