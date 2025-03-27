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

The application registers a **GlueCatalog** with the Flink environment to interact with Glue’s metadata. This enables Flink to read Glue’s schema and catalog information for its SQL operations.

```java
Catalog glueCatalog = new GlueCatalog("glue_catalog", "default", "us-east-1");
tEnv.registerCatalog("glue_catalog", glueCatalog);
tEnv.useCatalog("glue_catalog");
```
### 2. Creating and Interacting with Glue Tables
TBD