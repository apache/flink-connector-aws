# Apache Flink AWS Glue Catalog

Flink `Catalog` implementation backed by the AWS Glue Data Catalog. It lets Flink SQL and the Table
API store and resolve databases, tables, views, functions and partitions in Glue, and read tables
created by other engines (Athena, Glue crawlers, Spark, Hive).

## Documentation

The user documentation lives with the other connector docs and is published on the Flink website:

- Source: [`docs/content/docs/connectors/table/glue.md`](../../docs/content/docs/connectors/table/glue.md)
- Covers configuration options (`region`, `default-database`, `aws.*`, `http-client.*`), the data
  type mapping, schema fidelity, case handling, supported catalog operations and limitations.

## Quick start

```sql
CREATE CATALOG glue WITH (
  'type' = 'glue',
  'region' = 'us-east-1',
  'default-database' = 'default'
);
USE CATALOG glue;
```

## Module layout

| Package | Contents |
|---|---|
| `org.apache.flink.table.catalog.glue` | `GlueCatalog`, the `Catalog` implementation |
| `...glue.factory` | `GlueCatalogFactory`, registered as catalog type `glue` |
| `...glue.operator` | One class per Glue resource (database, table, partition, function) wrapping the SDK calls |
| `...glue.util` | Type conversion, schema fidelity parameters, connector-specific storage handling |

## Testing

Unit tests run against an in-memory fake Glue by default. The same suites run against real AWS Glue
when credentials are supplied, and `GlueCatalog*MotoITCase` exercises the wire protocol against a
[moto](https://github.com/getmoto/moto) container:

```bash
# fake Glue (default)
mvn test -pl flink-catalog-aws/flink-catalog-aws-glue

# real AWS Glue (uses the default AWS credential chain)
IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS=true IT_CASE_GLUE_CATALOG_REGION=eu-central-1 \
  mvn test -pl flink-catalog-aws/flink-catalog-aws-glue

# moto wire tests (requires Docker)
mvn verify -pl flink-catalog-aws/flink-catalog-aws-glue
```

End-to-end SQL tests against real Glue live in
`flink-connector-aws-e2e-tests/flink-catalog-aws-glue-e2e-tests`.
