package com.amazonaws.services.msf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJob {

    // Logger to log messages for debugging or info
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Create a new Flink configuration object
        Configuration configuration = new Configuration();

        // Create a local streaming execution environment with web UI for monitoring
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // Create the TableEnvironment using the provided execution environment and settings
        TableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().build());

        // Create a GlueCatalog for connecting Flink with AWS Glue
        Catalog glueCatalog = new GlueCatalog("glue_catalog", "default", "us-east-1");

        // Register the Glue catalog in the TableEnvironment so that it can be accessed
        tEnv.registerCatalog("glue_catalog", glueCatalog);

        // Set the registered Glue catalog as the default catalog
        tEnv.useCatalog("glue_catalog");

        // Example SQL statements to interact with the Glue catalog and databases
        // Drop the 'test' database if it exists and create it
        tEnv.executeSql("DROP DATABASE IF EXISTS test").print();
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();

        // Show the list of databases in the catalog
        tEnv.executeSql("SHOW DATABASES").print();

        // Switch to the 'test' database
        tEnv.executeSql("USE test").print();

        // Show the list of tables in the 'test' database
        tEnv.executeSql("SHOW TABLES").print();

        // Drop the 'fran' table if it exists and create it
        tEnv.executeSql("DROP TABLE IF EXISTS kinesisTable").print();

        // Create a new table 'fran' with specified schema and configuration
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS kinesisTable (" +
                "  `user_id` STRING," +
                "  `productName` STRING," +
                "  `color` STRING," +
                "  `department` STRING," +
                "  `product` STRING," +
                "  `campaign` STRING," +
                "  `price` DOUBLE," +
                "  `creationTimestamp` STRING" +
                ")" +
                "WITH (" +
                "  'connector' = 'kinesis'," +
                "  'stream.arn' = 'arn:aws:kinesis:us-east-1:xxxx:stream/input'," +
                "  'aws.region' = 'us-east-1'," +
                "  'source.init.position' = 'TRIM_HORIZON'," +
                "  'format' = 'json'" +
                ");").print();

        // Show the list of tables in the 'test' database again after creating the new table
        tEnv.executeSql("SHOW TABLES").print();

        // Run a query to select all records from the 'fran' table and print the results
        tEnv.executeSql("SELECT * FROM kinesisTable").print();
    }
}
