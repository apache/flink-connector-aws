package com.amazonaws.services.msf;

import com.amazonaws.services.msf.operations.FakeGlueClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.glue.GlueClient;

import static org.junit.jupiter.api.Assertions.*;
public class StreamingJobTest {

    private MiniCluster miniCluster;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private GlueClient fakeGlueClient;

    @Before
    public void setup() throws Exception {

        fakeGlueClient = new FakeGlueClient();  // Assume this is your fake client
        // Set up the Flink MiniCluster
        miniCluster = new MiniCluster(new MiniClusterConfiguration.Builder().setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
        miniCluster.start();

        // Set up the Flink StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Set up the StreamTableEnvironment
        tEnv = StreamTableEnvironment.create(env);
    }

    @After
    public void tearDown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    public void testCreateDatabase() throws Exception {
        // Initialize GlueCatalog with necessary parameters
        String region = "us-east-1";
        String databaseName = "test";

        GlueCatalog glueCatalog = new GlueCatalog("glueCatalog","default",region,fakeGlueClient);

        // Register the catalog in Flink
        tEnv.registerCatalog("myGlueCatalog", glueCatalog);
        tEnv.useCatalog("myGlueCatalog");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();
        // Show the list of databases in the catalog
        assertTrue(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testDropDatabase() throws Exception {
        // Initialize GlueCatalog with necessary parameters
        String region = "us-east-1";
        String databaseName = "test";

        GlueCatalog glueCatalog = new GlueCatalog("glueCatalog","default",region,fakeGlueClient);

        // Register the catalog in Flink
        tEnv.registerCatalog("myGlueCatalog", glueCatalog);
        tEnv.useCatalog("myGlueCatalog");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();
        tEnv.executeSql("DROP DATABASE test").print();
        // Show the list of databases in the catalog
        assertFalse(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testCreateTable() throws Exception {
        // Initialize GlueCatalog with necessary parameters
        String region = "us-east-1";
        String databaseName = "test";
        String tableName = "kinesisTable";

        GlueCatalog glueCatalog = new GlueCatalog("glueCatalog",databaseName,region,fakeGlueClient);

        // Register the catalog in Flink
        tEnv.registerCatalog("myGlueCatalog", glueCatalog);
        tEnv.useCatalog("myGlueCatalog");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();
        // Switch to the 'test' database
        tEnv.executeSql("USE test").print();
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
                "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input'," +
                "  'aws.region' = 'us-east-1'," +
                "  'source.init.position' = 'TRIM_HORIZON'," +
                "  'format' = 'json'" +
                ");").print();
        // Show the list of databases in the catalog
        ObjectPath objectPath = new ObjectPath(databaseName,tableName);
        assertTrue(glueCatalog.tableExists(objectPath));
    }

    @Test
    public void testDropTable() throws Exception {
        // Initialize GlueCatalog with necessary parameters
        String region = "us-east-1";
        String databaseName = "test";
        String tableName = "kinesisTable";

        GlueCatalog glueCatalog = new GlueCatalog("glueCatalog",databaseName,region,fakeGlueClient);

        // Register the catalog in Flink
        tEnv.registerCatalog("myGlueCatalog", glueCatalog);
        tEnv.useCatalog("myGlueCatalog");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();
        // Switch to the 'test' database
        tEnv.executeSql("USE test").print();
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
                "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input'," +
                "  'aws.region' = 'us-east-1'," +
                "  'source.init.position' = 'TRIM_HORIZON'," +
                "  'format' = 'json'" +
                ");").print();

        tEnv.executeSql("DROP TABLE kinesisTable").print();

        // Show the list of databases in the catalog
        ObjectPath objectPath = new ObjectPath(databaseName,tableName);
        assertFalse(glueCatalog.tableExists(objectPath));
    }

}
