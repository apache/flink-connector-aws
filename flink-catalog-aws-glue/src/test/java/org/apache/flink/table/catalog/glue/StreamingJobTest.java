package org.apache.flink.table.catalog.glue;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.glue.operations.FakeGlueClient;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Tests for the GlueCatalog integration with streaming SQL operations.
 * This test validates the correctness of SQL DDL operations (CREATE/DROP DATABASE/TABLE)
 * when using the GlueCatalog with a Flink streaming environment.
 *
 * <p>Note: This is an integration test and should be moved to a separate module.
 * Currently disabled as it's not appropriate for the catalog connector module.
 */
@Disabled("Integration test that should be moved to a separate module")
public class StreamingJobTest {

    private MiniCluster miniCluster;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private GlueClient fakeGlueClient;
    private GlueCatalog glueCatalog;
    private String databaseName;
    private String tableName;

    @BeforeEach
    public void setup() throws Exception {
        fakeGlueClient = new FakeGlueClient();

        // Set up the Flink MiniCluster
        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(1)
                                .build());
        miniCluster.start();

        // Set up the Flink StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Set up the StreamTableEnvironment
        tEnv = StreamTableEnvironment.create(env);
        String region = "us-east-1";
        String defaultDB = "default";
        databaseName = "test";
        tableName = "tableName";
        glueCatalog = new GlueCatalog("glueCatalog", defaultDB, region, fakeGlueClient);

        // Register the catalog in Flink
        tEnv.registerCatalog("myGlueCatalog", glueCatalog);
        tEnv.useCatalog("myGlueCatalog");
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String sqlStatement = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(sqlStatement).print();
        Assertions.assertTrue(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testCreateDatabaseAlreadyExists() throws Exception {
        // First create the database
        String sqlStatement = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(sqlStatement).print();

        // Try to create it again - should not throw exception due to IF NOT EXISTS
        Assertions.assertDoesNotThrow(() -> tEnv.executeSql(sqlStatement).print());
    }

    @Test
    public void testDropDatabase() throws Exception {
        String sqlCreateDatabaseStatement =
                String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(sqlCreateDatabaseStatement).print();
        String sqlDropDatabaseStatement = String.format("DROP DATABASE %s", databaseName);
        tEnv.executeSql(sqlDropDatabaseStatement).print();

        Assertions.assertFalse(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testDropNonExistentDatabase() throws Exception {
        String sqlDropDatabaseStatement = String.format("DROP DATABASE IF EXISTS %s", databaseName);
        Assertions.assertDoesNotThrow(() -> tEnv.executeSql(sqlDropDatabaseStatement).print());
    }

    @Test
    public void testCreateTable() throws Exception {
        String sqlStatementCreateDatabase =
                String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String sqlStatementUseDatabase = String.format("USE %s", databaseName);
        String sqlStatementCreateTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s ("
                                + "  `user_id` STRING,"
                                + "  `productName` STRING,"
                                + "  `color` STRING,"
                                + "  `department` STRING,"
                                + "  `product` STRING,"
                                + "  `campaign` STRING,"
                                + "  `price` DOUBLE,"
                                + "  `creationTimestamp` STRING"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'kinesis',"
                                + "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input',"
                                + "  'aws.region' = 'us-east-1',"
                                + "  'source.init.position' = 'TRIM_HORIZON',"
                                + "  'format' = 'json'"
                                + ");",
                        tableName);

        tEnv.executeSql(sqlStatementCreateDatabase).print();
        tEnv.executeSql(sqlStatementUseDatabase).print();
        tEnv.executeSql(sqlStatementCreateTable).print();

        ObjectPath objectPath = new ObjectPath(databaseName, tableName);
        Assertions.assertTrue(glueCatalog.tableExists(objectPath));
    }

    @Test
    public void testCreateTableAlreadyExists() throws Exception {
        String sqlStatementCreateDatabase =
                String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String sqlStatementUseDatabase = String.format("USE %s", databaseName);
        String sqlStatementCreateTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s ("
                                + "  `user_id` STRING"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'kinesis',"
                                + "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input',"
                                + "  'aws.region' = 'us-east-1',"
                                + "  'source.init.position' = 'TRIM_HORIZON',"
                                + "  'format' = 'json'"
                                + ");",
                        tableName);

        tEnv.executeSql(sqlStatementCreateDatabase).print();
        tEnv.executeSql(sqlStatementUseDatabase).print();
        tEnv.executeSql(sqlStatementCreateTable).print();

        // Try to create it again - should not throw exception due to IF NOT EXISTS
        Assertions.assertDoesNotThrow(() -> tEnv.executeSql(sqlStatementCreateTable).print());
    }

    @Test
    public void testDropTable() throws Exception {
        String sqlStatementCreateDatabase =
                String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String sqlStatementUseDatabase = String.format("USE %s", databaseName);
        String sqlStatementCreateTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s ("
                                + "  `user_id` STRING,"
                                + "  `productName` STRING,"
                                + "  `color` STRING,"
                                + "  `department` STRING,"
                                + "  `product` STRING,"
                                + "  `campaign` STRING,"
                                + "  `price` DOUBLE,"
                                + "  `creationTimestamp` STRING"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'kinesis',"
                                + "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input',"
                                + "  'aws.region' = 'us-east-1',"
                                + "  'source.init.position' = 'TRIM_HORIZON',"
                                + "  'format' = 'json'"
                                + ");",
                        tableName);

        String sqlStatementDropTable = String.format("DROP TABLE %s", tableName);

        tEnv.executeSql(sqlStatementCreateDatabase).print();
        tEnv.executeSql(sqlStatementUseDatabase).print();
        tEnv.executeSql(sqlStatementCreateTable).print();

        ObjectPath objectPath = new ObjectPath(databaseName, tableName);
        tEnv.executeSql(sqlStatementDropTable).print();

        Assertions.assertFalse(glueCatalog.tableExists(objectPath));
    }

    @Test
    public void testDropNonExistentTable() throws Exception {
        String sqlStatementCreateDatabase =
                String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String sqlStatementUseDatabase = String.format("USE %s", databaseName);
        String sqlStatementDropTable = String.format("DROP TABLE IF EXISTS %s", tableName);

        tEnv.executeSql(sqlStatementCreateDatabase).print();
        tEnv.executeSql(sqlStatementUseDatabase).print();

        // Should not throw exception due to IF EXISTS
        Assertions.assertDoesNotThrow(() -> tEnv.executeSql(sqlStatementDropTable).print());
    }

    @Test
    public void testCreateDatabaseInvalidName() {
        Assertions.assertThrows(
                Exception.class,
                () -> {
                    tEnv.executeSql("CREATE DATABASE invalid-name");
                });
    }

    @Test
    public void testCreateTableInvalidName() {
        String sqlStatementCreateDatabase =
                String.format("CREATE DATABASE IF NOT EXISTS %s", "test_db");
        tEnv.executeSql(sqlStatementCreateDatabase).print();
        Assertions.assertThrows(
                Exception.class,
                () -> {
                    tEnv.executeSql("CREATE TABLE test_db.invalid-name (id INT)");
                });
    }
}
