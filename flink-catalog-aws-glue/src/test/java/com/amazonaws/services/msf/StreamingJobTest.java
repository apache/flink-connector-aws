package com.amazonaws.services.msf;

import com.amazonaws.services.msf.operations.FakeGlueClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

import static org.junit.jupiter.api.Assertions.*;

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
        miniCluster = new MiniCluster(new MiniClusterConfiguration.Builder().setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
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
        glueCatalog = new GlueCatalog("glueCatalog", defaultDB, region,fakeGlueClient);

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
        String SQLStatement = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(SQLStatement).print();
        assertTrue(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testCreateDatabaseAlreadyExists() throws Exception {
        // First create the database
        String SQLStatement = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(SQLStatement).print();
        
        // Try to create it again - should not throw exception due to IF NOT EXISTS
        assertDoesNotThrow(() -> tEnv.executeSql(SQLStatement).print());
    }

    @Test
    public void testDropDatabase() throws Exception {
        String SQLCreateDatabaseStatement = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        tEnv.executeSql(SQLCreateDatabaseStatement).print();
        String SQLDropDatabaseStatement = String.format("DROP DATABASE %s", databaseName);
        tEnv.executeSql(SQLDropDatabaseStatement).print();

        assertFalse(glueCatalog.databaseExists(databaseName));
    }

    @Test
    public void testDropNonExistentDatabase() throws Exception {
        String SQLDropDatabaseStatement = String.format("DROP DATABASE IF EXISTS %s", databaseName);
        assertDoesNotThrow(() -> tEnv.executeSql(SQLDropDatabaseStatement).print());
    }

    @Test
    public void testCreateTable() throws Exception {
        String SQLStatementCreateDatabase = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String SQLStatementUseDatabase = String.format("USE %s", databaseName);
        String SQLStatementCreateTable = String.format("CREATE TABLE IF NOT EXISTS %s (" +
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
                ");", tableName);

        tEnv.executeSql(SQLStatementCreateDatabase).print();
        tEnv.executeSql(SQLStatementUseDatabase).print();
        tEnv.executeSql(SQLStatementCreateTable).print();
        
        ObjectPath objectPath = new ObjectPath(databaseName, tableName);
        assertTrue(glueCatalog.tableExists(objectPath));
    }

    @Test
    public void testCreateTableAlreadyExists() throws Exception {
        String SQLStatementCreateDatabase = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String SQLStatementUseDatabase = String.format("USE %s", databaseName);
        String SQLStatementCreateTable = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                "  `user_id` STRING" +
                ")" +
                "WITH (" +
                "  'connector' = 'kinesis'," +
                "  'stream.arn' = 'arn:aws:kinesis:us-east-1:116394013621:stream/input'," +
                "  'aws.region' = 'us-east-1'," +
                "  'source.init.position' = 'TRIM_HORIZON'," +
                "  'format' = 'json'" +
                ");", tableName);

        tEnv.executeSql(SQLStatementCreateDatabase).print();
        tEnv.executeSql(SQLStatementUseDatabase).print();
        tEnv.executeSql(SQLStatementCreateTable).print();
        
        // Try to create it again - should not throw exception due to IF NOT EXISTS
        assertDoesNotThrow(() -> tEnv.executeSql(SQLStatementCreateTable).print());
    }

    @Test
    public void testDropTable() throws Exception {
        String SQLStatementCreateDatabase = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String SQLStatementUseDatabase = String.format("USE %s", databaseName);
        String SQLStatementCreateTable = String.format("CREATE TABLE IF NOT EXISTS %s (" +
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
                ");", tableName);

        String SQLStatementDropTable = String.format("DROP TABLE %s", tableName);

        tEnv.executeSql(SQLStatementCreateDatabase).print();
        tEnv.executeSql(SQLStatementUseDatabase).print();
        tEnv.executeSql(SQLStatementCreateTable).print();
        
        ObjectPath objectPath = new ObjectPath(databaseName, tableName);
        tEnv.executeSql(SQLStatementDropTable).print();

        assertFalse(glueCatalog.tableExists(objectPath));
    }

    @Test
    public void testDropNonExistentTable() throws Exception {
        String SQLStatementCreateDatabase = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        String SQLStatementUseDatabase = String.format("USE %s", databaseName);
        String SQLStatementDropTable = String.format("DROP TABLE IF EXISTS %s", tableName);

        tEnv.executeSql(SQLStatementCreateDatabase).print();
        tEnv.executeSql(SQLStatementUseDatabase).print();
        
        // Should not throw exception due to IF EXISTS
        assertDoesNotThrow(() -> tEnv.executeSql(SQLStatementDropTable).print());
    }

    @Test
    public void testCreateDatabaseInvalidName() {
        assertThrows(Exception.class, () -> {
            tEnv.executeSql("CREATE DATABASE invalid-name");
        });
    }

    @Test
    public void testCreateTableInvalidName() {
        String SQLStatementCreateDatabase = String.format("CREATE DATABASE IF NOT EXISTS %s", "test_db");
        tEnv.executeSql(SQLStatementCreateDatabase).print();
        assertThrows(Exception.class, () -> {
            tEnv.executeSql("CREATE TABLE test_db.invalid-name (id INT)");
        });
    }
}
