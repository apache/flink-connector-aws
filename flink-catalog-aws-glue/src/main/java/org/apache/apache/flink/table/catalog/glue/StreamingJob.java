package org.apache.apache.flink.table.catalog.glue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
//
//        // Create a GlueCatalog for connecting Flink with AWS Glue
//        Catalog glueCatalog = new GlueCatalog("glue_catalog", "default", "us-east-1");
//
//        // Register the Glue catalog in the TableEnvironment so that it can be accessed
//        tEnv.registerCatalog("glue_catalog", glueCatalog);
//
//        // Set the registered Glue catalog as the default catalog
//        tEnv.useCatalog("glue_catalog");
//        // Show the list of tables in the 'test' database
        tEnv.executeSql("SHOW TABLES").print();
//        // Show the list of databases in the catalog
        tEnv.executeSql("SHOW DATABASES").print();
        // Register the Glue catalog using SQL
        tEnv.executeSql(
                "CREATE CATALOG glue_catalog WITH (" +
                        "'type' = 'glue', " +
                        "'region' = 'us-east-1', " +
                        "'default-database' = 'default' " +
                        ")"
        );
        tEnv.executeSql("USE CATALOG glue_catalog;").print();
//
//        // Example SQL statements to interact with the Glue catalog and databases
//        // Drop the 'test' database if it exists and create it
        tEnv.executeSql("DROP DATABASE IF EXISTS test").print();
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test").print();
//
//        // Show the list of databases in the catalog
        tEnv.executeSql("SHOW DATABASES").print();
//
//        // Switch to the 'test' database
        tEnv.executeSql("USE test").print();
//
//        // Show the list of tables in the 'test' database
        tEnv.executeSql("SHOW TABLES").print();

//
        // Create a new table 'fran' with specified schema and configuration
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS gen (" +
                "  `order_Number` BIGINT," +
                "  `price` DECIMAL(32,2)," +
                "  `order_Time` TIMESTAMP(3) " +
                ")" +
                "WITH (" +
                "  'connector' = 'datagen'" +
                ");").print();
//
//        // Show the list of tables in the 'test' database again after creating the new table
        tEnv.executeSql("SHOW TABLES").print();


        // =========================================================================
        // TEST CASE-SENSITIVITY EXAMPLES - Uncomment one at a time to test
        // These examples test how the GlueCatalog handles column name case sensitivity
        // =========================================================================
        
        // NOTE: AWS Glue automatically converts column names to lowercase, but our connector 
        // preserves the original case in column properties for accurate JSON parsing.
        
        // -------------------------------------------------------------------------
        // EXAMPLE 1: ALL LOWERCASE COLUMNS
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_lowercase (" +
//                 "  `id` INT," +
//                 "  `username` VARCHAR(255)," +
//                 "  `timestamp` TIMESTAMP(3)," +
//                 "  `data_value` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT * FROM case_test_lowercase").print();
//
        // -------------------------------------------------------------------------
        // EXAMPLE 2: ALL UPPERCASE COLUMNS
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_uppercase (" +
//                 "  `ID` INT," +
//                 "  `USERNAME` VARCHAR(255)," +
//                 "  `TIMESTAMP` TIMESTAMP(3)," +
//                 "  `DATA_VALUE` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT ID FROM case_test_uppercase limit 10").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 3: MIXED CASE COLUMNS (camelCase)
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_mixed (" +
//                 "  `userId` INT," +
//                 "  `userName` VARCHAR(255)," +
//                 "  `eventTimestamp` TIMESTAMP(3)," +
//                 "  `dataValue` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT userId FROM case_test_mixed").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 4: MIXED CASE COLUMNS (PascalCase)
        // -------------------------------------------------------------------------
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS case_test_pascal (" +
//                 "  `UserId` INT," +
//                 "  `UserName` VARCHAR(255)," +
//                 "  `EventTimestamp` TIMESTAMP(3)," +
//                 "  `DataValue` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'datagen'" +
//                 ");").print();
//
//         tEnv.executeSql("SELECT UserId FROM case_test_pascal limit 5").print();
        
        // -------------------------------------------------------------------------
//         EXAMPLE 5: JSON SOURCE TEST - Tests real JSON parsing with case-sensitive fields
//         -------------------------------------------------------------------------
         // First create a test file with JSON data that has case-sensitive field names
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS json_test (" +
//                 "  `UserId` INT," +
//                 "  `UserName` VARCHAR(255)," +
//                 "  `Timestamp` TIMESTAMP(3)," +
//                 "  `DATA_VALUE` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
//
//         // Create the file manually with:
//         // echo '{"UserId":1,"UserName":"john","Timestamp":"2023-04-01 12:00:00","DATA_VALUE":"test"}' > /tmp/json_test.json
//         // echo '{"UserId":2,"UserName":"jane","Timestamp":"2023-04-01 12:01:00","DATA_VALUE":"test2"}' >> /tmp/json_test.json
//
//         // Query the data to verify case-sensitivity handling
//         tEnv.executeSql("SELECT UserId FROM json_test").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 6: NESTED JSON TEST - Tests case sensitivity in nested structures
        // -------------------------------------------------------------------------
        // // Create a table with a complex nested structure using different case styles
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS nested_json_test (" +
//                 "  `Id` INT," +
//                 "  `UserProfile` ROW<" +
//                 "     `FirstName` VARCHAR(255), " +
//                 "     `lastName` VARCHAR(255)" +
//                 "  >," +
//                 "  `EventData` ROW<" +
//                 "     `EventType` VARCHAR(50)," +
//                 "     `eventTimestamp` TIMESTAMP(3)," +
//                 "      `METADATA` MAP<VARCHAR(100), VARCHAR(255)>>" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/nested_json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
        // 
        // // Create a simpler nested JSON file manually:
        // cat > /tmp/nested_json_test.json << 'EOF'
        // {"Id":1,"UserProfile":{"FirstName":"John","lastName":"Doe"},"event_data":{"EventType":"LOGIN","eventTimestamp":"2023-04-01 12:00:00"},"metadata":{"sourceSystem":"WebApp","correlationId":"abc-123"}}
        // {"Id":2,"UserProfile":{"FirstName":"Jane","lastName":"Smith"},"event_data":{"EventType":"PURCHASE","eventTimestamp":"2023-04-01 12:30:00"},"metadata":{"sourceSystem":"MobileApp","correlationId":"def-456"}}
        // EOF
        // 
        // // Test basic query to verify all fields are accessible with correct case
//         tEnv.executeSql("SELECT " +
//                 "  `Id`, " +
//                 "  `UserProfile`.`FirstName`, " +
//                 "  `UserProfile`, " +
//                 "  `EventData`," +
//                 "  `EventData`.`METADATA`," +
//                 "  `EventData`.`METADATA`['SourceSystem'] as source_system, " +
//                 "  `EventData`.`METADATA`['correlation_id'] as correlation_id " +
//                 "FROM nested_json_test limit 1").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 6-A: ALTERNATIVE NESTED STRUCTURE - First level only
        // -------------------------------------------------------------------------
        // // Create a simpler table with first-level nested fields only
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS flat_json_test (" +
//                 "  `UserId` INT," +
//                 "  `UserName` VARCHAR(255)," +
//                 "  `EventType` VARCHAR(50)," +
//                 "  `TimeStamp` TIMESTAMP(3)," +
//                 "  `DATA_VALUE` VARCHAR(255)" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/flat_json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
        // 
        // // Create a simple JSON file manually:
        // // echo '{"UserId":1,"UserName":"john","EventType":"LOGIN","TimeStamp":"2023-04-01 12:00:00","DATA_VALUE":"test"}' > /tmp/flat_json_test.json
        // // echo '{"UserId":2,"UserName":"jane","EventType":"LOGOUT","TimeStamp":"2023-04-01 12:30:00","DATA_VALUE":"test2"}' >> /tmp/flat_json_test.json
        // 
        // // Query the table with mixed case columns
//         tEnv.executeSql("SELECT `UserId`, `UserName`, `EventType`, `TimeStamp`, `DATA_VALUE` FROM flat_json_test").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 6-B: MAP STRUCTURE APPROACH - Using MAP instead of nested ROWs
        // -------------------------------------------------------------------------
        // // Create a table using MAPs instead of nested ROWs
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS map_json_test (" +
//                 "  `Id` INT," +
//                 "  `Profile` MAP<VARCHAR(100), VARCHAR(255)>," +
//                 "  `EventInfo` MAP<VARCHAR(100), VARCHAR(255)>" +
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/map_json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
        // 
        // // Create a JSON file manually with map-like structure:
        // // echo '{"Id":1,"Profile":{"FirstName":"John","LastName":"Doe"},"EventInfo":{"Type":"LOGIN","Time":"2023-04-01 12:00:00"}}' > /tmp/map_json_test.json
        // // echo '{"Id":2,"Profile":{"FirstName":"Jane","LastName":"Smith"},"EventInfo":{"Type":"PURCHASE","Time":"2023-04-01 12:30:00"}}' >> /tmp/map_json_test.json
        // 
        // // Query the table using map access
//         tEnv.executeSql("SELECT " +
//                 "  `Id`, " +
//                 "  `Profile`['FirstName'] as first_name, " +
//                 "  `Profile`['LastName'] as last_name, " +
//                 "  `EventInfo`['Type'] as event_type, " +
//                 "  `EventInfo`['Time'] as event_time " +
//                 "FROM map_json_test").print();
        
        // -------------------------------------------------------------------------
        // EXAMPLE 6-C: STRING JSON APPROACH - Store nested JSON as STRING
        // -------------------------------------------------------------------------
        // // Create a table with JSON stored as STRING
//         tEnv.executeSql("CREATE TABLE IF NOT EXISTS string_json_test (" +
//                 "  `Id` INT," +
//                 "  `UserData` STRING," +  // Store JSON as string
//                 "  `EventData` STRING" +  // Store JSON as string
//                 ")" +
//                 "WITH (" +
//                 "  'connector' = 'filesystem'," +
//                 "  'path' = '/tmp/string_json_test.json'," +
//                 "  'format' = 'json'" +
//                 ");").print();
        // 
        // // Create a JSON file manually:
        // // echo '{"Id":1,"UserData":"{\"FirstName\":\"John\",\"LastName\":\"Doe\"}","EventData":"{\"EventType\":\"LOGIN\",\"TimeStamp\":\"2023-04-01 12:00:00\"}"}' > /tmp/string_json_test.json
        // // echo '{"Id":2,"UserData":"{\"FirstName\":\"Jane\",\"LastName\":\"Smith\"}","EventData":"{\"EventType\":\"PURCHASE\",\"TimeStamp\":\"2023-04-01 12:30:00\"}"}' >> /tmp/string_json_test.json
        // 
        // // Query with JSON functions to extract fields
//         tEnv.executeSql("SELECT " +
//                 "  `Id`, " +
//                 "  JSON_VALUE(`UserData`, '$.FirstName') as first_name, " +
//                 "  JSON_VALUE(`UserData`, '$.LastName') as last_name, " +
//                 "  JSON_VALUE(`EventData`, '$.EventType') as event_type, " +
//                 "  JSON_VALUE(`EventData`, '$.TimeStamp') as event_time " +
//                 "FROM string_json_test").print();

    }
}