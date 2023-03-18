CREATE TEMPORARY TABLE datagen
    WITH (
        'connector' = 'datagen',
        'number-of-rows' = '${expectedNumOfElements}'
)
LIKE dynamo_db_table (EXCLUDING ALL);