-- Function to doublecheck the correct amount of data has been uploaded 
-- will output 1 if the counts of the data matches the requirements

-- FUNCTION: schema.check_counts_within_10_percent_csv(text, text, text, bigint)

-- DROP FUNCTION IF EXISTS schema.check_counts_within_10_percent_csv(text, text, text, bigint);

CREATE OR REPLACE FUNCTION schema.check_counts_within_10_percent_csv(
	schema_name text,
	current_table_name text,
	load_table_name text,
	load_count bigint)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE current_table_count bigint;
DECLARE load_table_count bigint;
DECLARE allowed_range bigint;
BEGIN
    -- Dynamic SQL query that counts the loaded table to compare it to the load count
    RAISE NOTICE 'Comparing counts for %', load_table_name;
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, load_table_name) INTO load_table_count;

    RAISE NOTICE 'CSV row count %', load_count;
    RAISE NOTICE 'Load table row count %', load_table_count;
    -- First checks if the SQL load is the same as the csv row count
    IF load_count = load_table_count THEN   
        EXECUTE format('SELECT count(*) FROM %I.%I', schema_name, current_table_name) INTO current_table_count;
        -- Calculate the allowed range (10% of current_table_count)
        allowed_range = current_table_count * 0.10; 
        RAISE NOTICE '%.% count: %',schema_name, current_table_name, current_table_count;
        RAISE NOTICE '%.% count: %',schema_name, load_table_name, load_table_count;

        -- Check if load_table_count is within the allowed range
        IF load_table_count >= current_table_count - allowed_range AND load_table_count <= current_table_count + allowed_range THEN
            -- If within range, continues with output
            RAISE NOTICE 'Counts within 10%%';
            RETURN 1;
        ELSE
            -- Do something else if the counts are not within the range
            RAISE NOTICE 'Counts not within 10%%, please double check data';
            RETURN 0;
        END IF;
    ELSE
        -- End the function as the rows weren't loaded in properly
        RAISE NOTICE 'CSV count is not equal to the count of %.%, data not loaded correctly, please check', schema_name, load_table_name;
        RETURN 0;
    END IF;
END;
$BODY$;

ALTER FUNCTION schema.check_counts_within_10_percent_csv(text, text, text, bigint)
    OWNER TO dataninja;
