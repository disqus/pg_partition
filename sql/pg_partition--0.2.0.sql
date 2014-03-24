CREATE OR REPLACE FUNCTION implementation_error(ANYELEMENT)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    RAISE EXCEPTION '%', $1;
    RETURN false;
END;
$$;

CREATE OR REPLACE FUNCTION validate_inputs(
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    in_start TIMESTAMP WITH TIME ZONE,
    in_end TIMESTAMP WITH TIME ZONE,
    in_interval INTERVAL
)
RETURNS boolean
STRICT
LANGUAGE sql
AS $q$
    SELECT
        CASE
        WHEN (SELECT setting <> 'UTC' FROM pg_catalog.pg_settings WHERE "name" ~* '^timezone$')
            THEN implementation_error($$SET timezone = 'UTC'; /* You can run other client time zones some other time. */$$::text)
        WHEN
            NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE
                    table_schema = in_schema AND
                    table_name = in_table AND
                    column_name = in_column AND data_type = 'timestamp with time zone'
            )
            THEN implementation_error(format('%s.%s.%s must be of type TIMESTAMP WITH TIME ZONE', in_schema, in_table, in_column))
        WHEN (in_start AT TIME ZONE 'UTC') <> date_trunc('day', in_start AT TIME ZONE 'UTC')
            THEN implementation_error('You really want UTC midnight timestamps to start.'::text)
        WHEN in_interval <> ALL(ARRAY[
                '1 year',
                '6 month',
                '3 month',
                '1 month',
                '1 week',
                '1 day',
                '1 hour',
                '1 minute',
                '1 second'
                ]::interval[])
            THEN implementation_error('Use your imagination for something other than partition size for a time series.'::text)
        ELSE true
        END;
$q$;

CREATE OR REPLACE FUNCTION partition_name_format(in_interval INTERVAL)
RETURNS TEXT
LANGUAGE SQL
AS $$
    SELECT CASE
        WHEN in_interval >= '1 year' THEN 'YYYY'
        WHEN in_interval >= '1 month' THEN 'YYYYMM'
        WHEN in_interval >= '1 day' THEN 'YYYYMMDD'
        WHEN in_interval >= '1 hour' THEN 'YYYYMMDDHH24'
        WHEN in_interval >= '1 minute' THEN 'YYYYMMDDHH24MI'
        WHEN in_interval >= '1 second' THEN 'YYYYMMDDHH24MISS'
    END;
$$;

CREATE OR REPLACE FUNCTION get_timestamps (
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    in_start TIMESTAMP WITH TIME ZONE,
    in_end TIMESTAMP WITH TIME ZONE,
    in_interval INTERVAL
)
RETURNS TABLE (
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    the_start TIMESTAMPTZ,
    the_end TIMESTAMPTZ,
    the_suffix TEXT
)
STRICT
LANGUAGE sql
AS $$
SELECT
    in_schema,
    in_table,
    in_column,
    the_start AS the_tstz,
    COALESCE(lead(the_tstz1) OVER (), the_tstz + (the_tstz - lag(the_tstz1) OVER ())) AS the_end,
    to_char(the_tstz partition_name_format(in_interval)) AS the_suffix
FROM
    generate_series(in_start, in_end, in_interval) AS s(i)
WHERE 
    validate_inputs(
        in_schema,
        in_table,
        in_column,
        in_start,
        in_end,
        in_interval
    )
AND
    i < in_end
$$;

CREATE OR REPLACE FUNCTION create_partition_table(
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    the_start TIMESTAMPTZ,
    the_end TIMESTAMPTZ,
    the_suffix TEXT
)
RETURNS TEXT
STRICT
LANGUAGE sql
AS $q$
SELECT format(
        $$CREATE TABLE %I(
    CHECK( %s >= %L AND %I < %L )
) INHERITS (%I);$$,
    in_table || '_' || the_suffix,
    in_column, the_start, in_column, the_end,
    in_table
)
$$;

CREATE OR REPLACE FUNCTION create_partitions_trigger_when(
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    in_start TIMESTAMP WITH TIME ZONE,
    in_end TIMESTAMP WITH TIME ZONE,
    in_interval INTERVAL
)
RETURNS SETOF TEXT
LANGUAGE SQL
AS $q$
WITH t AS (
    SELECT i
    FROM
    generate_series(in_start, in_end, in_interval) AS s(i)
    WHERE 
        validate_inputs(
            in_schema,
            in_table,
            in_column,
            in_start,
            in_end,
            in_interval
        )
)
SELECT
    format(
        $$CREATE TABLE %I(
    CHECK( %s >= %L AND %I < %L )
) INHERITS (%I);

CREATE INDEX ON %I(%I);

WITH t AS (
    DELETE FROM ONLY %I
    WHERE %s >= %L AND %I < %L
    RETURNING *
)
INSERT INTO %I
SELECT * FROM t;

CREATE OR REPLACE FUNCTION %I()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $t$
BEGIN
    INSERT INTO %I VALUES (NEW.*);
    RETURN NULL;
END$t$;

CREATE TRIGGER %I
    BEFORE INSERT ON %I
    FOR EACH ROW
    WHEN (NEW.%I >= %L AND NEW.%I < %L)
    EXECUTE PROCEDURE %I();
$$,
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table,
    in_table || '_' || to_char(i, partition_name_format(in_interval)), in_column,
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_table || '_' || to_char(i, partition_name_format(in_interval)) || '_insert', in_table, in_column, i,
    in_column,  COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table || '_' || to_char(i, partition_name_format(in_interval))
    )::text
FROM
    t
WHERE
    i < in_end;
$q$;

CREATE OR REPLACE FUNCTION create_partitions_rule(
    in_schema TEXT,
    in_table TEXT,
    in_column TEXT,
    in_start TIMESTAMP WITH TIME ZONE,
    in_end TIMESTAMP WITH TIME ZONE,
    in_interval INTERVAL
)
RETURNS SETOF text
STRICT
LANGUAGE sql
AS $q$
WITH t AS (
    SELECT i
    FROM
    generate_series(in_start, in_end, in_interval) AS s(i)
    WHERE 
        validate_inputs(
            in_schema,
            in_table,
            in_column,
            in_start,
            in_end,
            in_interval
        )
)
SELECT
    format(
        'CREATE TABLE %I(
    CHECK( %s >= %L AND %I < %L )
) INHERITS (%I);

CREATE INDEX ON %I(%I);

WITH t AS (
    DELETE FROM ONLY %I
    WHERE %s >= %L AND %I < %L
    RETURNING *
)
INSERT INTO %I
SELECT * FROM t;

CREATE RULE %I AS
    ON INSERT TO %I
    WHERE (NEW.%I >= %L AND NEW.%I < %L)
    DO INSTEAD INSERT INTO %I VALUES (NEW.*);

',
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table,
    in_table || '_' || to_char(i, partition_name_format(in_interval)), in_column,
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_table || '_' || to_char(i, partition_name_format(in_interval)),
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), i + (i - lag(i,1) OVER ())),
    in_table || '_' || to_char(i, partition_name_format(in_interval))
    )::text
FROM t
WHERE i < in_end;
$q$;
