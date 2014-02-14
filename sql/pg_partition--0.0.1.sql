CREATE OR REPLACE FUNCTION implementation_error(ANYELEMENT)
RETURNS ANYELEMENT
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    RAISE EXCEPTION '%', $1;
END;
$$;

CREATE OR REPLACE FUNCTION create_partitions(
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
    SELECT
        i,
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
        WHEN in_interval >= '1 year' THEN to_char(i, 'YYYY')
        WHEN in_interval >= '1 month' THEN to_char(i, 'YYYYMM')
        WHEN in_interval >= '1 day' THEN to_char(i, 'YYYYMMDD')
        WHEN in_interval >= '1 hour' THEN to_char(i, 'YYYYMMDDHH24')
        WHEN in_interval >= '1 minute' THEN to_char(i, 'YYYYMMDDHH24MI')
        WHEN in_interval >= '1 second' THEN to_char(i, 'YYYYMMDDHH24MISS')
        ELSE implementation_error('Time slices less than a second are way too fine. :P'::text)
        END AS "starter"
    FROM
    generate_series(in_start, in_end, in_interval) AS s(i)
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

CREATE OR REPLACE FUNCTION %I()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO %I VALUES (NEW.*);
    RETURN NULL;
END$$;

CREATE TRIGGER %I
    BEFORE INSERT ON %I
    FOR EACH ROW
    WHEN (NEW.%I >= %L AND NEW.%I < %L)
    EXECUTE PROCEDURE %I();
',
    in_table || '_' || "starter",
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table,
    in_table || '_' || "starter", in_column,
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table || '_' || "starter",
    in_table || '_' || "starter", in_table || '_' || "starter",
    in_table || '_' || "starter" || '_insert', in_table, in_column, i,
    in_column,  COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table || '_' || "starter"
    )::text
FROM t;
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
    SELECT
        i,
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
        WHEN in_interval >= '1 year' THEN to_char(i, 'YYYY')
        WHEN in_interval >= '1 month' THEN to_char(i, 'YYYYMM')
        WHEN in_interval >= '1 day' THEN to_char(i, 'YYYYMMDD')
        WHEN in_interval >= '1 hour' THEN to_char(i, 'YYYYMMDDHH24')
        WHEN in_interval >= '1 minute' THEN to_char(i, 'YYYYMMDDHH24MI')
        WHEN in_interval >= '1 second' THEN to_char(i, 'YYYYMMDDHH24MISS')
        ELSE implementation_error('Time slices less than a second are way too fine. :P'::text)
        END AS "starter"
    FROM
    generate_series(in_start, in_end, in_interval) AS s(i)
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
    in_table || '_' || "starter",
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table,
    in_table || '_' || "starter", in_column,
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table || '_' || "starter",
    in_table || '_' || "starter",
    in_table,
    in_column, i, in_column, COALESCE(lead(i,1) OVER (), 'infinity'),
    in_table || '_' || "starter"
    )::text
FROM t;
$q$;

















