SET timezone =  'UTC';

BEGIN;

CREATE EXTENSION pg_partition;

CREATE TABLE foo(id SERIAL, ts TIMESTAMP WITH TIME ZONE);

SELECT create_partitions_trigger_when(
    'public',
    'foo',
    'ts',
    date_trunc('month', '2014-02-13 00:00:00+00'::timestamptz),
    date_trunc('month', '2014-02-13 00:00:00+00'::timestamptz) + interval '1 month',
    interval '1 day'
);

ROLLBACK;
