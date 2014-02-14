pg_partition
============

Synopsis
--------

  Partition your PostgreSQL tables.

Description
-----------

Starting with TIMESTAMP WITH TIME ZONE because it's a common case.

Usage
-----

  psql -o partitions_table.sql -AtXc 'create_partitions(in_schema, in_table, in_column, in_start, in_end, in_interval)'
  $EDITOR partitions_table.sql
  psql -1f partitions_table.sql


Support
-------

  https://github.com/disqus/pg_partition/issues

Author
------

David Fetter <david.fetter@disqus.com>

Copyright and License
---------------------

Copyright (c) 2014 DISQUS

