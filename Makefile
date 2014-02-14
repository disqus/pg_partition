EXTENSION    = pg_partition
PG_CONFIG    = pg_config

DATA         = $(wildcard sql/*.sql)
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test


PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
