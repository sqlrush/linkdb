# src/test/modules/test_pgrac_r4_native_tx/Makefile

MODULE_big = test_pgrac_r4_native_tx
OBJS = \
	$(WIN32RES) \
	test_pgrac_r4_native_tx.o
PGFILEDESC = "test_pgrac_r4_native_tx - native transaction authority probes"

EXTENSION = test_pgrac_r4_native_tx
DATA = test_pgrac_r4_native_tx--1.0.sql

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pgrac_r4_native_tx
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
