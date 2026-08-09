-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pgrac_r4_native_tx" to load this file. \quit

CREATE FUNCTION test_pgrac_r4_two_phase_is_prepared(xid)
RETURNS boolean
AS 'MODULE_PATHNAME', 'test_pgrac_r4_two_phase_is_prepared'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_pgrac_r4_current_xid()
RETURNS xid
AS 'MODULE_PATHNAME', 'test_pgrac_r4_current_xid'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION test_pgrac_r4_subtrans_parent(xid)
RETURNS xid
AS 'MODULE_PATHNAME', 'test_pgrac_r4_subtrans_parent'
LANGUAGE C STRICT PARALLEL UNSAFE;
