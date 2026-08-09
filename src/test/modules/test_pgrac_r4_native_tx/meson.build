# Copyright (c) 2026, PostgreSQL Global Development Group

test_pgrac_r4_native_tx_sources = files(
  'test_pgrac_r4_native_tx.c',
)

if host_system == 'windows'
  test_pgrac_r4_native_tx_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_pgrac_r4_native_tx',
    '--FILEDESC', 'test_pgrac_r4_native_tx - native transaction authority probes',])
endif

test_pgrac_r4_native_tx = shared_module('test_pgrac_r4_native_tx',
  test_pgrac_r4_native_tx_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_pgrac_r4_native_tx

test_install_data += files(
  'test_pgrac_r4_native_tx.control',
  'test_pgrac_r4_native_tx--1.0.sql',
)

tests += {
  'name': 'test_pgrac_r4_native_tx',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_native_tx.pl',
    ],
  },
}
