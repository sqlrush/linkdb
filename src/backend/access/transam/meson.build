# Copyright (c) 2022-2023, PostgreSQL Global Development Group

backend_sources += files(
  'clog.c',
  'commit_ts.c',
  'generic_xlog.c',
  'multixact.c',
  'parallel.c',
  'rmgr.c',
  'slru.c',
  'subtrans.c',
  'timeline.c',
  'transam.c',
  'twophase.c',
  'twophase_rmgr.c',
  'varsup.c',
  'xact.c',
  'xlog.c',
  'xlogarchive.c',
  'xlogbackup.c',
  'xlogfuncs.c',
  'xloginsert.c',
  'xlogprefetcher.c',
  'xlogrecovery.c',
  'xlogstats.c',
  'xlogutils.c',
)

# used by frontend programs to build a frontend xlogreader
xlogreader_sources = files(
  'xlogreader.c',
)

backend_sources += xlogreader_sources

# PGRAC Stage 8 JIT Task3: build the real frontend decoder edge test.
cluster_page_edge_test = executable('cluster_page_edge_test',
  files('t/cluster_page_edge_test.c') + xlogreader_sources,
  dependencies: [frontend_code, lz4, zstd],
  include_directories: include_directories('../../../test/cluster_unit'),
  c_args: ['-DFRONTEND'],
  kwargs: default_bin_args + {
    'install': false,
  },
)
test('cluster_page_edge_test', cluster_page_edge_test)
