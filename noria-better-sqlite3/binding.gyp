# ===
# This is the main GYP file, which builds noria-better-sqlite3 with SQLite and Noria.
# ===

{
  'includes': ['deps/common.gypi'],
  'targets': [
    {
      'target_name': 'better_sqlite3',
      'dependencies': ['deps/sqlite3.gyp:sqlite3'],
      'sources': ['src/better_sqlite3.cpp'],
      'include_dirs': ['noria-ffi'],
      'defines': ['SQLITE_ENABLE_SESSION', 'SQLITE_ENABLE_PREUPDATE_HOOK'],
      'cflags_cc': ['-std=c++20'],
      'xcode_settings': {
        'OTHER_CPLUSPLUSFLAGS': ['-std=c++20', '-stdlib=libc++'],
      },
      'msvs_settings': {
        'VCCLCompilerTool': {
          'AdditionalOptions': [
            '/std:c++20',
          ],
        },
      },
      'conditions': [
        ['OS=="linux"', {
          'ldflags': [
            '-Wl,-Bsymbolic',
            '-Wl,--exclude-libs,ALL',
            '-Wl,-rpath,<(module_root_dir)/../target/release',
          ],
          'libraries': [
            '-L<(module_root_dir)/../target/release',
            '-lnoria_ffi',
            '-lpthread',
            '-ldl',
            '-lm',
          ],
        }],
        ['OS=="mac"', {
          'libraries': [
            '-L<(module_root_dir)/../target/release',
            '-lnoria_ffi',
            '-lpthread',
            '-ldl',
            '-lm',
            '-framework Security',
            '-framework CoreFoundation',
          ],
        }],
      ],
    },
    {
      'target_name': 'test_extension',
      'dependencies': ['deps/sqlite3.gyp:sqlite3'],
      'conditions': [['sqlite3 == ""', { 'sources': ['deps/test_extension.c'] }]],
    },
  ],
}
