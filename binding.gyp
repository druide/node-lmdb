{
  "targets": [
    {
      "target_name": "node-lmdb",
      "win_delay_load_hook": "false",
      "sources": [
        "libraries/liblmdb/mdb.c",
        "libraries/liblmdb/lmdb.h",
        "libraries/liblmdb/midl.h",
        "libraries/liblmdb/midl.c",
        "src/node-lmdb.cpp",
        "src/env.cpp",
        "src/misc.cpp",
        "src/txn.cpp",
        "src/dbi.cpp",
        "src/cursor.cpp"
      ],
      "include_dirs": [
        "<!(node -e \"require('nan')\")"
      ],
      "conditions": [
        ["OS=='linux'", {
          "ldflags": [
            "-O3",
            "-rdynamic"
          ],
          "cflags": [
            "-fPIC",
            "-fvisibility-inlines-hidden",
            "-O3",
            "-std=c++0x"
          ]
        }],
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CPLUSPLUSFLAGS" : ["-std=c++11","-mmacosx-version-min=10.7"],
            "OTHER_LDFLAGS": ["-std=c++11"],
          }
        }],
      ],
    }
  ]
}
