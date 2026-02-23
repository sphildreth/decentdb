{
  "targets": [
    {
      "target_name": "decentdb_native",
      "sources": [
        "src/addon.c",
        "src/native_lib.c"
      ],
      "include_dirs": [
        "<(module_root_dir)/src"
      ],
      "cflags": ["-std=c11"],
      "conditions": [
        ["OS=='linux'", {
          "libraries": ["-ldl"]
        }],
        ["OS=='mac'", {
          "libraries": []
        }]
      ]
    }
  ]
}
