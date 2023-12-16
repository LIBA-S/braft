workspace(name = "com_github_brpc_braft")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "com_google_googletest",
    strip_prefix = "googletest-0fe96607d85cf3a25ac40da369db62bbee2939a5",
    url = "https://github.com/google/googletest/archive/0fe96607d85cf3a25ac40da369db62bbee2939a5.tar.gz",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.6.1.3",
    sha256 = "9510dd2afc29e7245e9e884336f848c8a6600a14ae726adb6befdb4f786f0be2",
    type = "zip",
    url = "https://github.com/protocolbuffers/protobuf/archive/v3.6.1.3.zip",
)

http_archive(
    name = "com_github_gflags_gflags",
    strip_prefix = "gflags-46f73f88b18aee341538c0dfc22b1710a6abedef",
    url = "https://github.com/gflags/gflags/archive/46f73f88b18aee341538c0dfc22b1710a6abedef.tar.gz",
)

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

http_archive(
    name = "com_github_google_glog",
    build_file = "//:glog.BUILD",
    strip_prefix = "glog-a6a166db069520dbbd653c97c2e5b12e08a8bb26",
    url = "https://github.com/google/glog/archive/a6a166db069520dbbd653c97c2e5b12e08a8bb26.tar.gz"
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog",
)

http_archive(
    name = "com_github_google_leveldb",
    build_file = "//:leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    url = "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"
)

#http_archive(
#    name = "com_github_brpc_brpc",
#    sha256 = "c0168d22205724bfa1939c9ad79bd9f74a98e0bd05be9e8f5cc504ef44c676a1",
#    strip_prefix = "incubator-brpc-1.0.0-rc02",
#    url = "https://github.com/apache/incubator-brpc/archive/refs/tags/1.0.0-rc02.tar.gz"
#)

git_repository(
   name = "com_github_brpc_brpc",
   remote = "https://git.woa.com/elasticfaiss/brpc.git",
   branch = "vectordb",
)

bind(
    name = "brpc",
    actual = "@com_github_brpc_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_github_brpc_brpc//:butil",
)

new_local_repository(
    name = "openssl",
    path = "/usr",
    build_file = "//:openssl.BUILD",
)

bind(
    name = "ssl",
    actual = "@openssl//:ssl"
)

new_local_repository(
    name = "zlib",
    build_file = "//:zlib.BUILD",
    path = "/usr",
)
