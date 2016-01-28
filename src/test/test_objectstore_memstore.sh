#!/bin/sh -ex

rm -rf store_test_temp_dir
$CEPH_BIN/ceph_test_objectstore --gtest_filter=\*/0

echo OK
