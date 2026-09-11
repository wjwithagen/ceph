#!/bin/sh -xve
export NPROC=`sysctl -n hw.ncpu`

$SUDO

CLEAN=1
if [ x"$1"x = x"--incremental"x ]; then
    CLEAN=0
    shift
fi

if [ x"$1"x = x"--deps"x ]; then
    $SUDO ./install-deps.sh
    shift
fi

if [ x"$CEPH_DEV"x != xx ]; then
    BUILDOPTS="$BUILDOPTS V=1 VERBOSE=1"
    CXX_FLAGS_DEBUG="-DCEPH_DEV"
    C_FLAGS_DEBUG="-DCEPH_DEV"
fi

COMPILE_FLAGS="-O0 -g -Wno-unused-command-line-argument"
CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG $COMPILE_FLAGS"
CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG $COMPILE_FLAGS"

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

if [ "$CLEAN" = "1" ]; then
    echo "Full clean build requested"
    if [ -d ${BUILD_DIR}.old ]; then
        $SUDO mv ${BUILD_DIR}.old ${BUILD_DIR}.del
        $SUDO rm -rf ${BUILD_DIR}.del &
    fi
    if [ -d ${BUILD_DIR} ]; then
        $SUDO mv ${BUILD_DIR} ${BUILD_DIR}.old
    fi

    ./do_cmake.sh "$*" \
            -D WITH_CCACHE=ON \
            -D CMAKE_BUILD_TYPE=Debug \
            -D CMAKE_CXX_FLAGS_DEBUG="$CMAKE_CXX_FLAGS_DEBUG" \
            -D CMAKE_C_FLAGS_DEBUG="$CMAKE_C_FLAGS_DEBUG" \
            -D ENABLE_GIT_VERSION=OFF \
            -D WITH_SYSTEMD=OFF \
            -D WITH_SYSTEM_BOOST=ON \
            -D WITH_SYSTEM_NPM=ON \
            -D WITH_LTTNG=OFF \
            -D WITH_BABELTRACE=OFF \
            -D WITH_CRIMSON=OFF \
            -D WITH_FUSE=OFF \
            -D WITH_KRBD=OFF \
            -D WITH_XFS=OFF \
            -D WITH_KVS=ON \
            -D CEPH_MAN_DIR=man \
            -D WITH_LIBCEPHFS=ON -D WITH_LIBCEPHFS_PROXY=OFF \
            -D WITH_CEPHFS=ON \
            -D WITH_MGR=ON -D WITH_MGR_DASHBOARD_FRONTEND=OFF \
            -D WITH_RDMA=OFF \
            -D WITH_SPDK=OFF \
            -D WITH_JAEGER=OFF \
            -D WITH_BREAKPAD=OFF \
            -D WITH_LIBURING=OFF \
            -D WITH_RADOSGW_AMQP_ENDPOINT=OFF \
            -D WITH_RADOSGW_KAFKA_ENDPOINT=OFF \
            -D WITH_RADOSGW_ARROW_FLIGHT=OFF \
            -D WITH_RADOSGW_SELECT_PARQUET=OFF \
            -D WITH_RADOSGW_POSIX=OFF \
            -D WITH_NVMEOF_GATEWAY_MONITOR_CLIENT=OFF \
            -D WITH_QATLIB=OFF -D WITH_QATZIP=OFF \
            2>&1 | tee cmake.log
else
    echo "Incremental build requested — skipping cmake reconfigure"
fi

echo -n "start building: "; date
printenv

cd ${BUILD_DIR}

BUILD_STATUS=0
if [ -f build.ninja ]; then
    ninja -j${NPROC} 2>&1 | tee ../build.log || BUILD_STATUS=1
    ninja tests 2>&1 | tee -a ../build.log || BUILD_STATUS=1
else
    gmake -j${NPROC} V=1 VERBOSE=1 2>&1 | tee ../build.log || BUILD_STATUS=1
    gmake tests 2>&1 | tee -a ../build.log || BUILD_STATUS=1
fi

if [ ${BUILD_STATUS} -ne 0 ]; then
    echo "BUILD FAILED — see build.log — skipping tests"
    exit ${BUILD_STATUS}
fi

echo -n "start testing: "; date

TEST_STATUS=0
ctest -j ${NPROC} --output-on-failure 2>&1 | tee ../ctest.log || TEST_STATUS=1

if [ ${TEST_STATUS} -ne 0 ]; then
    echo "Some tests failed, cleaning up leftovers and retrying failed tests"
    killall ceph-osd 2>/dev/null || true
    killall ceph-mgr 2>/dev/null || true
    killall ceph-mds 2>/dev/null || true
    killall ceph-mon 2>/dev/null || true
    rm -rf td/* /tmp/td src/test/td/* 2>/dev/null || true
    rm -rf /tmp/ceph-asok.* /tmp/cores.* /tmp/*.core 2>/dev/null || true

    ctest --output-on-failure --rerun-failed 2>&1 | tee -a ../ctest.log || TEST_STATUS=1
fi

rm -rf /tmp/tmp* /tmp/foo /tmp/pip* /tmp/big* /tmp/pymp* $TMPDIR 2>/dev/null || true

echo -n "Ended: "; date

if [ ${TEST_STATUS} -ne 0 ]; then
    echo "FINAL RESULT: TESTS FAILED"
    exit 1
fi

echo "FINAL RESULT: SUCCESS"
exit 0

