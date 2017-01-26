#!/bin/sh -xve
NPROC=`sysctl -n hw.ncpu`

# we need bash first otherwise almost nothing will work
if [ ! -L /bin/bash ]; then
    echo install bash and link /bin/bash to /usr/local/bin/bash
    echo Run:
    echo     sudo pkg install bash
    echo     ln -s /usr/local/bin/bash /bin/bash
    exit 1
fi
if [ x"$1"x = x"--deps"x ]; then
    sudo ./install-deps.sh
fi
if ! grep -q ENODATA /usr/include/errno.h; then
    echo Need ENODATA in /usr/include/errno.h for cython compilations
    echo Please add it manually after ENOATTR with value 87
    echo '#define ENOATTR         87'
    exit 1
fi
if [ -x /usr/bin/getopt ] && [ x"`/usr/bin/getopt -v`"x == x" --"x ]; then
    echo fix getopt path
    echo Native FreeBSD getopt is not compatible with the Linux getopt that is
    echo expected with Ceph.
    echo Easiest is to rename/remove /usr/bin/getopt.
    echo     mv /usr/bin/getopt /usr/bin/getopt.freebsd
    exit 1
fi

if [ x"$CEPH_DEV"x != xx ]; then
    BUILDOPTS="$BUILDOPTS V=1 VERBOSE=1"
    CXX_FLAGS_DEBUG="-DCEPH_DEV"
    C_FLAGS_DEBUG="-DCEPH_DEV"
fi

#   To test with a new release Clang, use with cmake:
#	-D CMAKE_CXX_COMPILER="/usr/local/bin/clang++-devel" \
#	-D CMAKE_C_COMPILER="/usr/local/bin/clang-devel" \

if [ x"$CEPHDEV"x != xx ]; then
    BUILDOPTS="$BUILDOPTS V=1 VERBOSE=1"
fi
rm -rf build && ./do_cmake.sh "$*" \
	-D CMAKE_BUILD_TYPE=Debug \
	-D CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG -O0 -g" \
	-D CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG -O0 -g" \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_SYSTEM_BOOST=ON \
	-D WITH_CCACHE=ON \
	-D WITH_LTTNG=OFF \
	-D WITH_BLKID=OFF \
	-D WITH_FUSE=OFF \
	-D WITH_KRBD=OFF \
	-D WITH_XFS=OFF \
	-D WITH_KVS=OFF \
	-D WITH_MANPAGE=OFF \
	-D WITH_LIBCEPHFS=OFF \
	-D WITH_CEPHFS=OFF \
	2>&1 | tee cmake.log

cd build
gmake -j$NPROC $BUILDOPTS
gmake check 

