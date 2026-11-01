#!/usr/bin/env bash
#
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
set -ex
source $(dirname $0)/detect-build-env-vars.sh

test `uname` = FreeBSD

# Sudo is required to be able to (un)load the GEOM gate kernel module

# Preload sudo credentials so later commands needing sudo don't block
# waiting for an interactive password prompt (ctest runs without a tty).
# Abort clearly instead of silently continuing if this fails, e.g.
# because NOPASSWD isn't configured in sudoers for this user.
if ! timeout -s INT -k 30 15 sudo -nE true; then
    echo "FATAL: sudo preload failed - check NOPASSWD sudoers entry for this user" >&2
    exit 1
fi
echo Sudo preloaded

CEPH_CLI_TEST_DUP_COMMAND=1 \
MON=1 OSD=3 MDS=0 MGR=1 CEPH_PORT=7206 $CEPH_ROOT/src/test/vstart_wrapper.sh \
    $CEPH_ROOT/qa/workunits/rbd/rbd-ggate.sh \
