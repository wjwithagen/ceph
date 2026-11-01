#!/usr/bin/env bash
#
# test_smoke.sh
#
# Runs smoke.sh directly (bypassing ctest, whose environment handling
# doesn't reliably reach the ceph-mon/ceph-osd/ceph-mgr child processes
# spawned by ceph-helpers.sh). Requires CEPH_ROOT to already be set
# (e.g. in your shell init: `setenv CEPH_ROOT ~/Src/Ceph/ceph/ggate/ceph`).
#
# ceph-helpers.sh's main() unsets CEPH_ARGS and hardcodes CEPH_CONF=/dev/null,
# so neither can carry overrides into the daemons. It does NOT touch
# EXTRA_OPTS though (appended to every daemon's args), so
# erasure-code-dir / plugin-dir overrides (pointing at build/lib instead
# of the empty /usr/local/lib/ceph install-prefix) must be patched once,
# manually, into ceph-helpers.sh's EXTRA_OPTS default -- this script
# checks for that patch and tells you how to apply it if missing.
#
# Usage: run from the ceph checkout root (the fallback below assumes
# $PWD is CEPH_ROOT when CEPH_ROOT isn't set) -- NOT from build/:
#
#   bash test_smoke.sh
#
# Kills any leftover ceph-mon/ceph-osd from a previous aborted run,
# clears stale core files and the td/smoke test directory, then runs
# smoke.sh directly.

set -e

if [ -z "$CEPH_ROOT" ]; then
  echo "WARNING: CEPH_ROOT is not set. Falling back to the current directory:"
  echo "  $PWD"
  echo "Set it explicitly to avoid this guess, e.g.:"
  echo '  setenv CEPH_ROOT ~/Src/Ceph/ceph/ggate/ceph   # tcsh'
  CEPH_ROOT="$PWD"
fi

if [ ! -f "$CEPH_ROOT/src/test/smoke.sh" ]; then
  echo "ERROR: $CEPH_ROOT does not look like a ceph checkout"
  echo "(missing src/test/smoke.sh). Set CEPH_ROOT correctly and retry."
  exit 1
fi

BUILD_DIR="$CEPH_ROOT/build"

echo "--- killing leftover ceph-mon / ceph-osd ---"
pkill -9 -f ceph-mon || true
pkill -9 -f ceph-osd || true
sleep 2

echo "--- cleaning stale state ---"
cd "$BUILD_DIR"
rm -rf td/smoke
if ls *.core >/dev/null 2>&1; then
  rm -f *.core
fi

echo "--- checking ceph-helpers.sh EXTRA_OPTS default ---"
HELPERS="$CEPH_ROOT/qa/standalone/ceph-helpers.sh"
if ! grep -q "erasure-code-dir" "$HELPERS"; then
  echo "ERROR: $HELPERS line ~43 still has EXTRA_OPTS=\"\" unpatched."
  echo "Edit it once manually (in vi) to read:"
  echo '  EXTRA_OPTS="${EXTRA_OPTS:---erasure-code-dir='"$BUILD_DIR"'/lib --plugin-dir='"$BUILD_DIR"'/lib}"'
  echo "then re-run this script."
  exit 1
fi

echo "--- environment ---"
export LOCALRUN=1
export PATH="$BUILD_DIR/bin:$PATH"
echo "CEPH_ROOT=$CEPH_ROOT"
echo "LOCALRUN=$LOCALRUN"
echo "PATH=$PATH"

echo "--- running smoke.sh ---"
bash "$CEPH_ROOT/src/test/smoke.sh"
STATUS=$?

echo "--- smoke.sh exited with status $STATUS ---"
exit $STATUS

