#!/bin/bash -x

source $(dirname $0)/../ceph-helpers.sh

set -e
set -o functrace
PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '
SUDO=${SUDO:-sudo}

function check_no_osd_down()
{
    ! ceph osd dump | grep ' down '
}

function wait_no_osd_down()
{
  max_run=300
  for i in $(seq 1 $max_run) ; do
    if ! check_no_osd_down ; then
      echo "waiting for osd(s) to come back up ($i/$max_run)"
      sleep 1
    else
      break
    fi
  done
  check_no_osd_down
}

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}


TEMP_DIR=$(mktemp -d ${TMPDIR-/tmp}/cephtool.XXX)
trap "rm -fr $TEMP_DIR" 0

TMPFILE=$(mktemp $TEMP_DIR/test_invalid.XXX)

#
# retry_eagain max cmd args ...
#
# retry cmd args ... if it exits on error and its output contains the
# string EAGAIN, at most $max times
#
function retry_eagain()
{
    local max=$1
    shift
    local status
    local tmpfile=$TEMP_DIR/retry_eagain.$$
    local count
    for count in $(seq 1 $max) ; do
        status=0
        "$@" > $tmpfile 2>&1 || status=$?
        if test $status = 0 || 
            ! grep --quiet EAGAIN $tmpfile ; then
            break
        fi
        sleep 1
    done
    if test $count = $max ; then
        echo retried with non zero exit status, $max times: "$@" >&2
    fi
    cat $tmpfile
    rm $tmpfile
    return $status
}

#
# map_enxio_to_eagain cmd arg ...
#
# add EAGAIN to the output of cmd arg ... if the output contains
# ENXIO.
#
function map_enxio_to_eagain()
{
    local status=0
    local tmpfile=$TEMP_DIR/map_enxio_to_eagain.$$

    "$@" > $tmpfile 2>&1 || status=$?
    if test $status != 0 &&
        grep --quiet ENXIO $tmpfile ; then
        echo "EAGAIN added by $0::map_enxio_to_eagain" >> $tmpfile
    fi
    cat $tmpfile
    rm $tmpfile
    return $status
}

function check_response()
{
	expected_string=$1
	retcode=$2
	expected_retcode=$3
	if [ "$expected_retcode" -a $retcode != $expected_retcode ] ; then
		echo "return code invalid: got $retcode, expected $expected_retcode" >&2
		exit 1
	fi

	if ! grep --quiet -- "$expected_string" $TMPFILE ; then 
		echo "Didn't find $expected_string in output" >&2
		cat $TMPFILE >&2
		exit 1
	fi
}

function get_config_value_or_die()
{
  local target config_opt raw val

  target=$1
  config_opt=$2

  raw="`$SUDO ceph daemon $target config get $config_opt 2>/dev/null`"
  if [[ $? -ne 0 ]]; then
    echo "error obtaining config opt '$config_opt' from '$target': $raw"
    exit 1
  fi

  raw=`echo $raw | sed -e 's/[{} "]//g'`
  val=`echo $raw | cut -f2 -d:`

  echo "$val"
  return 0
}

function expect_config_value()
{
  local target config_opt expected_val val
  target=$1
  config_opt=$2
  expected_val=$3

  val=$(get_config_value_or_die $target $config_opt)

  if [[ "$val" != "$expected_val" ]]; then
    echo "expected '$expected_val', got '$val'"
    exit 1
  fi
}

function ceph_watch_start()
{
    local whatch_opt=--watch

    if [ -n "$1" ]; then
	whatch_opt=--watch-$1
    fi

    CEPH_WATCH_FILE=${TEMP_DIR}/CEPH_WATCH_$$
    ceph $whatch_opt > $CEPH_WATCH_FILE &
    CEPH_WATCH_PID=$!

    # wait until the "ceph" client is connected and receiving
    # log messages from monitor
    for i in `seq 3`; do
        grep -q "cluster" $CEPH_WATCH_FILE && break
        sleep 1
    done
}

function ceph_watch_wait()
{
    local regexp=$1
    local timeout=30

    if [ -n "$2" ]; then
	timeout=$2
    fi

    for i in `seq ${timeout}`; do
	grep -q "$regexp" $CEPH_WATCH_FILE && break
	sleep 1
    done

    kill $CEPH_WATCH_PID

    if ! grep "$regexp" $CEPH_WATCH_FILE; then
	echo "pattern ${regexp} not found in watch file. Full watch file content:" >&2
	cat $CEPH_WATCH_FILE >&2
	return 1
    fi
}

function test_mon_rebind()
{
  ceph osd set noup
  ceph osd down 0
  ceph osd dump | grep 'osd.0 down'
  ceph osd unset noup
  max_run=1000
  for ((i=0; i < $max_run; i++)); do
    if ! ceph osd dump | grep 'osd.0 up'; then
      echo "waiting for osd.0 to come back up ($i/$max_run)"
      sleep 1
    else
      break
    fi
  done
  ceph osd dump | grep 'osd.0 up'

#  ceph osd thrash 0

#  ceph osd dump | grep 'osd.0 up'
#  ceph osd out 0
#  ceph osd dump | grep 'osd.0.*out'
#  ceph osd in 0
#  ceph osd dump | grep 'osd.0.*in'
#  ceph osd find 0

  # make sure mark out preserves weight
#  ceph osd reweight osd.0 .5
#  ceph osd dump | grep ^osd.0 | grep 'weight 0.5'
#  ceph osd out 0
#  ceph osd in 0
#  ceph osd dump | grep ^osd.0 | grep 'weight 0.5'

  for id in `ceph osd ls` ; do
    retry_eagain 5 map_enxio_to_eagain ceph tell osd.$id version
  done

  ceph osd rm 0 2>&1 | grep 'EBUSY'
}

tests_to_run="mon_rebind"

for i in $tests_to_run; do
  if $sanity_check ; then
      check_no_osd_down
  fi
  set -x
  test_${i}
  set +x
done
if $sanity_check ; then
    check_no_osd_down
fi

set -x

echo OK
