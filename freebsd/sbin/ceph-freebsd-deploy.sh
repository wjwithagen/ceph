#!/usr/bin/env bash
#
# ceph-freebsd-deploy.sh -- create the first node of a Ceph cluster on FreeBSD
#
# Does, in this order:
#   1. checks the system, and picks the monitor address: the IPv4 address of
#      this server (asks which one when there are several)
#   2. lets you pick the OSD disks, and optionally the SSDs for the BlueStore
#      DB and WAL, with dialog(1); or takes them from the command line
#   3. shows the plan and asks for confirmation (the disks are wiped)
#   4. writes ceph.conf
#   5. creates the monitor, admin and bootstrap-osd keyrings and the monmap
#   6. creates and starts the first monitor, and waits for quorum
#   7. creates and starts a manager
#   8. partitions the DB/WAL devices, prepares the OSDs with
#      "ceph-volume zfs prepare" and starts them
#   9. shows the cluster status, and which keyrings to copy to other nodes
#      and which to remove again once more monitors exist
#
# It uses the rc.d scripts of the net/ceph21 port (ceph_mon, ceph_mgr,
# ceph_osd) and enables them in /etc/rc.conf with sysrc(8).
#
# It refuses to run when this node already has a monitor or a ceph.conf; it
# never overwrites an existing cluster.
#
# DB and WAL devices: "ceph-volume zfs prepare --block.db <dev>" puts a
# zpool on the whole device it is given, one per OSD. To let one SSD serve
# several OSDs, this script splits every selected device into equal GPT
# partitions, one per OSD, and hands those partitions to ceph-volume.
#
# Examples:
#   ceph-freebsd-deploy.sh
#   ceph-freebsd-deploy.sh --mon-ip 192.0.2.11 --public-network 192.0.2.0/24
#   ceph-freebsd-deploy.sh --disks "da0 da1 da2 da3" --db-disks nda0 --dry-run

set -euo pipefail

# Where Ceph is installed; the rc.d scripts read the same variable.
CEPH_PREFIX="${CEPH_PREFIX:-/usr/local}"
# ceph-volume is installed in sbin, not in bin.
CEPH_VOLUME="${CEPH_VOLUME:-${CEPH_PREFIX}/sbin/ceph-volume}"
CLUSTER="ceph"
MON_ID=""
MON_IP=""
MON_IFACE=""
PUBLIC_NETWORK=""
FSID=""
DISKS=""
DB_DISKS=""
WAL_DISKS=""
NO_OSDS="no"
PREPARE_ARGS=""
SINGLE_NODE="no"
DRY_RUN="no"
ASSUME_YES="no"
QUORUM_TIMEOUT=120
OSD_TIMEOUT=180
# A WAL partition does not need to be large.
WAL_CAP_MB=4096
DIALOG=""

# STAGE is "prep" until the monitor store is created; after that the node is
# partly set up and nothing is removed automatically.
STAGE="prep"
declare -a CREATED=()
declare -a DISK_LIST=() DB_DEVS=() WAL_DEVS=() DB_TARGET=() WAL_TARGET=()
declare -a PART_PLAN=() DISK_ORDER=()
declare -A DISK_BYTES=() DISK_SIZE=() DISK_TYPE=() DISK_DESC=()

usage()
{
	cat << EOF
usage: $(basename "$0") [options]

  --mon-ip <ip>             IP address of the first monitor. Default: the IPv4
                            address of this server; when there are several,
                            the one on the default route interface, else dialog
                            asks which one.
  --mon-id <id>             monitor and manager id (default: short hostname)
  --public-network <cidr>   public_network for ceph.conf. Default: the network
                            of the automatically chosen address, with its
                            netmask; not set when --mon-ip is given.
  --disks "<d1> <d2> ..."   disks to turn into OSDs, e.g. "da0 da1"; wiped.
                            Without --disks, dialog asks which disks to use.
  --db-disks "<d1> ..."     SSDs for the BlueStore DB, shared by all OSDs
  --wal-disks "<d1> ..."    SSDs for the BlueStore WAL, shared by all OSDs
                            (both are split into one partition per OSD, and
                            wiped; they need --disks)
  --no-osds                 only create the monitor and the manager
  --prepare-args "<args>"   extra arguments for every "ceph-volume zfs
                            prepare", e.g. "--thin --crush-device-class hdd"
  --single-node             settings for a one-node test cluster: pool size 1
                            and failure domain osd
  --fsid <uuid>             cluster id (default: generated)
  --cluster <name>          cluster name (default: ceph)
  -n, --dry-run             show what would be done, change nothing
  -y, --yes                 do not ask for confirmation
  -h, --help                this text

environment:
  CEPH_PREFIX               where Ceph is installed (default: /usr/local)
  CEPH_VOLUME               ceph-volume command (default: $CEPH_PREFIX/sbin/ceph-volume)
EOF
}

die()
{
	echo "ERROR: $*" >&2
	exit 1
}

info()
{
	echo "==> $*"
}

# Print a command, and run it unless this is a dry run.
run()
{
	printf '+'
	printf ' %q' "$@"
	printf '\n'
	if [ "${DRY_RUN}" != "yes" ]; then
		"$@"
	fi
}

while [ $# -gt 0 ]; do
	case "$1" in
	-h|--help)
		usage
		exit 0
		;;
	-n|--dry-run)
		DRY_RUN="yes"
		shift
		;;
	-y|--yes)
		ASSUME_YES="yes"
		shift
		;;
	--single-node)
		SINGLE_NODE="yes"
		shift
		;;
	--no-osds)
		NO_OSDS="yes"
		shift
		;;
	--mon-id|--mon-ip|--public-network|--disks|--db-disks|--wal-disks|--prepare-args|--fsid|--cluster)
		[ $# -ge 2 ] || die "$1 needs an argument"
		case "$1" in
		--mon-id)		MON_ID="$2" ;;
		--mon-ip)		MON_IP="$2" ;;
		--public-network)	PUBLIC_NETWORK="$2" ;;
		--disks)		DISKS="$2" ;;
		--db-disks)		DB_DISKS="$2" ;;
		--wal-disks)		WAL_DISKS="$2" ;;
		--prepare-args)		PREPARE_ARGS="$2" ;;
		--fsid)			FSID="$2" ;;
		--cluster)		CLUSTER="$2" ;;
		esac
		shift 2
		;;
	*)
		usage >&2
		die "unknown option: $1"
		;;
	esac
done

[ -n "${MON_ID}" ] || MON_ID=$(hostname -s)
[ -n "${FSID}" ] || FSID=$(uuidgen)

CONFDIR="${CEPH_PREFIX}/etc/ceph"
CONF="${CONFDIR}/${CLUSTER}.conf"
ADMIN_KEYRING="${CONFDIR}/${CLUSTER}.client.admin.keyring"
BOOTSTRAP_OSD_KEYRING="/var/lib/ceph/bootstrap-osd/${CLUSTER}.keyring"
MON_DIR="/var/lib/ceph/mon/${CLUSTER}-${MON_ID}"
MGR_DIR="/var/lib/ceph/mgr/${CLUSTER}-${MON_ID}"
CEPH_CLI=("${CEPH_PREFIX}/bin/ceph" --cluster "${CLUSTER}" --connect-timeout 10)

# Accept both "da0" and "/dev/da0".
strip_dev()
{
	local -n list="$1"
	local i

	for i in "${!list[@]}"; do
		list[i]="${list[i]#/dev/}"
	done
}

read -r -a DISK_LIST <<< "${DISKS}"
read -r -a DB_DEVS <<< "${DB_DISKS}"
read -r -a WAL_DEVS <<< "${WAL_DISKS}"
strip_dev DISK_LIST
strip_dev DB_DEVS
strip_dev WAL_DEVS

INTERACTIVE="no"
if [ "${#DISK_LIST[@]}" -eq 0 ] && [ "${NO_OSDS}" = "no" ]; then
	INTERACTIVE="yes"
fi
if [ "${#DISK_LIST[@]}" -eq 0 ] && { [ "${#DB_DEVS[@]}" -gt 0 ] || [ "${#WAL_DEVS[@]}" -gt 0 ]; }; then
	die "--db-disks and --wal-disks need --disks"
fi
if [ "${NO_OSDS}" = "yes" ] && [ "${#DISK_LIST[@]}" -gt 0 ]; then
	die "--no-osds and --disks exclude each other"
fi

# ---------------------------------------------------------------- address

# Use dialog(1), or bsddialog(1) from the base system.
find_dialog()
{
	if [ -n "${DIALOG}" ]; then
		return 0
	fi
	if [ -n "$(command -v dialog)" ]; then
		DIALOG="dialog"
	elif [ -n "$(command -v bsddialog)" ]; then
		DIALOG="bsddialog"
	else
		return 1
	fi
}

# usage: dialog_radiolist <title> <text> <result variable> <tag item status>...
dialog_radiolist()
{
	local title="$1" text="$2"
	local -n result="$3"
	local lh rc=0

	shift 3
	lh=$(($# / 3))
	[ "${lh}" -le 12 ] || lh=12
	result=""
	"${DIALOG}" --output-fd 3 --title "${title}" --radiolist "${text}" 18 78 "${lh}" "$@" \
	    3> "${WORK}/dialog.out" || rc=$?
	clear
	[ "${rc}" -eq 0 ] || die "cancelled"
	result=$(cat "${WORK}/dialog.out")
}

# One line per IPv4 address of this host: interface, address, mask. The mask
# is a prefix length, or a netmask when ifconfig printed no prefix length.
local_ipv4()
{
	ifconfig -f inet:cidr -a inet | awk '
	/^[^ \t]/ { iface = $1; sub(/:$/, "", iface) }
	$1 == "inet" {
		addr = $2
		mask = ""
		n = index(addr, "/")
		if (n > 0) {
			mask = substr(addr, n + 1)
			addr = substr(addr, 1, n - 1)
		} else {
			for (i = 3; i < NF; i++)
				if ($i == "netmask")
					mask = $(i + 1)
		}
		print iface, addr, mask
	}'
}

# The interface that carries the default route, if any.
default_interface()
{
	netstat -rn -f inet | awk '$1 == "default" && !seen { print $4; seen = 1 }'
}

# Prefix length of a prefix length, a hex netmask or a dotted netmask.
mask_to_prefix()
{
	local m="$1" n=0 o
	local IFS=.

	case "${m}" in
	[0-9]|[0-9][0-9])
		echo "${m}"
		;;
	0x*|0X*)
		m=$((m))
		while [ "${m}" -gt 0 ]; do
			n=$((n + (m & 1)))
			m=$((m >> 1))
		done
		echo "${n}"
		;;
	*.*.*.*)
		for o in ${m}; do
			while [ "${o}" -gt 0 ]; do
				n=$((n + (o & 1)))
				o=$((o >> 1))
			done
		done
		echo "${n}"
		;;
	*)
		echo ""
		;;
	esac
}

# The network address of <address>/<prefix>.
ip_network()
{
	local a b c d ip mask net

	IFS=. read -r a b c d <<< "$1"
	ip=$(( (a << 24) + (b << 16) + (c << 8) + d ))
	mask=$(( (0xFFFFFFFF << (32 - $2)) & 0xFFFFFFFF ))
	net=$(( ip & mask ))
	echo "$(( (net >> 24) & 255 )).$(( (net >> 16) & 255 )).$(( (net >> 8) & 255 )).$(( net & 255 ))"
}

# Take the monitor address from this server, with its netmask.
choose_mon_ip()
{
	local -a cand=() cand_default=() items=()
	local iface addr mask prefix defif pick="" first c sel status

	while read -r iface addr mask; do
		case "${iface}" in
		lo*)	continue ;;
		esac
		case "${addr}" in
		127.*|169.254.*)	continue ;;
		esac
		prefix=$(mask_to_prefix "${mask}")
		[ -n "${prefix}" ] || continue
		# A /32 has no subnet to derive a public network from.
		[ "${prefix}" -lt 32 ] || continue
		cand+=("${iface} ${addr} ${prefix}")
	done < <(local_ipv4)
	[ "${#cand[@]}" -gt 0 ] || die "no usable IPv4 address found on this host, use --mon-ip"

	if [ "${#cand[@]}" -eq 1 ]; then
		pick="${cand[0]}"
	else
		defif=$(default_interface)
		for c in "${cand[@]}"; do
			if [ -n "${defif}" ] && [ "${c%% *}" = "${defif}" ]; then
				cand_default+=("${c}")
			fi
		done
		if [ "${#cand_default[@]}" -eq 1 ]; then
			pick="${cand_default[0]}"
		else
			if ! { [ -t 0 ] && [ -t 1 ] && find_dialog; }; then
				echo "Several IPv4 addresses found:" >&2
				for c in "${cand[@]}"; do
					echo "  ${c}" >&2
				done
				die "cannot choose between them without a terminal and dialog, use --mon-ip"
			fi
			first="${cand[0]}"
			[ "${#cand_default[@]}" -eq 0 ] || first="${cand_default[0]}"
			for c in "${cand[@]}"; do
				read -r iface addr prefix <<< "${c}"
				status="off"
				[ "${c}" != "${first}" ] || status="on"
				items+=("${addr}/${prefix}" "${iface}" "${status}")
			done
			dialog_radiolist "Monitor address" \
			    "Select the IPv4 address of this server for the first monitor. Its netmask gives the public network." \
			    sel "${items[@]}"
			for c in "${cand[@]}"; do
				read -r iface addr prefix <<< "${c}"
				if [ "${addr}/${prefix}" = "${sel}" ]; then
					pick="${c}"
				fi
			done
			[ -n "${pick}" ] || die "no address selected"
		fi
	fi

	read -r iface addr prefix <<< "${pick}"
	MON_IP="${addr}"
	MON_IFACE="${iface}"
	if [ -z "${PUBLIC_NETWORK}" ]; then
		PUBLIC_NETWORK="$(ip_network "${addr}" "${prefix}")/${prefix}"
	fi
	info "Monitor address ${addr}/${prefix} on ${iface}, public network ${PUBLIC_NETWORK}"
}

# ---------------------------------------------------------------- disks

# One line per disk: name, bytes, human readable size, rotation rate, model.
disk_inventory()
{
	geom disk list | awk '
	function emit() {
		printf "%s\t%s\t%s\t%s\t%s\n", name, bytes, human, rr, descr
	}
	/^Geom name:/ {
		if (name != "") emit()
		name = $3; bytes = 0; human = ""; descr = ""; rr = "unknown"
	}
	/Mediasize:/ { bytes = $2; human = $3; gsub(/[()]/, "", human) }
	/descr:/ { sub(/^[ \t]*descr:[ \t]*/, ""); descr = $0 }
	/rotationrate:/ { rr = $2 }
	END { if (name != "") emit() }'
}

load_inventory()
{
	local name bytes human rr descr type

	while IFS=$'\t' read -r name bytes human rr descr; do
		case "${name}" in
		cd*)	continue ;;
		esac
		# A rotation rate of 0 or 1 means non-rotating.
		case "${rr}" in
		0|1)		type="SSD" ;;
		unknown)	type="?" ;;
		*)		type="HDD" ;;
		esac
		case "${name}" in
		nda*|nvd*|nvme*)	type="SSD" ;;
		esac
		DISK_ORDER+=("${name}")
		DISK_BYTES[${name}]="${bytes}"
		DISK_SIZE[${name}]="${human}"
		DISK_TYPE[${name}]="${type}"
		DISK_DESC[${name}]="${descr}"
	done < <(disk_inventory)
}

# Prints why a disk cannot be used, nothing when it is free.
disk_problem()
{
	local d="$1"

	if [ ! -c "/dev/${d}" ]; then
		echo "is not a device"
	elif gpart status | awk -v d="${d}" '$3 == d { f = 1 } END { exit !f }'; then
		echo "has partitions"
	elif mount -p | awk -v d="/dev/${d}" 'index($1, d) == 1 { f = 1 } END { exit !f }'; then
		echo "is mounted"
	elif zpool status |& awk -v d="${d}" '$1 == d { f = 1 } END { exit !f }'; then
		echo "is part of a zpool"
	fi
}

free_disks()
{
	local d

	for d in "${DISK_ORDER[@]}"; do
		if [ -z "$(disk_problem "${d}")" ]; then
			echo "${d}"
		fi
	done
}

in_list()
{
	local x="$1" y

	shift
	for y in "$@"; do
		if [ "${x}" = "${y}" ]; then
			return 0
		fi
	done
	return 1
}

# usage: dialog_checklist <title> <text> <cancel label> <result array> <tag item status>...
# Returns 1 when the cancel button was used; Esc aborts the script.
dialog_checklist()
{
	local title="$1" text="$2" cancel="$3"
	local -n result="$4"
	local lh rc=0

	shift 4
	lh=$(($# / 3))
	[ "${lh}" -le 12 ] || lh=12
	result=()
	"${DIALOG}" --output-fd 3 --separate-output --title "${title}" \
	    --cancel-label "${cancel}" --checklist "${text}" 22 78 "${lh}" "$@" \
	    3> "${WORK}/dialog.out" || rc=$?
	clear
	[ "${rc}" -ne 255 ] || die "cancelled"
	if [ "${rc}" -eq 0 ]; then
		# result is a nameref to the caller's array.
		# shellcheck disable=SC2034
		mapfile -t result < "${WORK}/dialog.out"
	fi
	return "${rc}"
}

# Ask which disks become OSDs, and which SSDs get the DB and the WAL.
select_disks()
{
	local -a free=() rest=() items=() sel=()
	local d status

	mapfile -t free < <(free_disks)
	[ "${#free[@]}" -gt 0 ] || die "no unused disks found; disks with partitions, mounts or a zpool are not offered"

	for d in "${free[@]}"; do
		status="on"
		[ "${DISK_TYPE[${d}]}" != "SSD" ] || status="off"
		items+=("${d}" "${DISK_SIZE[${d}]} ${DISK_TYPE[${d}]} ${DISK_DESC[${d}]}" "${status}")
	done
	dialog_checklist "OSD disks" \
	    "Select the disks that become OSDs.\n\nALL DATA ON THE SELECTED DISKS IS DESTROYED.\n\nOnly disks without partitions, mounts and zpools are listed. SSDs are not preselected, keep them for the DB and WAL." \
	    "Quit" sel "${items[@]}" || die "cancelled"
	DISK_LIST=("${sel[@]}")
	if [ "${#DISK_LIST[@]}" -eq 0 ]; then
		echo "No OSD disks selected, only the monitor and manager are created."
		return 0
	fi

	for d in "${free[@]}"; do
		in_list "${d}" "${DISK_LIST[@]}" || rest+=("${d}")
	done
	[ "${#rest[@]}" -gt 0 ] || return 0

	# Nothing is preselected: these devices are wiped, so opt in explicitly.
	items=()
	for d in "${rest[@]}"; do
		items+=("${d}" "${DISK_SIZE[${d}]} ${DISK_TYPE[${d}]} ${DISK_DESC[${d}]}" "off")
	done
	dialog_checklist "BlueStore DB devices" \
	    "Optional: select fast devices for the BlueStore DB (RocksDB).\n\nEach selected device is split into equal partitions, one per OSD, and WIPED. Use Space to select.\n\nSkip to keep the DB on the OSD disks." \
	    "Skip" sel "${items[@]}" || true
	DB_DEVS=("${sel[@]}")

	items=()
	for d in "${rest[@]}"; do
		if [ "${#DB_DEVS[@]}" -gt 0 ] && in_list "${d}" "${DB_DEVS[@]}"; then
			continue
		fi
		items+=("${d}" "${DISK_SIZE[${d}]} ${DISK_TYPE[${d}]} ${DISK_DESC[${d}]}" "off")
	done
	[ "${#items[@]}" -gt 0 ] || return 0
	dialog_checklist "BlueStore WAL devices" \
	    "Optional: select fast devices for the BlueStore WAL (write-ahead log).\n\nEach selected device is split into equal partitions, one per OSD, and WIPED. A WAL partition is at most ${WAL_CAP_MB} MiB. Use Space to select.\n\nSkip to keep the WAL with the DB." \
	    "Skip" sel "${items[@]}" || true
	WAL_DEVS=("${sel[@]}")
}

# Check explicitly given or selected disks.
validate_disks()
{
	local d p
	local -a all=("${DISK_LIST[@]}" "${DB_DEVS[@]}" "${WAL_DEVS[@]}")
	local -A seen=()

	for d in "${all[@]}"; do
		if [ -n "${seen[${d}]:-}" ]; then
			die "/dev/${d} is selected more than once"
		fi
		seen[${d}]=1
		[ -n "${DISK_BYTES[${d}]:-}" ] || die "/dev/${d} is not a disk (see: geom disk list)"
		p=$(disk_problem "${d}")
		[ -z "${p}" ] || die "/dev/${d} ${p}, clear it first with: ceph-volume zfs zap /dev/${d} --force"
	done
}

# Split fast devices into one equal partition per OSD.
# usage: plan_fast <type> <cap in MiB, 0 for none> <devices array> <targets array>
plan_fast()
{
	local type="$1" cap="$2"
	local -n devs="$3"
	local -n targets="$4"
	local n="${#DISK_LIST[@]}" nd="${#devs[@]}"
	local i d mb idx label
	local -a used=()
	local -A count=() seen=()

	targets=()
	[ "${nd}" -gt 0 ] || return 0

	for ((i = 0; i < n; i++)); do
		d="${devs[$((i % nd))]}"
		count[${d}]=$((${count[${d}]:-0} + 1))
	done
	# Devices beyond the number of OSDs get no partition; leave them alone.
	for d in "${devs[@]}"; do
		if [ "${count[${d}]:-0}" -eq 0 ]; then
			echo "WARNING: /dev/${d} is not used for the ${type}: there are more ${type} devices than OSDs" >&2
		else
			used+=("${d}")
		fi
	done
	devs=("${used[@]}")
	nd="${#devs[@]}"
	for ((i = 0; i < n; i++)); do
		d="${devs[$((i % nd))]}"
		seen[${d}]=$((${seen[${d}]:-0} + 1))
		idx="${seen[${d}]}"
		# Leave a little room for the partition table and alignment.
		mb=$((${DISK_BYTES[${d}]} / 1048576 - 4))
		mb=$((mb / ${count[${d}]}))
		if [ "${cap}" -gt 0 ] && [ "${mb}" -gt "${cap}" ]; then
			mb="${cap}"
		fi
		[ "${mb}" -ge 1024 ] || die "/dev/${d} is too small to hold the ${type} of ${count[${d}]} OSDs"
		label="ceph-${type}-${d}-${idx}"
		PART_PLAN+=("${d}|${label}|${mb}")
		targets+=("/dev/gpt/${label}")
	done
}

plan_fast_devices()
{
	PART_PLAN=()
	plan_fast db 0 DB_DEVS DB_TARGET
	plan_fast wal "${WAL_CAP_MB}" WAL_DEVS WAL_TARGET
}

# ---------------------------------------------------------------- preflight

# Fail now, before anything is written, when a Ceph binary cannot start
# because a shared library is missing (for instance a different boost).
check_libs()
{
	local tool out missing bad=""

	for tool in "$@"; do
		out=$(ldd "${CEPH_PREFIX}/bin/${tool}") || die "ldd failed on ${CEPH_PREFIX}/bin/${tool}"
		missing=$(awk '/not found/' <<< "${out}")
		if [ -n "${missing}" ]; then
			echo "${CEPH_PREFIX}/bin/${tool} cannot find these shared libraries:" >&2
			echo "${missing}" >&2
			bad="yes"
		fi
	done
	if [ -n "${bad}" ]; then
		echo "Fix the installation first, for example with:" >&2
		echo "  pkg info -d ceph21 | grep boost; pkg info boost-libs; pkg check -d -a" >&2
		die "Ceph binaries are missing shared libraries, nothing was changed"
	fi
}

preflight()
{
	local tool ceph_uid d

	[ "$(uname -s)" = "FreeBSD" ] || die "this script is for FreeBSD"
	[ "$(id -u)" -eq 0 ] || die "run this as root"
	ceph_uid=$(id -u ceph) || die "user ceph does not exist, is the package installed?"
	[ -n "${ceph_uid}" ] || die "could not determine the uid of user ceph"

	for tool in ceph ceph-mon ceph-mgr ceph-osd ceph-authtool monmaptool; do
		[ -x "${CEPH_PREFIX}/bin/${tool}" ] || die "${CEPH_PREFIX}/bin/${tool} not found"
	done
	check_libs ceph-mon ceph-mgr ceph-osd ceph-authtool monmaptool
	if [ "${NO_OSDS}" = "no" ]; then
		[ -x "${CEPH_VOLUME}" ] || die "${CEPH_VOLUME} not found; set CEPH_VOLUME to its path, or use --no-osds"
	fi
	for tool in jq timeout sysrc uuidgen geom gpart; do
		[ -n "$(command -v "${tool}")" ] || die "${tool} not found"
	done
	for tool in ceph_mon ceph_mgr ceph_osd; do
		[ -x "${CEPH_PREFIX}/etc/rc.d/${tool}" ] || die "rc.d script ${CEPH_PREFIX}/etc/rc.d/${tool} not found"
	done
	[ -f "${CEPH_PREFIX}/libexec/ceph/rc.ceph.subr" ] || die "${CEPH_PREFIX}/libexec/ceph/rc.ceph.subr not found, the rc.d scripts need it"
	[ -d "${CONFDIR}" ] || die "${CONFDIR} does not exist"
	for d in mon mgr osd bootstrap-osd; do
		[ -d "/var/lib/ceph/${d}" ] || die "/var/lib/ceph/${d} does not exist"
	done

	[ ! -e "${CONF}" ] || die "${CONF} exists, this node already has a cluster configuration"
	[ ! -e "${MON_DIR}" ] || die "${MON_DIR} exists, this node already has a monitor"
	[ ! -e "${ADMIN_KEYRING}" ] || die "${ADMIN_KEYRING} exists"
	[ ! -e "${BOOTSTRAP_OSD_KEYRING}" ] || die "${BOOTSTRAP_OSD_KEYRING} exists"

	[ "$(sysrc -n zfs_enable)" = "YES" ] || echo "WARNING: zfs_enable is not YES, the OSD pools will not be imported at boot" >&2
	[ "$(sysrc -n ntpd_enable)" = "YES" ] || echo "WARNING: ntpd_enable is not YES, monitors need synchronised clocks" >&2

	if [ "${INTERACTIVE}" = "yes" ]; then
		[ -t 0 ] && [ -t 1 ] || die "no terminal for the disk selection; use --disks or --no-osds"
		find_dialog || die "dialog not found; install devel/dialog, or use --disks or --no-osds"
	fi
}

show_plan()
{
	local i e d label mb
	local -a wipe=()

	cat << EOF

Plan for the first node of cluster "${CLUSTER}":

  fsid            ${FSID}
  monitor/manager ${MON_ID} at ${MON_IP}${MON_IFACE:+ (${MON_IFACE})}
  public network  ${PUBLIC_NETWORK:-(not set)}
  single node     ${SINGLE_NODE}
  config          ${CONF}
  admin keyring   ${ADMIN_KEYRING}
  prepare args    ${PREPARE_ARGS:-(none)}

EOF
	if [ "${#DISK_LIST[@]}" -eq 0 ]; then
		echo "  OSDs            none"
		echo
		return 0
	fi

	echo "OSDs (${#DISK_LIST[@]}):"
	for ((i = 0; i < ${#DISK_LIST[@]}; i++)); do
		d="${DISK_LIST[i]}"
		printf '  /dev/%-8s %-7s %s' "${d}" "${DISK_SIZE[${d}]}" "${DISK_TYPE[${d}]}"
		if [ "${#DB_TARGET[@]}" -gt 0 ]; then
			printf '  db=%s' "${DB_TARGET[i]}"
		fi
		if [ "${#WAL_TARGET[@]}" -gt 0 ]; then
			printf '  wal=%s' "${WAL_TARGET[i]}"
		fi
		printf '\n'
	done
	if [ "${#PART_PLAN[@]}" -gt 0 ]; then
		echo
		echo "Fast devices, one GPT partition per OSD:"
		for e in "${PART_PLAN[@]}"; do
			IFS='|' read -r d label mb <<< "${e}"
			printf '  /dev/%-8s gpt/%-22s %6s MiB\n' "${d}" "${label}" "${mb}"
		done
	fi
	wipe=("${DISK_LIST[@]}" "${DB_DEVS[@]}" "${WAL_DEVS[@]}")
	echo
	echo "ALL DATA ON THESE DEVICES WILL BE DESTROYED: ${wipe[*]}"
	echo
}

confirm()
{
	local answer

	[ "${DRY_RUN}" = "yes" ] && return 0
	[ "${ASSUME_YES}" = "yes" ] && return 0
	[ -t 0 ] || die "stdin is not a terminal, use --yes"
	read -r -p "Type yes to continue: " answer
	[ "${answer}" = "yes" ] || die "aborted"
}

# ------------------------------------------------------------------ helpers

conf_text()
{
	cat << EOF
[global]
fsid = ${FSID}
mon_initial_members = ${MON_ID}
mon_host = ${MON_IP}
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx
osd_objectstore = bluestore
EOF
	if [ -n "${PUBLIC_NETWORK}" ]; then
		echo "public_network = ${PUBLIC_NETWORK}"
	fi
	if [ "${SINGLE_NODE}" = "yes" ]; then
		cat << EOF
mon_allow_pool_size_one = true
osd_pool_default_size = 1
osd_pool_default_min_size = 1
osd_crush_chooseleaf_type = 0
EOF
	fi
}

write_conf()
{
	info "Writing ${CONF}"
	CREATED+=("${CONF}")
	if [ "${DRY_RUN}" = "yes" ]; then
		conf_text | sed 's/^/    /'
	else
		conf_text > "${CONF}"
	fi
}

# Poll until a function succeeds. usage: wait_for <seconds> <what> <function>
wait_for()
{
	local limit="$1" what="$2" fn="$3" waited=0

	[ "${DRY_RUN}" = "yes" ] && { echo "(would wait for ${what})"; return 0; }
	until "${fn}"; do
		if [ "${waited}" -ge "${limit}" ]; then
			echo "WARNING: ${what} not reached after ${limit}s" >&2
			return 1
		fi
		sleep 5
		waited=$((waited + 5))
	done
	echo "${what}: ok"
}

mon_has_quorum()
{
	# A monitor only answers quorum_status while it is in a quorum.
	timeout 30 "${CEPH_CLI[@]}" quorum_status --format json > "${WORK}/quorum.json"
}

mgr_available()
{
	[ "$(timeout 30 "${CEPH_CLI[@]}" mgr stat --format json | jq -r '.available')" = "true" ]
}

osds_up()
{
	local n

	n=$(timeout 30 "${CEPH_CLI[@]}" osd stat --format json | jq -r '.num_up_osds')
	[ "${n}" -ge "${#DISK_LIST[@]}" ]
}

# ------------------------------------------------------------------- steps

create_keyrings_and_monmap()
{
	info "Creating keyrings and the monitor map"
	CREATED+=("${ADMIN_KEYRING}" "${BOOTSTRAP_OSD_KEYRING}")
	run "${CEPH_PREFIX}/bin/ceph-authtool" --create-keyring "${WORK}/mon.keyring" \
	    --gen-key -n mon. --cap mon 'allow *'
	run "${CEPH_PREFIX}/bin/ceph-authtool" --create-keyring "${ADMIN_KEYRING}" \
	    --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' \
	    --cap mds 'allow *' --cap mgr 'allow *'
	run "${CEPH_PREFIX}/bin/ceph-authtool" --create-keyring "${BOOTSTRAP_OSD_KEYRING}" \
	    --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd' \
	    --cap mgr 'allow r'
	run "${CEPH_PREFIX}/bin/ceph-authtool" "${WORK}/mon.keyring" \
	    --import-keyring "${ADMIN_KEYRING}"
	run "${CEPH_PREFIX}/bin/ceph-authtool" "${WORK}/mon.keyring" \
	    --import-keyring "${BOOTSTRAP_OSD_KEYRING}"
	run "${CEPH_PREFIX}/bin/monmaptool" --create --add "${MON_ID}" "${MON_IP}" \
	    --fsid "${FSID}" "${WORK}/monmap"
	run chown ceph:ceph "${WORK}" "${WORK}/mon.keyring" "${WORK}/monmap" \
	    "${BOOTSTRAP_OSD_KEYRING}"
}

create_mon()
{
	info "Creating and starting monitor ${MON_ID}"
	STAGE="started"
	run "${CEPH_PREFIX}/bin/ceph-mon" --cluster "${CLUSTER}" --mkfs -i "${MON_ID}" \
	    --monmap "${WORK}/monmap" --keyring "${WORK}/mon.keyring" \
	    --setuser ceph --setgroup ceph
	[ "${CLUSTER}" = "ceph" ] || run sysrc "ceph_cluster=${CLUSTER}"
	run sysrc ceph_mon_enable=YES "ceph_mon_instances=${MON_ID}"
	run service ceph_mon start
	wait_for "${QUORUM_TIMEOUT}" "monitor quorum" mon_has_quorum \
	    || die "the monitor did not reach quorum, see /var/log/messages and /var/log/ceph"
}

create_mgr()
{
	info "Creating and starting manager ${MON_ID}"
	run mkdir -p "${MGR_DIR}"
	run "${CEPH_CLI[@]}" auth get-or-create "mgr.${MON_ID}" mon 'allow profile mgr' \
	    osd 'allow *' mds 'allow *' -o "${MGR_DIR}/keyring"
	run chown -R ceph:ceph "${MGR_DIR}"
	run sysrc ceph_mgr_enable=YES "ceph_mgr_instances=${MON_ID}"
	run service ceph_mgr start
	wait_for 60 "manager" mgr_available || true
}

create_fast_partitions()
{
	local e d label mb
	local -A created=()

	[ "${#PART_PLAN[@]}" -gt 0 ] || return 0
	info "Partitioning the DB/WAL devices"
	for e in "${PART_PLAN[@]}"; do
		IFS='|' read -r d label mb <<< "${e}"
		if [ -z "${created[${d}]:-}" ]; then
			run gpart create -s gpt "${d}"
			created[${d}]=1
		fi
		run gpart add -t freebsd-zfs -a 1m -l "${label}" -s "${mb}m" "${d}"
	done
}

create_osds()
{
	local i d
	local -a extra

	[ "${#DISK_LIST[@]}" -gt 0 ] || { info "No OSD disks, not creating OSDs"; return 0; }

	create_fast_partitions

	info "Preparing ${#DISK_LIST[@]} OSD(s)"
	for ((i = 0; i < ${#DISK_LIST[@]}; i++)); do
		d="${DISK_LIST[i]}"
		extra=()
		if [ "${#DB_TARGET[@]}" -gt 0 ]; then
			extra+=(--block.db "${DB_TARGET[i]}")
		fi
		if [ "${#WAL_TARGET[@]}" -gt 0 ]; then
			extra+=(--block.wal "${WAL_TARGET[i]}")
		fi
		# PREPARE_ARGS is meant to be split into words.
		# shellcheck disable=SC2086
		run "${CEPH_VOLUME}" --cluster "${CLUSTER}" zfs prepare \
		    --data "/dev/${d}" "${extra[@]}" ${PREPARE_ARGS}
	done

	info "Starting the OSDs"
	# Slow shutdown of many OSDs needs more than the default 90 seconds.
	run sysrc ceph_osd_enable=YES rcshutdown_timeout=300
	run service ceph_osd start
	wait_for "${OSD_TIMEOUT}" "all OSDs up" osds_up || true
}

# ------------------------------------------------------------------ keyrings

# Say exactly which files go to other nodes, and which can go afterwards.
keyring_guide()
{
	cat << EOF

==> Keyrings: what to copy to other nodes, and what to remove later

Copy to EVERY other node (all daemons and clients read it):
  ${CONF}

Copy only to the nodes that need it (root only, mode 0600):
  ${ADMIN_KEYRING}
      client.admin, full control of the cluster. Needed on a node where
      you run "ceph" commands as admin, and once on a node that becomes
      an additional monitor ("ceph auth get mon.", "ceph mon getmap").
      Not needed on OSD-only nodes, and not needed by any daemon.
  ${BOOTSTRAP_OSD_KEYRING}
      client.bootstrap-osd, can only register new OSDs. Needed on each
      node where "ceph-volume zfs prepare" runs. Not needed by OSDs that
      are already running.

Never copy (every daemon has its own key, it stays where it is):
  ${MON_DIR}/keyring
  ${MGR_DIR}/keyring
  /var/lib/ceph/osd/${CLUSTER}-<id>/keyring
  The temporary mon keyring (mon., admin and bootstrap-osd keys) is
  deleted when this script exits. New monitors get the mon. key from the
  cluster with "ceph auth get mon.".

Commands, for a node called <node>:
  scp -p ${CONF} root@<node>:${CONFDIR}/
  scp -p ${ADMIN_KEYRING} root@<node>:${CONFDIR}/
  scp -p ${BOOTSTRAP_OSD_KEYRING} root@<node>:/var/lib/ceph/bootstrap-osd/
  and on <node>: chown ceph:ceph ${BOOTSTRAP_OSD_KEYRING}

After you have added more monitors:
  - add every monitor to mon_host in ${CONF} and copy that file to
    all nodes again
  - on each new monitor node remove /tmp/ceph.mon.keyring and /tmp/monmap
    as soon as "ceph-mon --mkfs" has run; they hold the mon. key
  - remove ${ADMIN_KEYRING}
    from every node you do not administer the cluster from, this
    node included if you administer from another one
  - remove ${BOOTSTRAP_OSD_KEYRING}
    from nodes that are finished creating OSDs
  - keep ${CONF} on every node
EOF
}

# --------------------------------------------------------------------- main

# Runs on every exit. After a failure before the monitor was created, remove
# the files this run wrote (preflight guarantees they did not exist before),
# so the script can simply be run again.
on_exit()
{
	local rc=$?

	rm -rf "${WORK}"
	[ "${rc}" -ne 0 ] || return 0
	[ "${DRY_RUN}" != "yes" ] || return 0

	if [ "${STAGE}" = "prep" ]; then
		if [ "${#CREATED[@]}" -gt 0 ]; then
			echo "Stopped before anything was started. Removing the files this run created:" >&2
			rm -fv "${CREATED[@]}" >&2
			echo "Fix the problem and run the script again." >&2
		fi
		return 0
	fi

	cat >&2 << EOF

The script stopped while creating the monitor or later, so this node is partly
set up. Nothing is removed automatically. To start over:
  service ceph_osd stop; service ceph_mgr stop; service ceph_mon stop
  rm -rf ${MON_DIR} ${MGR_DIR}
  rm -f ${CONF} ${ADMIN_KEYRING} ${BOOTSTRAP_OSD_KEYRING}
  sysrc -x ceph_mon_enable ceph_mon_instances ceph_mgr_enable ceph_mgr_instances ceph_osd_enable
Pools and zvols of OSDs that were already prepared: ceph-volume zfs list, and
ceph-volume zfs zap /dev/<disk> --destroy --force
EOF
}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/ceph-deploy.XXXXXXXX")
trap on_exit EXIT

preflight
if [ -z "${MON_IP}" ]; then
	choose_mon_ip
fi
load_inventory
if [ "${INTERACTIVE}" = "yes" ]; then
	select_disks
fi
validate_disks
plan_fast_devices
show_plan
confirm

write_conf
create_keyrings_and_monmap
create_mon
create_mgr
create_osds

info "Cluster status"
if [ "${DRY_RUN}" = "yes" ]; then
	echo "(dry run, nothing was changed)"
	keyring_guide
else
	"${CEPH_CLI[@]}" -s
	cat << EOF

The first node is up. A new cluster reports HEALTH_WARN until there are
enough OSDs for the pool size. Next steps:

  - copy the configuration and keyrings to the other nodes, see below
  - add more monitors, see the manual deployment documentation
  - create pools, for example: ceph osd pool create rbd
EOF
	keyring_guide
fi
