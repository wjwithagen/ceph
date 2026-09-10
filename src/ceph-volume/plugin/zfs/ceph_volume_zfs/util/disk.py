import re

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info
from ceph_volume import conf

report_template = """
/dev/{geomname:<16} {mediasize:<16} {rotational!s:<7} {available:<11} {descr}"""


def camcontrol_devlist_parser():
    """
    Parses `camcontrol devlist` output to enumerate all CAM-visible
    devices, including their pass-through node. This is the primary
    disk enumeration source: it catches devices the kernel sees on the
    CAM bus even if geom hasn't (yet) attached a provider for them.

    Example line:
    <ATA CORSAIR CSSD-F40GB2 M028>    at scbus2 target 0 lun 0 (ada0,pass0)

    Returns a dict keyed by disk device name (e.g. 'ada0'), each value
    a dict with 'descr', 'scbus', 'target', 'lun', 'pass'.
    """
    command = ['/sbin/camcontrol', 'devlist']
    out, err, rc = process.call(command)
    devices = {}
    line_re = re.compile(
        r'^<(?P<descr>.*?)>\s+at\s+scbus(?P<scbus>\d+)\s+'
        r'target\s+(?P<target>\d+)\s+lun\s+(?P<lun>\d+)\s+'
        r'\((?P<nodes>[^)]+)\)'
    )
    for line in out:
        m = line_re.match(line.strip())
        if not m:
            continue
        nodes = [n.strip() for n in m.group('nodes').split(',')]
        disk_node = next((n for n in nodes if not n.startswith('pass')), None)
        pass_node = next((n for n in nodes if n.startswith('pass')), None)
        if disk_node is None:
            continue
        devices[disk_node] = {
            'descr': m.group('descr'),
            'scbus': m.group('scbus'),
            'target': m.group('target'),
            'lun': m.group('lun'),
            'pass': pass_node,
        }
    return devices


def geom_disk_parser(block):
    """
    Parses lines in 'geom disk list` output.

    Geom name: ada3
    Providers:
    1. Name: ada3
       Mediasize: 40018599936 (37G)
       Sectorsize: 512
       Stripesize: 4096
       Stripeoffset: 0
       Mode: r2w2e4
       descr: Corsair CSSD-F40GB2
       lunid: 5000000000000236
       ident: 111465010000101800EC
       rotationrate: 0
       fwsectors: 63
       fwheads: 16

    :param line: A string, with the full block for `geom disk list`
    """
    pairs = block.split(';')
    parsed = {}
    for pair in pairs:
        if 'Providers' in pair:
            continue
        try:
            column, value = pair.split(':')
        except ValueError:
            continue
        # fixup
        column = re.sub(r"\s+", "", column)
        column = re.sub(r"^[0-9]+\.", "", column)
        value = value.strip()
        value = re.sub(r"\([0-9A-Z]+\)", '', value)
        parsed[column.lower()] = value
    return parsed


def get_geom_disk(diskname):
    """
    Captures all available info from geom for a single disk name
    (e.g. 'ada0', no /dev/ prefix), along with interesting metadata
    like sectors, size, vendor, solid/rotational, etc.

    Returns a dictionary, with all the geom fields as keys. Returns an
    empty dict if geom has no provider for this disk (e.g. it hasn't
    attached yet) rather than raising, since camcontrol is now the
    source of truth for whether the disk exists at all.
    """
    command = ['/sbin/geom', 'disk', 'list', diskname]
    out, err, rc = process.call(command)
    if rc != 0:
        return {}
    geom_block = ""
    for line in out:
        line.strip()
        geom_block += ";" + line
    return geom_disk_parser(geom_block)


def get_partitions(diskname):
    """
    Runs `gpart show -p <diskname>` and returns a list of partition
    dicts: [{'name': 'ada0p1', 'type': 'freebsd-boot',
    'size_human': '512K'}, ...].

    The -p flag makes gpart print full partition device names
    (ada0p1) instead of bare index numbers, which is what we need to
    cross-reference against `mount -p` output. Free-space gaps
    (unallocated regions between/after partitions) are skipped -- gpart
    reports these with no partition name, they're not real partitions.

    Returns an empty list if the disk has no partition table at all.
    """
    command = ['/sbin/gpart', 'show', '-p', diskname]
    # verbose_on_failure=False: a non-zero rc here means "no partition
    # table", which is an expected, meaningful result for us -- not a
    # failure worth printing "gpart: No such geom: adaN." to stderr.
    out, err, rc = process.call(command, verbose_on_failure=False)
    partitions = []
    if rc != 0 or not out:
        return partitions
    line_re = re.compile(r'^\s*\d+\s+\d+\s+(\S+)\s+(\S+)\s+\(([^)]+)\)\s*$')
    for line in out:
        if line.strip().startswith('=>'):
            continue
        m = line_re.match(line)
        if not m:
            continue
        name, ptype, human = m.groups()
        if name == '-' or not name.startswith(diskname):
            # unallocated / free space region, not a real partition
            continue
        partitions.append({'name': name, 'type': ptype, 'size_human': human})
    return partitions


def get_gpart_info(diskname):
    """
    Runs `gpart show <diskname>` to detect an existing partition
    scheme, and augments with the actual partition list from
    get_partitions(). Returns a dict with:
      - 'has_partitions': bool
      - 'scheme': 'GPT'/'MBR'/None
      - 'empty': bool -- True if there's no partition table AND no
        partitions at all (the disk is genuinely blank)
      - 'partitions': list of partition dicts (see get_partitions)
      - 'raw': raw `gpart show` output lines, for reference/logging

    A disk with an existing partition table is not safe to blindly
    hand to `prepare` without an explicit zap first.
    """
    command = ['/sbin/gpart', 'show', diskname]
    # verbose_on_failure=False: see note in get_partitions() -- a
    # non-zero rc is the expected signal for an unpartitioned disk.
    out, err, rc = process.call(command, verbose_on_failure=False)
    info = {
        'has_partitions': False,
        'scheme': None,
        'empty': True,
        'partitions': [],
        'raw': out,
    }
    if rc != 0 or not out:
        # rc != 0 typically means "no such geom" -- i.e. no partition
        # table at all, which gpart reports as an error rather than
        # empty output. Disk is genuinely empty.
        return info
    header = out[0]
    # Header looks like: "=>       40  976773127  ada0  GPT  (466G)"
    # -- scheme is the bare word before the parenthesized human-size
    # field, not inside the parens (that's the size, e.g. "(466G)").
    m = re.search(r'\s(\S+)\s+\([^)]+\)\s*$', header)
    if m:
        info['scheme'] = m.group(1)
        info['has_partitions'] = True
        info['empty'] = False
    partitions = get_partitions(diskname)
    info['partitions'] = partitions
    if partitions:
        info['empty'] = False
        info['has_partitions'] = True
    return info


def _walk_vdevs(vdevs, diskname, pool_name, found):
    """
    Recursively walks the 'vdevs' dict from `zpool status -j` output
    (which nests: root -> mirror/raidz/normal -> leaf disks) looking
    for a leaf vdev whose 'path' matches this disk, bare or
    partitioned (e.g. /dev/ada0 or /dev/ada0p3).
    """
    if found['in_pool']:
        return
    for name, vdev in vdevs.items():
        path = vdev.get('path', '')
        if re.match(r"^/dev/" + re.escape(diskname) + r"(p\d+)?$", path):
            found['in_pool'] = True
            found['pool_name'] = pool_name
            return
        children = vdev.get('vdevs')
        if children:
            _walk_vdevs(children, diskname, pool_name, found)
            if found['in_pool']:
                return


def get_zpool_membership(diskname):
    """
    Checks whether this disk (or any gpart partition on it) is
    already a member of a zpool, using `zpool status -j` for
    structured, reliable parsing (rather than screen-scraping the
    plain-text vdev tree).

    Returns a dict: {'in_pool': bool, 'pool_name': str or None}.

    Note: this only sees *imported* pools. A pool that exists on disk
    but is currently exported won't show up here -- callers that need
    that level of safety should also check `zpool import` (with no
    args, lists importable-but-not-imported pools) before treating a
    disk as free.
    """
    import json
    command = ['/sbin/zpool', 'status', '-j']
    out, err, rc = process.call(command)
    result = {'in_pool': False, 'pool_name': None}
    if rc != 0 or not out:
        return result
    try:
        data = json.loads(''.join(out))
    except (ValueError, TypeError):
        return result
    for pool_name, pool in data.get('pools', {}).items():
        top_vdevs = pool.get('vdevs', {})
        _walk_vdevs(top_vdevs, diskname, pool_name, result)
        if result['in_pool']:
            break
    return result


def get_mount_info(diskname, partitions=None):
    """
    Cross-references `mount -p` (parseable output) against this disk's
    partitions to catch cases where something on the disk is actively
    mounted, even outside ZFS/Ceph's awareness (e.g. a stray UFS
    partition from a previous life of this drive).

    Reports per-partition, not just a disk-level yes/no -- pass the
    partition list from get_partitions() so every partition gets an
    entry (mounted or not), not just the ones that happen to be
    mounted.

    Returns a dict:
      {
        'mounted': bool,               # true if ANY partition (or the
                                        # bare disk device) is mounted
        'mountpoints': [str, ...],     # flat list, for quick display
        'by_partition': {              # per-partition detail
            'ada0p1': {'mounted': True, 'mountpoint': '/'},
            'ada0p2': {'mounted': False, 'mountpoint': None},
        },
      }

    Known gap: gpt-label/gptid mounts (/dev/gpt/somelabel,
    /dev/gptid/...) won't match here, only plain /dev/adaN /
    /dev/adaNpM device paths. Worth extending if this box mounts by
    label rather than raw device path.
    """
    command = ['/sbin/mount', '-p']
    out, err, rc = process.call(command)
    result = {'mounted': False, 'mountpoints': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'mounted': False, 'mountpoint': None}
    if rc != 0:
        return result
    for line in out:
        fields = line.split()
        if not fields:
            continue
        device = fields[0]
        mountpoint = fields[1] if len(fields) > 1 else None
        m = re.match(r"^/dev/(" + re.escape(diskname) + r"(p\d+)?)$", device)
        if not m:
            continue
        matched_name = m.group(1)
        result['mounted'] = True
        if mountpoint:
            result['mountpoints'].append(mountpoint)
        if matched_name in result['by_partition']:
            result['by_partition'][matched_name] = {
                'mounted': True,
                'mountpoint': mountpoint,
            }
        else:
            # mounted device that wasn't in the partitions list -- e.g.
            # the bare disk device itself mounted directly, no
            # partition table involved
            result['by_partition'][matched_name] = {
                'mounted': True,
                'mountpoint': mountpoint,
            }
    return result


def get_swap_info(diskname, partitions=None):
    """
    Cross-references `swapctl -l` against this disk's partitions to
    catch active swap devices. Swap is NOT visible via `mount -p` --
    it's activated with swapon/swapctl, not mounted as a filesystem --
    so this is a separate check from get_mount_info(), needed for the
    same reason: destroying the partition table under an active swap
    device is exactly the kind of thing zap must refuse before ever
    reaching gpart destroy (which may itself refuse with a generic
    "Device busy", but that's the OS catching it late, after we've
    already told the user what we're about to destroy -- this check
    catches it upfront with a clear reason).

    Returns a dict:
      {
        'active': bool,               # true if ANY partition (or the
                                       # bare disk) is active swap
        'devices': [str, ...],        # flat list of active swap
                                       # device paths on this disk
        'by_partition': {
            'ada0p1': {'active': True},
            'ada0p2': {'active': False},
        },
      }
    """
    command = ['/sbin/swapctl', '-l']
    out, err, rc = process.call(command)
    result = {'active': False, 'devices': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'active': False}
    if rc != 0 or not out:
        return result
    for line in out[1:]:  # first line is the header: "Device  1K-blocks  Used"
        fields = line.split()
        if not fields:
            continue
        device = fields[0]
        m = re.match(r"^/dev/(" + re.escape(diskname) + r"(p\d+)?)$", device)
        if not m:
            continue
        matched_name = m.group(1)
        result['active'] = True
        result['devices'].append(device)
        result['by_partition'][matched_name] = {'active': True}
    return result


def get_zpool_ceph_properties(pool_name):
    """
    Reads back the `ceph:*` user properties that
    ceph_volume_zfs.objectstore.Zfs._tag_zpool() sets on pools it
    creates. Used to positively identify a pool as one this plugin
    made before allowing anything destructive to touch it.

    Returns a dict of the ceph:* properties found (without the
    'ceph:' prefix), empty if none / pool doesn't exist.

    A pool is considered managed by this plugin only if it carries
    'managed_by' == 'ceph-volume-zfs'. Anything else -- an unrelated
    user pool, zroot -- has no such property and must never be
    treated as ours.
    """
    command = ['/sbin/zpool', 'get', '-H', '-o', 'property,value', 'all', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    properties = {}
    if rc != 0 or not out:
        return properties
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        key, value = fields[0], fields[1]
        if key.startswith('ceph:'):
            properties[key[len('ceph:'):]] = value
    return properties


def zpool_is_ceph_managed(pool_name):
    """
    True only if the pool carries ceph:managed_by=ceph-volume-zfs,
    i.e. this plugin created it. Everything else is somebody else's
    pool.
    """
    props = get_zpool_ceph_properties(pool_name)
    return props.get('managed_by') == 'ceph-volume-zfs'


def list_zpools():
    """
    Returns a list of all zpool names on the system (imported pools
    only -- exported pools aren't visible to `zpool list`).
    """
    command = ['/sbin/zpool', 'list', '-H', '-o', 'name']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return []
    return [line.strip() for line in out if line.strip()]


def get_zvols(pool_name):
    """
    Returns the zvols (ZFS volumes) inside a pool, as a list of
    dicts: [{'name': 'osd-block-<fsid>', 'dataset':
    '<pool>/osd-block-<fsid>', 'size': '<bytes>'}, ...].
    """
    command = ['/sbin/zfs', 'list', '-H', '-p', '-t', 'volume',
               '-o', 'name,volsize', '-r', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    zvols = []
    if rc != 0 or not out:
        return zvols
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        dataset, volsize = fields[0], fields[1]
        zvols.append({
            'dataset': dataset,
            'name': dataset.split('/', 1)[-1],
            'size': volsize,
        })
    return zvols


def get_zvol_ceph_properties(dataset):
    """
    Reads back the ceph:* user properties set on a zvol by
    ceph_volume_zfs.objectstore.Zfs._tag_zvol(). Returns a dict
    keyed without the 'ceph:' prefix.
    """
    command = ['/sbin/zfs', 'get', '-H', '-o', 'property,value', 'all', dataset]
    out, err, rc = process.call(command, verbose_on_failure=False)
    properties = {}
    if rc != 0 or not out:
        return properties
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        key, value = fields[0], fields[1]
        if key.startswith('ceph:'):
            properties[key[len('ceph:'):]] = value
    return properties


def _collect_vdev_paths(vdevs, paths):
    """
    Recursively collects leaf vdev 'path' values from the nested
    'vdevs' structure of `zpool status -j`.
    """
    for name, vdev in vdevs.items():
        path = vdev.get('path')
        children = vdev.get('vdevs')
        if children:
            _collect_vdev_paths(children, paths)
        elif path:
            paths.append({
                'path': path,
                'state': vdev.get('state', ''),
                'vdev_type': vdev.get('vdev_type', ''),
            })


def get_zpool_vdevs(pool_name):
    """
    Returns the physical devices backing a pool, as a list of dicts:
    [{'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'}].

    Reads `zpool status -j` and walks the same nested vdev tree that
    get_zpool_membership() searches, but collecting every leaf rather
    than matching one disk.
    """
    import json
    command = ['/sbin/zpool', 'status', '-j', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths = []
    if rc != 0 or not out:
        return paths
    try:
        data = json.loads(''.join(out))
    except (ValueError, TypeError):
        return paths
    pool = data.get('pools', {}).get(pool_name, {})
    _collect_vdev_paths(pool.get('vdevs', {}), paths)
    return paths


def list_ceph_zpools():
    """
    Discovery entry point for `ceph-volume zfs list` (and, later,
    activate): enumerates every imported zpool and returns only those
    this plugin created, identified by
    ceph:managed_by=ceph-volume-zfs.

    Returns a list of dicts, one per Ceph-managed pool:
      {
        'pool': 'ceph-osd-5',
        'properties': {...},        # pool-level ceph:* properties
        'vdevs': [                  # physical devices backing the pool
            {'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'},
        ],
        'zvols': [                  # each zvol with its own ceph:* props
            {'name': ..., 'dataset': ..., 'size': ...,
             'device': '/dev/zvol/<dataset>', 'properties': {...}},
        ],
      }
    """
    results = []
    for pool in list_zpools():
        pool_props = get_zpool_ceph_properties(pool)
        if pool_props.get('managed_by') != 'ceph-volume-zfs':
            continue
        zvols = []
        for zvol in get_zvols(pool):
            zvol['device'] = '/dev/zvol/{}'.format(zvol['dataset'])
            zvol['properties'] = get_zvol_ceph_properties(zvol['dataset'])
            zvols.append(zvol)
        results.append({
            'pool': pool,
            'properties': pool_props,
            'vdevs': get_zpool_vdevs(pool),
            'zvols': zvols,
        })
    return results


def get_disks():
    """
    Primary enumeration entry point. Walks camcontrol devlist as the
    source of truth for "what disks exist", then augments each with
    geom, gpart, zpool-membership, and mount info.

    Optical drives (cd0, cd1, ...) are skipped entirely -- they're
    never valid OSD candidates, and running gpart/zpool checks
    against them is pointless noise (gpart errors on a geom that was
    never created for a driveless optical device).
    """
    cam_devices = camcontrol_devlist_parser()
    disks = {}
    for dsk, cam_info in cam_devices.items():
        if re.match(r'^cd\d+$', dsk):
            continue
        disk = get_geom_disk(dsk)
        disk['cam'] = cam_info
        disk['gpart'] = get_gpart_info(dsk)
        disk['zpool'] = get_zpool_membership(dsk)
        disk['mount'] = get_mount_info(dsk, partitions=disk['gpart'].get('partitions'))
        disk['swap'] = get_swap_info(dsk, partitions=disk['gpart'].get('partitions'))
        disks['/dev/' + dsk] = disk
    return disks


class Disks(object):

    def __init__(self, path=None):
        if not sys_info.devices:
            sys_info.devices = get_disks()
        self.disks = {}
        for k in sys_info.devices:
            if path != None:
                if path in k:
                    self.disks[k] = Disk(k)
            else:
                self.disks[k] = Disk(k)

    def available_candidates(self):
        """
        Returns only the disks that passed all safety checks
        (no existing partitions, not zpool members, not mounted,
        usable media size) -- i.e. what's actually safe to hand to
        `prepare`. This is the filtered view callers should use
        instead of iterating self.disks directly, which includes
        rejected/unsafe devices too.
        """
        return {k: v for k, v in self.disks.items() if v.available}

    def verbose_report(self):
        """
        Human-readable, per-disk report: status, size, model,
        partition layout (or "no partitions"), and per-partition
        mount status. This is the -v style detail view -- use this
        instead of printing raw sys_api dicts.
        """
        sections = [d.describe() for d in
                    (self.disks[k] for k in sorted(self.disks))]
        return '\n\n'.join(sections) + '\n'

    def pretty_report(self, all=True):
        output = [
            report_template.format(
                geomname='Device Path',
                mediasize='Size',
                rotational='rotates',
                available='available',
                descr='Model name',
            )]
        for disk in sorted(self.disks):
            output.append(self.disks[disk].report())
        return ''.join(output)

    def json_report(self):
        output = []
        for disk in sorted(self.disks):
            output.append(self.disks[disk].json_report())
        return output


class Disk(object):

    report_fields = [
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
    ]
    pretty_report_sys_fields = [
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    def __init__(self, path):
        self.abspath = path
        self.path = path
        self.reject_reasons = []
        self.available = True
        self.sys_api = sys_info.devices.get(path, {})
        self._evaluate_availability()

    def _evaluate_availability(self):
        """
        Populates reject_reasons / available based on the augmented
        info gathered in get_disks(): existing partitions, zpool
        membership, and active mounts. A disk failing any of these
        checks is not safe for prepare/zap to touch without an
        explicit override.
        """
        mediasize = self.sys_api.get('mediasize')
        try:
            has_usable_size = int(mediasize) > 0
        except (TypeError, ValueError):
            has_usable_size = False
        if not has_usable_size:
            self.reject_reasons.append(
                'No usable media size reported (e.g. empty optical drive, or device not yet attached)'
            )
            self.available = False

        gpart = self.sys_api.get('gpart', {})
        if gpart.get('has_partitions'):
            self.reject_reasons.append(
                'Has an existing {} partition table'.format(gpart.get('scheme'))
            )
            self.available = False

        zpool = self.sys_api.get('zpool', {})
        if zpool.get('in_pool'):
            self.reject_reasons.append(
                'Already a member of zpool "{}"'.format(zpool.get('pool_name'))
            )
            self.available = False

        mount = self.sys_api.get('mount', {})
        if mount.get('mounted'):
            self.reject_reasons.append(
                'Has mounted filesystem(s) at {}'.format(', '.join(mount.get('mountpoints', [])))
            )
            self.available = False

        swap = self.sys_api.get('swap', {})
        if swap.get('active'):
            self.reject_reasons.append(
                'Has active swap on {}'.format(', '.join(swap.get('devices', [])))
            )
            self.available = False

    @staticmethod
    def _safe_int(value, default=0):
        """
        geom fields are sometimes non-numeric strings, e.g.
        'Mediasize: Unknown' on an optical drive with no media
        inserted. Fall back to `default` instead of raising.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def report(self):
        if self.available:
            available_str = 'True'
        else:
            # keep it short for the fixed-width column; full reasons
            # are in describe()/reject_reasons for the verbose view
            available_str = 'False (' + self.reject_reasons[0] + ')' if self.reject_reasons else 'False'
        return report_template.format(
            geomname=self.sys_api.get('geomname', self.path),
            mediasize=human_readable_size(self._safe_int(self.sys_api.get('mediasize'))),
            rotational=self._safe_int(self.sys_api.get('rotationrate')) != 0,
            available=available_str,
            descr=self.sys_api.get('descr')
        )

    def describe(self):
        """
        Human-readable, multi-line description of this disk: status,
        size, model, partition layout, and per-partition mount state.
        Used by Disks.verbose_report() for the -v style detail view.
        """
        size = human_readable_size(self._safe_int(self.sys_api.get('mediasize')))
        model = self.sys_api.get('descr', 'unknown model')
        status = 'AVAILABLE' if self.available else 'NOT AVAILABLE'

        lines = ['{path}  ({model}, {size})'.format(
            path=self.path, model=model, size=size)]
        lines.append('  status: {}'.format(status))
        if self.reject_reasons:
            for reason in self.reject_reasons:
                lines.append('    - {}'.format(reason))

        gpart = self.sys_api.get('gpart', {})
        partitions = gpart.get('partitions', [])
        if not partitions:
            lines.append('  partitions: none (disk is empty)')
        else:
            lines.append('  partitions:')
            mount = self.sys_api.get('mount', {})
            swap = self.sys_api.get('swap', {})
            mount_by_partition = mount.get('by_partition', {})
            swap_by_partition = swap.get('by_partition', {})
            for part in partitions:
                info = mount_by_partition.get(part['name'], {})
                if info.get('mounted'):
                    mount_desc = 'mounted at {}'.format(info.get('mountpoint'))
                elif swap_by_partition.get(part['name'], {}).get('active'):
                    mount_desc = 'active swap'
                else:
                    mount_desc = 'not mounted'
                lines.append('    {name}  {type}  {size}  ({mount})'.format(
                    name=part['name'],
                    type=part['type'],
                    size=part['size_human'],
                    mount=mount_desc,
                ))
        return '\n'.join(lines)

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items()}
        if not getattr(conf, 'debug', False):
            # 'raw' (literal `gpart show` output lines) is debug-only
            # noise in normal reports -- strip it out of the copy we
            # hand back, without touching the live sys_api dict that
            # get_gpart_info()/_evaluate_availability() rely on.
            sys_api = dict(output.get('sys_api', {}))
            gpart = sys_api.get('gpart')
            if isinstance(gpart, dict) and 'raw' in gpart:
                gpart = dict(gpart)
                gpart.pop('raw', None)
                sys_api['gpart'] = gpart
            output['sys_api'] = sys_api
        return output

