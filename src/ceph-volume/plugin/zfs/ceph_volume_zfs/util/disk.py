import re

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info

report_template = """
{geomname:<21} {mediasize:<16} {rotational!s:<7} {available:<6} {descr}  {reason}"""


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
        value = re.sub(r"\([0-9A-Z.]+\)", '', value).strip()
        parsed[column.lower()] = value
    return parsed


def get_camcontrol_identify(diskname):
    """
    Runs `camcontrol identify <diskname>` to get disk geometry for
    SAS/SCSI (da*) devices where `geom disk list` can't report the
    mediasize via ATA commands.

    Returns a dict with 'mediasize' (bytes as int) and
    'sectorsize' (logical sector size), or empty dict on failure.
    """
    command = ['/sbin/camcontrol', 'identify', diskname]
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return {}
    result = {}
    sector_size = 512  # default
    lba48 = None
    lba = None
    for line in out:
        stripped = line.strip()
        # "sector size    logical 512, physical 4096, offset 0"
        m = re.match(r'sector size\s+logical\s+(\d+)', stripped)
        if m:
            sector_size = int(m.group(1))
        # "LBA48 supported    5860533168 sectors"
        m = re.match(r'LBA48 supported\s+(\d+)\s+sectors', stripped)
        if m:
            lba48 = int(m.group(1))
        # "LBA supported      268435455 sectors"
        m = re.match(r'LBA supported\s+(\d+)\s+sectors', stripped)
        if m:
            lba = int(m.group(1))
    sectors = lba48 or lba
    if sectors:
        result['mediasize'] = str(sectors * sector_size)
        result['sectorsize'] = str(sector_size)
    return result


def get_geom_disk(diskname):
    """
    Captures all available info from geom
    along with interesting metadata like sectors, size, vendor,
    solid/rotational, etc...

    Returns a dictionary, with all the geom fields as keys.
    """

    command = ['/sbin/geom', 'disk', 'list', re.sub('/dev/', '', diskname)]
    out, err, rc = process.call(command)
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
    # Sort by partition index number so p4 always appears before p5
    # regardless of how gpart orders them in its output (which is by
    # start sector, not by partition number).
    def _part_index(p):
        m = re.search(r'p(\d+)$', p['name'])
        return int(m.group(1)) if m else 0
    partitions.sort(key=_part_index)
    # `gpart show` does not print the label, the raw uuid or whether the
    # partition is open; `gpart list` does. Add them when available.
    details = get_partition_details(diskname)
    for part in partitions:
        part.update(details.get(part['name'], {}))
    return partitions


def get_partition_details(diskname):
    """
    Runs `gpart list <diskname>` and returns, per partition, what
    `gpart show` does not print: the GPT label, the raw uuid (what a
    gptid/<uuid> device name is made of) and the GEOM open mode:

      {'da4p3': {'label': 'zfs0', 'rawuuid': 'fa257269-...', 'mode': 'r1w1e2'}}

    The mode is how GEOM says that something has the partition open, for
    instance an imported zpool. r0w0e0 is idle; anything else is in use.
    Returns an empty dict when there is no partition table.
    """
    command = ['/sbin/gpart', 'list', diskname]
    out, err, rc = process.call(command, verbose_on_failure=False)
    details = {}
    if rc != 0 or not out:
        return details
    in_providers = False
    current = None
    for line in out:
        text = line.strip()
        if text == 'Providers:':
            in_providers = True
            current = None
            continue
        if text == 'Consumers:':
            in_providers = False
            current = None
            continue
        if not in_providers:
            continue
        m = re.match(r'^\d+\.\s+Name:\s+(\S+)$', text)
        if m:
            current = details.setdefault(m.group(1), {})
            continue
        if current is None:
            continue
        m = re.match(r'^(label|rawuuid|Mode):\s+(\S+)$', text)
        if m:
            key, value = m.groups()
            if key == 'Mode':
                current['mode'] = value
            elif value != '(null)':
                current[key] = value
    return details


def get_label_aliases(partitions=None):
    """
    Maps GEOM label names (gpt/zfs0, gptid/fa257269-..., diskid/DISK-...)
    to the device they stand for ('da4p3'). zpool, mount and swap often
    use these names, so comparing only with /dev/daNpM misses them.

    Built from `glabel status`, plus the label and raw uuid of the given
    partitions for the cases glabel does not list.
    """
    aliases = {}
    out, err, rc = process.call(['/sbin/glabel', 'status'], verbose_on_failure=False)
    if rc == 0:
        for line in out:
            fields = line.split()
            if len(fields) >= 3 and fields[0] != 'Name':
                aliases[fields[0]] = fields[2]
    for part in partitions or []:
        if part.get('label'):
            aliases.setdefault('gpt/' + part['label'], part['name'])
        if part.get('rawuuid'):
            aliases.setdefault('gptid/' + part['rawuuid'], part['name'])
    return aliases


def resolve_device(path, aliases):
    """'/dev/gptid/fa257269-...' -> 'da4p3'; plain names are returned as they are."""
    name = re.sub(r'^/dev/', '', path)
    return aliases.get(name, name)


def _device_on_disk(name, diskname):
    """'da4p3' -> 'da4p3' when it is the disk or one of its partitions, else None."""
    m = re.match(r'^(' + re.escape(diskname) + r'(p\d+)?)$', name)
    return m.group(1) if m else None


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


def _vdev_type(name):
    """'raidz2-0' -> 'raidz2', 'mirror-1' -> 'mirror', anything else as it is."""
    if 'raidz' in name:
        return name.split('-')[0]
    if 'mirror' in name:
        return 'mirror'
    return name


def _collect_leaf_vdevs(vdevs, pool_name, parent_name, leaves):
    """
    Walks the nested 'vdevs' of `zpool status -j` (root -> mirror/raidz ->
    leaf disks) and collects every leaf, with the group it sits in.
    """
    for name, vdev in vdevs.items():
        children = vdev.get('vdevs')
        if children:
            _collect_leaf_vdevs(children, pool_name, name, leaves)
        elif vdev.get('path'):
            grouped = parent_name and parent_name != pool_name
            leaves.append({
                'pool_name': pool_name,
                'vdev_name': parent_name if grouped else None,
                'vdev_type': _vdev_type(parent_name) if grouped else 'stripe',
                'path': vdev['path'],
            })


def _zpool_leaf_vdevs_plaintext():
    """
    Fallback for `zpool status -j` (OpenZFS < 2.3, e.g. ZFS 2.2.x on
    FreeBSD 14): reads `zpool status -P`, where a leaf vdev is a line that
    starts with a full device path. Indentation tells a leaf of a group
    (mirror-0, raidz2-0, logs, cache, spares) from a direct child of the
    pool.
    """
    command = ['/sbin/zpool', 'status', '-P']
    out, err, rc = process.call(command, verbose_on_failure=False)
    leaves = []
    if rc != 0 or not out:
        return leaves
    pool = None
    group = None
    group_indent = -1
    for line in out:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        m = re.match(r'^pool:\s+(\S+)', stripped)
        if m:
            pool = m.group(1)
            group = None
            group_indent = -1
            continue
        m = re.match(r'^(raidz\d*-\d+|mirror-\d+|logs|cache|spares)\s', stripped)
        if m:
            group = m.group(1)
            group_indent = indent
            continue
        fields = stripped.split()
        if not pool or not fields or not fields[0].startswith('/'):
            continue
        grouped = group is not None and indent > group_indent
        leaves.append({
            'pool_name': pool,
            'vdev_name': group if grouped else None,
            'vdev_type': _vdev_type(group) if grouped else 'stripe',
            'path': fields[0],
        })
    return leaves


def get_zpool_leaf_vdevs():
    """
    Every leaf vdev of every imported pool:
    [{'pool_name': 'zroot', 'vdev_name': 'mirror-0', 'vdev_type': 'mirror',
      'path': '/dev/gptid/fa257269-...'}, ...]
    vdev_name is None for a plain stripe member.
    """
    import json
    command = ['/sbin/zpool', 'status', '-j']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc == 0 and out:
        try:
            data = json.loads(''.join(out))
        except (ValueError, TypeError):
            data = None
        if data is not None:
            leaves = []
            for pool_name, pool in data.get('pools', {}).items():
                for key in ('vdevs', 'logs', 'l2cache', 'spares'):
                    _collect_leaf_vdevs(pool.get(key) or {}, pool_name, pool_name, leaves)
            return leaves
    return _zpool_leaf_vdevs_plaintext()


def get_zpool_membership(diskname, partitions=None):
    """
    Checks whether this disk (or any gpart partition on it) is a member of
    an imported zpool. A pool names its devices as it was created: by
    /dev/daNpM, but just as often by label (gpt/zfs0) or by gptid
    (gptid/<uuid>), so every vdev path is resolved to the device it stands
    for before it is compared with this disk.

    Returns a dict:
      {'in_pool': bool, 'pool_name': str or None, 'vdev_name': str or None,
       'vdev_type': str or None,
       'partitions': {'da4p3': {'pool_name': ..., 'vdev_name': ..., 'vdev_type': ...}}}
    The first four describe the first match; 'partitions' has every
    partition (or the bare disk) that is a member.
    """
    result = {'in_pool': False, 'pool_name': None, 'vdev_name': None,
              'vdev_type': None, 'partitions': {}}
    leaves = get_zpool_leaf_vdevs()
    if not leaves:
        return result
    if partitions is None:
        partitions = get_partitions(diskname)
    aliases = get_label_aliases(partitions)
    for leaf in leaves:
        matched = _device_on_disk(resolve_device(leaf['path'], aliases), diskname)
        if not matched:
            continue
        member = {'pool_name': leaf['pool_name'], 'vdev_name': leaf['vdev_name'],
                  'vdev_type': leaf['vdev_type']}
        result['partitions'][matched] = member
        if not result['in_pool']:
            result.update(in_pool=True, **member)
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

    A mount by label (/dev/gpt/somelabel) or by gptid (/dev/gptid/...)
    is resolved to the partition it stands for first.
    """
    command = ['/sbin/mount', '-p']
    out, err, rc = process.call(command)
    result = {'mounted': False, 'mountpoints': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'mounted': False, 'mountpoint': None}
    if rc != 0:
        return result
    aliases = get_label_aliases(partitions)
    for line in out:
        fields = line.split()
        if not fields:
            continue
        device = fields[0]
        mountpoint = fields[1] if len(fields) > 1 else None
        if not device.startswith('/dev/'):
            continue
        matched_name = _device_on_disk(resolve_device(device, aliases), diskname)
        if not matched_name:
            continue
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
    aliases = get_label_aliases(partitions)
    for line in out[1:]:  # first line is the header: "Device  1K-blocks  Used"
        fields = line.split()
        if not fields:
            continue
        device = fields[0]
        if not device.startswith('/dev/'):
            continue
        matched_name = _device_on_disk(resolve_device(device, aliases), diskname)
        if not matched_name:
            continue
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


def _get_zpool_vdevs_plaintext(pool_name):
    """
    Plaintext fallback for get_zpool_vdevs() on OpenZFS < 2.3.
    Parses `zpool status -P` output to extract vdev device paths.
    """
    command = ['/sbin/zpool', 'status', '-P', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths = []
    if rc != 0 or not out:
        return paths
    in_config = False
    for line in out:
        stripped = line.strip()
        if stripped.startswith('config:'):
            in_config = True
            continue
        if stripped.startswith('errors:'):
            in_config = False
            continue
        if not in_config:
            continue
        # leaf vdev lines look like:  /dev/ada0p3   ONLINE   0   0   0
        m = re.match(r'^(/dev/\S+)\s+(\w+)', stripped)
        if m:
            paths.append({
                'path': m.group(1),
                'state': m.group(2),
                'vdev_type': 'disk',
            })
    return paths


def get_zpool_vdevs(pool_name):
    """
    Returns the physical devices backing a pool, as a list of dicts:
    [{'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'}].

    Tries `zpool status -j` (OpenZFS >= 2.3) first, falls back to
    plaintext parsing on older versions.
    """
    import json
    command = ['/sbin/zpool', 'status', '-j', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths = []
    if rc != 0:
        return _get_zpool_vdevs_plaintext(pool_name)
    if not out:
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
    cam_devices = camcontrol_devlist_parser()
    disks = {}
    for dsk, cam_info in cam_devices.items():
        if re.match(r'^cd\d+$', dsk):
            continue
        # ses = SCSI Enclosure Services, pass = CAM passthrough -- never OSD targets
        if re.match(r'^(ses|pass)\d+$', dsk):
            continue
        # strip any accidental /dev/ prefix from the camcontrol parser
        dsk = dsk.replace('/dev/', '')
        disk = get_geom_disk(dsk)
        # For SAS/SCSI (da*) devices geom sometimes can't report mediasize
        # via ATA commands, or reports 0. Augment from camcontrol identify.
        if re.match(r'^da\d+$', dsk):
            raw_size = disk.get('mediasize', '').strip()
            try:
                geom_size = int(raw_size.split()[0]) if raw_size else 0
            except (ValueError, IndexError):
                geom_size = 0
            if geom_size == 0:
                cam_id = get_camcontrol_identify(dsk)
                if cam_id.get('mediasize'):
                    disk.update(cam_id)
        disk['cam'] = cam_info
        disk['gpart'] = get_gpart_info(dsk)
        disk['zpool'] = get_zpool_membership(dsk, partitions=disk['gpart'].get('partitions'))
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

    def pretty_report(self, all=True):
        output = [
            report_template.format(
                geomname='Device Path',
                mediasize='Size',
                rotational='rotates',
                available='avail',
                descr='Model name',
                reason='',
            )]
        for disk in sorted(self.disks):
            output.append(self.disks[disk].report())
        return ''.join(output)

    def verbose_report(self):
        """
        Detailed report for -v: one block per disk with its status, the
        reasons it is not available, its partitions and mount state.
        """
        return '\n\n'.join(self.disks[disk].describe() for disk in sorted(self.disks))

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

        Order matters: zpool/partition/mount checks run first so that
        the most actionable reason appears first -- especially for
        SAS/SCSI (da*) disks where geom reports no mediasize but the
        real reason is zpool membership.
        """
        zpool = self.sys_api.get('zpool', {})
        if zpool.get('in_pool'):
            vdev_name = zpool.get('vdev_name')
            vdev_type = zpool.get('vdev_type') or 'stripe'
            if vdev_name:
                self.reject_reasons.append(
                    'Member of {} vdev in zpool "{}"'.format(
                        vdev_name, zpool.get('pool_name'))
                )
            else:
                self.reject_reasons.append(
                    'Single-disk (no redundancy) member of zpool "{}"'.format(
                        zpool.get('pool_name'))
                )
            self.available = False

        gpart = self.sys_api.get('gpart', {})
        if gpart.get('has_partitions'):
            self.reject_reasons.append(
                'Has an existing {} partition table'.format(gpart.get('scheme'))
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

        # mediasize check last -- for SAS/SCSI (da*) disks via HBA
        # geom often can't read the size via ATA commands, but the
        # real reason they're unavailable is usually zpool membership
        # (checked above). Only flag missing size if nothing else
        # already marked the disk unavailable.
        if not self.reject_reasons:
            mediasize = self.sys_api.get('mediasize')
            try:
                has_usable_size = int(mediasize) > 0
            except (TypeError, ValueError):
                has_usable_size = False
            if not has_usable_size:
                if re.match(r'^da\d+$', self.sys_api.get('geomname', '')):
                    reason = 'No usable media size from geom (SAS/SCSI device via HBA)'
                else:
                    reason = 'No usable media size reported (e.g. empty optical drive, or device not yet attached)'
                self.reject_reasons.append(reason)
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
            reason_str = ''
        else:
            available_str = 'False'
            reason_str = '(' + self.reject_reasons[0] + ')' if self.reject_reasons else ''
        return report_template.format(
            geomname='/dev/' + self.sys_api.get('geomname', self.path).replace('/dev/', ''),
            mediasize=human_readable_size(self._safe_int(self.sys_api.get('mediasize'))),
            rotational=self._safe_int(self.sys_api.get('rotationrate')) != 0,
            available=available_str,
            descr=self.sys_api.get('descr'),
            reason=reason_str,
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
            zpool = self.sys_api.get('zpool', {})
            if zpool.get('in_pool'):
                vdev_name = zpool.get('vdev_name')
                if vdev_name:
                    lines.append(
                        '  partitions: none (whole-disk member of {} in zpool "{}")'.format(
                            vdev_name, zpool.get('pool_name')))
                else:
                    lines.append(
                        '  partitions: none (single-disk member of zpool "{}")'.format(
                            zpool.get('pool_name')))
            else:
                lines.append('  partitions: none (disk is empty)')
        else:
            lines.append('  partitions:')
            for part in partitions:
                lines.append('    {name}  {type}  {size}{label}  ({state})'.format(
                    name=part['name'],
                    type=part['type'],
                    size=part['size_human'],
                    label='  [{}]'.format(part['label']) if part.get('label') else '',
                    state=self._partition_state(part),
                ))
        return '\n'.join(lines)

    def _partition_state(self, part):
        """
        What this partition is being used for: a mount, active swap,
        membership of an imported pool, or - failing those - what its type
        says it is. "not mounted" would be wrong for a ZFS partition, which
        is never in `mount -p`, and for boot code and swap, which are not
        filesystems at all.
        """
        name = part['name']
        mount = self.sys_api.get('mount', {}).get('by_partition', {}).get(name, {})
        swap = self.sys_api.get('swap', {}).get('by_partition', {}).get(name, {})
        member = self.sys_api.get('zpool', {}).get('partitions', {}).get(name)
        if mount.get('mounted'):
            return 'mounted at {}'.format(mount.get('mountpoint'))
        if swap.get('active'):
            return 'active swap'
        if member:
            where = 'zpool "{}"'.format(member.get('pool_name'))
            if member.get('vdev_name'):
                where = '{} in {}'.format(member['vdev_name'], where)
            return 'member of {}'.format(where)
        if part.get('type') == 'freebsd-boot':
            return 'boot code, not a filesystem'
        if part.get('type') == 'freebsd-swap':
            return 'swap, not active'
        mode = part.get('mode')
        if mode and mode != 'r0w0e0':
            return 'in use, open {}'.format(mode)
        return 'not in use'

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items()}
        return output

