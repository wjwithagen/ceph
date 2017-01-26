#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Owen Synge <osynge@suse.com>
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see `<http://www.gnu.org/licenses/>`.
#
import logging
import mock
import testtools

import ceph_detect_init
from ceph_detect_init import alpine
from ceph_detect_init import arch
from ceph_detect_init import centos
from ceph_detect_init import debian
from ceph_detect_init import exc
from ceph_detect_init import fedora
from ceph_detect_init import main
from ceph_detect_init import rhel
from ceph_detect_init import suse
from ceph_detect_init import gentoo
from ceph_detect_init import freebsd

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCephDetectInit(testtools.TestCase):

    def test_alpine(self):
        self.assertEqual('openrc', alpine.choose_init())

    def test_arch(self):
        self.assertEqual('systemd', arch.choose_init())

    def test_freebsd(self):
        self.assertEqual('bsdrc', freebsd.choose_init())

    def test_centos(self):
        with mock.patch('ceph_detect_init.centos.release',
                        '7.0'):
            self.assertEqual('systemd', centos.choose_init())
        self.assertEqual('sysvinit', centos.choose_init())

    def test_debian(self):
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='debian',
                                 codename='wheezy'):
            self.assertEqual('sysvinit', debian.choose_init())
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='debian',
                                 codename='squeeze'):
            self.assertEqual('sysvinit', debian.choose_init())
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='debian',
                                 codename='jessie'):
            self.assertEqual('systemd', debian.choose_init())
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='ubuntu',
                                 codename='trusty'):
            self.assertEqual('upstart', debian.choose_init())
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='ubuntu',
                                 codename='vivid'):
            self.assertEqual('systemd', debian.choose_init())
        with mock.patch.multiple('ceph_detect_init.debian',
                                 distro='not-debian',
                                 codename='andy'):
            self.assertIs(None, debian.choose_init())

    def test_fedora(self):
        with mock.patch('ceph_detect_init.fedora.release',
                        '22'):
            self.assertEqual('systemd', fedora.choose_init())
        self.assertEqual('sysvinit', fedora.choose_init())

    def test_rhel(self):
        with mock.patch('ceph_detect_init.rhel.release',
                        '7.0'):
            self.assertEqual('systemd', rhel.choose_init())
        self.assertEqual('sysvinit', rhel.choose_init())

    def test_suse(self):
        with mock.patch('ceph_detect_init.suse.release',
                        '11'):
            self.assertEqual('sysvinit', suse.choose_init())
        with mock.patch('ceph_detect_init.suse.release',
                        '12'):
            self.assertEqual('systemd', suse.choose_init())
        with mock.patch('ceph_detect_init.suse.release',
                        '13.1'):
            self.assertEqual('systemd', suse.choose_init())
        with mock.patch('ceph_detect_init.suse.release',
                        '13.2'):
            self.assertEqual('systemd', suse.choose_init())

    def test_gentoo_is_openrc(self):
        with mock.patch('os.path.isdir', return_value=True):
            self.assertEqual(gentoo.is_openrc(), True)
        with mock.patch('os.path.isdir', return_value=False):
            self.assertEqual(gentoo.is_openrc(), False)

    def test_gentoo_is_systemd(self):
        import sys
        if sys.version_info >= (3, 0):
            mocked_fn = 'builtins.open'
        else:
            mocked_fn = '__builtin__.open'

        f = mock.mock_open(read_data='systemd')
        with mock.patch(mocked_fn, f, create=True) as m:
            self.assertEqual(gentoo.is_systemd(), True)
            m.assert_called_once_with('/proc/1/comm')
        f = mock.mock_open(read_data='init')
        with mock.patch(mocked_fn, f, create=True) as m:
            self.assertEqual(gentoo.is_systemd(), False)
            m.assert_called_once_with('/proc/1/comm')
        f = mock.mock_open(read_data='upstart')
        with mock.patch(mocked_fn, f, create=True) as m:
            self.assertEqual(gentoo.is_systemd(), False)
            m.assert_called_once_with('/proc/1/comm')

    def test_gentoo(self):
        with mock.patch.multiple('ceph_detect_init.gentoo',
                                 is_systemd=(lambda: True),
                                 is_openrc=(lambda: True)):
            self.assertEqual('openrc', gentoo.choose_init())
        with mock.patch.multiple('ceph_detect_init.gentoo',
                                 is_systemd=(lambda: True),
                                 is_openrc=(lambda: False)):
            self.assertEqual('systemd', gentoo.choose_init())
        with mock.patch.multiple('ceph_detect_init.gentoo',
                                 is_systemd=(lambda: False),
                                 is_openrc=(lambda: True)):
            self.assertEqual('openrc', gentoo.choose_init())
        with mock.patch.multiple('ceph_detect_init.gentoo',
                                 is_systemd=(lambda: False),
                                 is_openrc=(lambda: False)):
            self.assertEqual('unknown', gentoo.choose_init())

    def test_get(self):
        with mock.patch.multiple(
                'platform',
                system=lambda: 'Linux',
                linux_distribution=lambda **kwargs: (('unknown', '', ''))):
            g = ceph_detect_init.get
            self.assertRaises(exc.UnsupportedPlatform, g)
            try:
                g()
            except exc.UnsupportedPlatform as e:
                self.assertIn('Platform is not supported', str(e))

        with mock.patch.multiple(
                'platform',
                system=lambda: 'Linux',
                linux_distribution=lambda **kwargs: (('debian', '6.0', ''))):
            distro = ceph_detect_init.get()
            self.assertEqual(debian, distro)
            self.assertEqual('debian', distro.name)
            self.assertEqual('debian', distro.normalized_name)
            self.assertEqual('debian', distro.distro)
            self.assertEqual(False, distro.is_el)
            self.assertEqual('6.0', distro.release)
            self.assertEqual('squeeze', distro.codename)
            self.assertEqual('sysvinit', distro.init)

        with mock.patch('platform.system', lambda: 'FreeBSD'):
            with mock.patch.multiple('platform',
                                     release=lambda: '12.0-CURRENT',
                                     version=lambda: 'FreeBSD 12 1 r306554M:'):
                distro = ceph_detect_init.get()
                self.assertEqual(freebsd, distro)
                self.assertEqual('freebsd', distro.name)
                self.assertEqual('freebsd', distro.normalized_name)
                self.assertEqual('freebsd', distro.distro)
                self.assertFalse(distro.is_el)
                self.assertEqual('12.0-CURRENT', distro.release)
                self.assertEqual('r306554M', distro.codename)
                self.assertEqual('bsdrc', distro.init)

        with mock.patch('platform.system',
                        lambda: 'cephix'):
            self.assertRaises(exc.UnsupportedPlatform, ceph_detect_init.get)

    def test_get_distro(self):
        g = ceph_detect_init._get_distro
        self.assertEqual(None, g(None))
        self.assertEqual(debian, g('debian'))
        self.assertEqual(debian, g('ubuntu'))
        self.assertEqual(centos, g('centos'))
        self.assertEqual(centos, g('scientific'))
        self.assertEqual(fedora, g('fedora'))
        self.assertEqual(suse, g('suse'))
        self.assertEqual(rhel, g('redhat', use_rhceph=True))
        self.assertEqual(gentoo, g('gentoo'))

    def test_normalized_distro_name(self):
        n = ceph_detect_init._normalized_distro_name
        self.assertEqual('redhat', n('RedHat'))
        self.assertEqual('redhat', n('redhat'))
        self.assertEqual('redhat', n('Red Hat'))
        self.assertEqual('redhat', n('red hat'))
        self.assertEqual('scientific', n('scientific'))
        self.assertEqual('scientific', n('Scientific'))
        self.assertEqual('scientific', n('Scientific Linux'))
        self.assertEqual('scientific', n('scientific linux'))
        self.assertEqual('suse', n('SUSE'))
        self.assertEqual('suse', n('suse'))
        self.assertEqual('suse', n('openSUSE'))
        self.assertEqual('suse', n('opensuse'))
        self.assertEqual('centos', n('CentOS'))
        self.assertEqual('centos', n('centos'))
        self.assertEqual('debian', n('Debian'))
        self.assertEqual('debian', n('debian'))
        self.assertEqual('ubuntu', n('Ubuntu'))
        self.assertEqual('ubuntu', n('ubuntu'))
        self.assertEqual('gentoo', n('Gentoo'))
        self.assertEqual('gentoo', n('gentoo'))
        self.assertEqual('gentoo', n('Funtoo'))
        self.assertEqual('gentoo', n('funtoo'))
        self.assertEqual('gentoo', n('Exherbo'))
        self.assertEqual('gentoo', n('exherbo'))

    @mock.patch('platform.system', lambda: 'Linux')
    def test_platform_information_linux(self):
        with mock.patch('platform.linux_distribution',
                        lambda **kwargs: (('debian', '6.0', ''))):
            self.assertEqual(('debian', '6.0', 'squeeze'),
                             ceph_detect_init.platform_information())

            with mock.patch('platform.linux_distribution',
                            lambda **kwargs: (('debian', '7.0', ''))):
                self.assertEqual(('debian', '7.0', 'wheezy'),
                                 ceph_detect_init.platform_information())

            with mock.patch('platform.linux_distribution',
                            lambda **kwargs: (('debian', '8.0', ''))):
                self.assertEqual(('debian', '8.0', 'jessie'),
                                 ceph_detect_init.platform_information())

            with mock.patch('platform.linux_distribution',
                            lambda **kwargs: (('debian', 'jessie/sid', ''))):
                self.assertEqual(('debian', 'jessie/sid', 'sid'),
                                 ceph_detect_init.platform_information())

            with mock.patch('platform.linux_distribution',
                            lambda **kwargs: (('debian', 'sid/jessie', ''))):
                self.assertEqual(('debian', 'sid/jessie', 'sid'),
                                 ceph_detect_init.platform_information())

    @mock.patch('platform.system', lambda: 'FreeBSD')
    def test_platform_information_freebsd(self):
        with mock.patch.multiple('platform',
                                 release=lambda: '12.0-CURRENT',
                                 version=lambda: 'FreeBSD 12.0 #1 r306554M:'):
            self.assertEqual(('freebsd', '12.0-CURRENT', 'r306554M'),
                             ceph_detect_init.platform_information())

    def test_run(self):
        argv = ['--use-rhceph', '--verbose']
        self.assertEqual(0, main.run(argv))

        with mock.patch.multiple(
                'platform',
                system=lambda: 'Linux',
                linux_distribution=lambda **kwargs: (('unknown', '', ''))):
            self.assertRaises(exc.UnsupportedPlatform, main.run, argv)
            self.assertEqual(0, main.run(argv + ['--default=sysvinit']))

# Local Variables:
# compile-command: "cd .. ; .tox/py27/bin/py.test tests/test_all.py"
# End:
