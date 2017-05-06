# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright (c) 2010 Citrix Systems, Inc.
# Copyright (c) 2011 Piston Cloud Computing, Inc
# Copyright (c) 2012 University Of Minho
# (c) Copyright 2013 Hewlett-Packard Development Company, L.P.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import eventlet

from oslo_log import log as logging
from oslo_utils import excutils, importutils

from os_brick import encryptors
from nova import utils, exception
import nova.conf
from nova.i18n import _LI
from nova.i18n import _LW
from nova.i18n import _LE
from oslo_concurrency import processutils
from nova.utils import get_root_helper

"""
A connection to a hypervisor through libvirt.

Supports KVM, LXC, QEMU, UML, XEN and Parallels.

"""

import nova.virt.libvirt.driver as orglibvdriver
from nova.virt import driver

CONF = nova.conf.CONF
LOG = logging.getLogger(__name__)

orglibvdriver.libvirt_volume_drivers = [
    'iscsi=nova.virt.libvirt.volume.iscsi.LibvirtISCSIVolumeDriver',
    'iser=nova.virt.libvirt.volume.iser.LibvirtISERVolumeDriver',
    'local=nova.virt.libvirt.volume.volume.LibvirtVolumeDriver',
    'fake=nova.virt.libvirt.volume.volume.LibvirtFakeVolumeDriver',
    'rbd=nova.virt.libvirt.volume.rbd.LibvirtRBDVolumeDriver',
    'sheepdog=nova.virt.libvirt.volume.net.LibvirtNetVolumeDriver',
    'nfs=nova.virt.libvirt.volume.nfs.LibvirtNFSVolumeDriver',
    'smbfs=nova.virt.libvirt.volume.smbfs.LibvirtSMBFSVolumeDriver',
    'aoe=nova.virt.libvirt.volume.aoe.LibvirtAOEVolumeDriver',
    'glusterfs='
        'nova.virt.libvirt.volume.glusterfs.LibvirtGlusterfsVolumeDriver',
    'fibre_channel='
        'nova.virt.libvirt.volume.fibrechannel.'
        'LibvirtFibreChannelVolumeDriver',
    'scality=nova.virt.libvirt.volume.scality.LibvirtScalityVolumeDriver',
    'gpfs=nova.virt.libvirt.volume.gpfs.LibvirtGPFSVolumeDriver',
    'quobyte=nova.virt.libvirt.volume.quobyte.LibvirtQuobyteVolumeDriver',
    'hgst=nova.virt.libvirt.volume.hgst.LibvirtHGSTVolumeDriver',
    'scaleio=nova.virt.libvirt.volume.scaleio.LibvirtScaleIOVolumeDriver',
    'disco=nova.virt.libvirt.volume.disco.LibvirtDISCOVolumeDriver',
    'vzstorage='
        'nova.virt.libvirt.volume.vzstorage.LibvirtVZStorageVolumeDriver',
]

class LibvirtDriver2(orglibvdriver.LibvirtDriver):
    def _get_volume_encryptor(self, connection_info, encryption):
        root_helper = get_root_helper()
        cls_keymgr = importutils.import_class(CONF.key_manager.api_class)
        encryptor = encryptors.get_volume_encryptor(root_helper,
                                                    connection_info,
                                                    cls_keymgr(CONF),
                                                    **encryption)
        return encryptor

    def _create_domain_and_network(self, context, xml, instance, network_info,
                                   disk_info, block_device_info=None,
                                   power_on=True, reboot=False,
                                   vifs_already_plugged=False,
                                   post_xml_callback=None,
                                   destroy_disks_on_failure=False):

        """Do required network setup and create domain."""
        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)

        for vol in block_device_mapping:
            connection_info = vol['connection_info']

            if ('data' in connection_info and
                    'volume_id' in connection_info['data']):
                volume_id = connection_info['data']['volume_id']
                encryption = encryptors.get_encryption_metadata(
                    context, self._volume_api, volume_id, connection_info)

                if encryption:
                    encryptor = self._get_volume_encryptor(connection_info,
                                                           encryption)
                    try:
                        encryptor.attach_volume(context, **encryption)
                    except processutils.ProcessExecutionError as e:
                        if e.exit_code == 5:
                            LOG.info(_LI('volume %(volume)s is already'
                                         ' encrypted'),
                                     {'volume': volume_id},
                                     instance=instance)
                        else:
                            raise

        timeout = CONF.vif_plugging_timeout
        if (self._conn_supports_start_paused and
            utils.is_neutron() and not
            vifs_already_plugged and power_on and timeout):
            events = self._get_neutron_events(network_info)
        else:
            events = []

        pause = bool(events)
        guest = None
        try:
            with self.virtapi.wait_for_instance_event(
                    instance, events, deadline=timeout,
                    error_callback=self._neutron_failed_callback):
                self.plug_vifs(instance, network_info)
                self.firewall_driver.setup_basic_filtering(instance,
                                                           network_info)
                self.firewall_driver.prepare_instance_filter(instance,
                                                             network_info)
                with self._lxc_disk_handler(instance, instance.image_meta,
                                            block_device_info, disk_info):
                    guest = self._create_domain(
                        xml, pause=pause, power_on=power_on,
                        post_xml_callback=post_xml_callback)

                self.firewall_driver.apply_instance_filter(instance,
                                                           network_info)
        except exception.VirtualInterfaceCreateException:
            # Neutron reported failure and we didn't swallow it, so
            # bail here
            with excutils.save_and_reraise_exception():
                self._cleanup_failed_start(context, instance, network_info,
                                           block_device_info, guest,
                                           destroy_disks_on_failure)
        except eventlet.timeout.Timeout:
            # We never heard from Neutron
            LOG.warning(_LW('Timeout waiting for vif plugging callback for '
                         'instance %(uuid)s'), {'uuid': instance.uuid},
                     instance=instance)
            if CONF.vif_plugging_is_fatal:
                self._cleanup_failed_start(context, instance, network_info,
                                           block_device_info, guest,
                                           destroy_disks_on_failure)
                raise exception.VirtualInterfaceCreateException()
        except Exception:
            # Any other error, be sure to clean up
            LOG.error(_LE('Failed to start libvirt guest'),
                      instance=instance)
            with excutils.save_and_reraise_exception():
                self._cleanup_failed_start(context, instance, network_info,
                                           block_device_info, guest,
                                           destroy_disks_on_failure)

        # Resume only if domain has been paused
        if pause:
            guest.resume()
        return guest