# This file is part of fedimg.
# Copyright (C) 2014-17 Red Hat, Inc.
#
# fedimg is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# fedimg is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with fedimg; if not, see http://www.gnu.org/licenses,
# or write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Authors:  David Gay <dgay@redhat.com>
#           Ralph Bean <rbean@redhat.com>
#           Sayan Chowdhury <sayanchowdhury@fedoraproject.org>

import logging
import re

from retrying import retry

import fedimg
import fedimg.messenger

from fedimg.util import get_file_arch, region_to_driver, retry_if_result_false
from fedimg.util import run_system_command

log = logging.getLogger("fedmsg")


class EC2ServiceException(Exception):
    """ Custom exception for EC2Service. """
    pass


class EC2UtilityException(EC2ServiceException):
    """ Something went wrong with writing the image file to a volume with the
        utility instance. """
    pass


class EC2AMITestException(EC2ServiceException):
    """ Something went wrong when a newly-registered AMI was tested. """
    pass


class EC2Service(object):
    """ An object for interacting with an EC2 upload process.
        Takes a URL to a raw.xz image. """

    def __init__(self, raw_url, cwd, virt_type='hvm', vol_type='standard'):

        self.raw_url = raw_url
        self.virt_type = virt_type
        self.vol_type = vol_type

        # Location of the downloaded image file
        self.cwd = cwd

        # All of these are set to appropriate values
        # throughout the upload process.
        self.volume = None
        self.image = None
        self.snapshot = None

        regions = fedimg.AWS_REGIONS.split('|')

        # `region` is the region where the first AMI is created
        self.region = regions[0]

        # Get file name, build name, a description, and the image arch
        # all from the .raw.xz file name.
        self.file_name = self.raw_url.split('/')[-1]
        self.build_name = self.file_name.replace('.raw.xz', '')
        self.image_desc = "Created from build {0}".format(self.build_name)
        self.image_arch = get_file_arch(self.file_name)

    def _clean_up(self, driver, delete_images=False):
        """ Cleans up resources via a libcloud driver. """
        log.info('Cleaning up resources')

        if delete_images and self.image is not None:
            driver.delete_image(self.image)

        if self.snapshot and self.image is not None:
            driver.destroy_volume_snapshot(self.snapshot)
            self.snapshot = None

    def upload(self, compose_meta):
        """ Registers the image in each EC2 region. """

        log.info('EC2 upload process started')

        region = self.region
        self.destination = 'EC2 ({region})'.format(region=region)

        fedimg.messenger.message('image.upload', self.raw_url,
                                 self.destination, 'started',
                                 compose=compose_meta)

        cls = region_to_driver(region)
        self.driver = cls(fedimg.AWS_ACCESS_ID, fedimg.AWS_SECRET_KEY)

        bucket_name = '{bucket_name}-{region}'.format(
            bucket_name=fedimg.AWS_BUCKET_NAME,
            region=region
        )
        availability_zone = self.get_availability_zone()

        params = {
            'image_name': self.file_name,
            'image_format': 'raw',
            'region': region,
            'bucket_name': bucket_name,
            'availability_zone': availability_zone.name,
        }
        out, err = self.import_image_volume(**params)
        task_id = self.match_regex_pattern(regex='\s(import-vol-\w{8})',
                                           output=out)

        volume_id = self.describe_conversion_tasks(task_id, region)
        self.create_snapshot(volume_id)

        # Make the snapshot public, so that the AMIs can be copied
        self.driver.ex_modify_snapshot_attribute(self.snapshot, {
            'CreateVolumePermission.Add.1.Group': 'all'
        })

        # Delete the volume now that we've got the snapshot
        log.info('Destroying the volume: %s' % volume_id)
        self.driver.destroy_volume(self.volume)

        # Make sure Fedimg knows that the vol is gone
        self.volume = None
        log.info('Destroyed volume')

        # Actually register image
        log.info('Registering image as an AMI')

        if self.virt_type == 'paravirtual':
            image_name = "{0}-{1}-PV-{2}-0".format(
                self.build_name,
                region,
                self.vol_type
            )
            reg_root_device_name = '/dev/sda'
        else:  # HVM
            image_name = "{0}-{1}-HVM-{2}-0".format(
                self.build_name,
                region,
                self.vol_type
            )
            reg_root_device_name = '/dev/sda1'

        # For this block device mapping, we have our volume be
        # based on the snapshot's ID
        mapping = [{
            'DeviceName': reg_root_device_name,
            'Ebs': {
                'SnapshotId': self.snapshot.id,
                'VolumeSize': fedimg.AWS_VOL_SIZE,
                'VolumeType': self.vol_type,
                'DeleteOnTermination': 'true'
            }
        }]

        log.info('Start AMI registration')
        self.register_image(image_name, reg_root_device_name, mapping)
        log.info('AMI registration complete')

        fedimg.messenger.message(
            'image.create', self.raw_url, self.destination, 'created',
            compose_meta, extra={
                'id': self.image.id,
                'virt_type': self.virt_type,
                'vol_type': self.vol_type,
                'region': region
            }
        )

    def create_snapshot(self, volume_id):
        """
        Create the snapshot out of the volume created

        :param volume_id: Volume id
        :type volume_id: ``str``
        """

        SNAPSHOT_NAME = 'fedimg-snap-{build_name}'.format(
            build_name=self.build_name)

        log.info('Taking a snapshot of the volume: %s' % volume_id)
        # Take a snapshot of the volume the image was written to
        self.volume = [v for v in self.driver.list_volumes()
                       if v.id == volume_id][0]

        self.snapshot = self.driver.create_volume_snapshot(self.volume,
                                                           name=SNAPSHOT_NAME)

        return self._check_snapshot_exists(str(self.snapshot.id))

    @retry(retry_on_result=retry_if_result_false, wait_fixed=5000)
    def describe_conversion_tasks(self, task_id, region):
        """
        Executes the command ``euca-describe-conversion-tasks`` and checks if
        the task has been completed.

        :param task_id: Task id
        :type task_id: ``str``

        :param region: Region
        :type region: ``str``
        """
        params = {
            'task_id': task_id,
            'region': region
        }
        CMD = 'euca-describe-conversion-tasks {task_id} --region {region}'

        log.info('Retreiving information for task_id: %s' % task_id)
        cmd = CMD.format(**params)
        out, err = run_system_command(cmd)

        if 'completed' in out:
            match = re.search('\s(vol-\w{17})', out)
            volume_id = match.group(1)
            log.info('Volume Created: %s' % volume_id)
            return volume_id
        else:
            return ''

    def download_image(self, image_url):
        """
        Downloads the raw image for the image to be uploaded to all the regions.

        :param image_url: URL of the image
        :type image_url: ``str``
        """
        cmd = "wget {image_url} -P {location}".format(image_url=image_url,
                                                      location=self.cwd)
        out, err = run_system_command(cmd)

        return out, err

    def get_availability_zone(self):
        """
        Returns a availability zone for the region
        """
        availabilty_zone = self.driver.ex_list_availability_zones(
                only_available=True)
        return availabilty_zone[0]

    def import_image_volume(self, image_name, image_format, region,
                            bucket_name, availability_zone):
        """
        Executes the command ``euca-import-volume`` and imports a volume in AWS

        :param image_name: Name of the image
        :type image_name: ``str``

        :param region: Region
        :type region: ``str``

        :param bucket_name: Name of the bucket
        :type bucket_name: ``str``

        :param availability_zone: Availability Zone
        :type availability_zone: ``str``
        """
        params = {
            'location': self.cwd,
            'image_name': image_name,
            'image_format': image_format,
            'region': region,
            'bucket_name': bucket_name,
            'availability_zone': availability_zone,
        }
        cmd = 'euca-import-volume {location}/{image_name} -f {image_format} --region \
               {region} -b {bucket_name} -z {availability_zone}'.format(**params)

        out, err = run_system_command(cmd)

        return out, err

    def match_regex_pattern(self, regex, output):
        """
        Returns the taskid from the output
        :param regex: regex pattern
        :type regex: ``str``

        :param output: output in which the pattern would be searched
        :type output: ``str``
        """
        match = re.search(regex, output)
        if match is None:
            return ''
        else:
            return match.group(1)

    def register_image(self, image_name, reg_root_device_name,
                       blk_device_mapping):
        """
        Registers an AMI using the snapshot created.

        :param image_name: Name of the image
        :type regex: ``str``

        :param reg_root_device_name: Root Device Name
        :type regex: ``str``

        :param blk_device_mapping: Block Device mapping
        :type regex: ``str``
        """
        #TODO: Conver this method from `while` loop to @retry
        # Avoid duplicate image name by incrementing the number at the
        # end of the image name if there is already an AMI with that name.
        cnt = 0
        while True:
            if cnt > 0:
                image_name = re.sub(
                    '\d(?!\d)',
                    lambda x: str(int(x.group(0)) + 1),
                    image_name)
            try:
                self.image = self.driver.ex_register_image(
                    name=image_name,
                    description=self.image_desc,
                    root_device_name=reg_root_device_name,
                    block_device_mapping=blk_device_mapping,
                    virtualization_type=self.virt_type,
                    architecture=self.image_arch
                )
                break
            except Exception as e:
                if 'InvalidAMIName.Duplicate' in str(e):
                    log.debug('AMI %s exists. Retying again' % image_name)
                    cnt += 1
                else:
                    raise
        return

    @retry(retry_on_result=retry_if_result_false, wait_fixed=10000)
    def _check_snapshot_exists(self, snapshot_id):
        """
        Check if the snapshot has been created.

        :param snapshot_id: Id of the snapshot
        :type snapshot_id: ``str``
        """
        if self.snapshot.extra['state'] != 'completed':
            self.snapshot = [snapshot
                             for snapshot in self.driver.list_snapshots()
                             if snapshot.id == snapshot_id][0]
            return False
        else:
            log.info('Snapshot Taken')
            return True
