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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with fedimg; if not, see http://www.gnu.org/licenses,
# or write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Authors:  David Gay <dgay@redhat.com>
#           Sayan Chowdhury <sayanchowdhury@fedoraproject.org>

import shutil
import tempfile

from fedimg.services.ec2 import EC2Service
from fedimg.util import virt_types_from_url, run_system_command

import logging
log = logging.getLogger("fedmsg")


def download_image(image_url, cwd):
    """
    Downloads the raw image for the image to be uploaded to all the regions.

    :param image_url: URL of the image
    :type image_url: ``str``
    """
    cmd = "wget {image_url} -P {location}".format(image_url=image_url,
                                                  location=cwd)
    out, err = run_system_command(cmd)

    return out, err


def upload(pool, urls, compose_meta):
    """ Takes a list (urls) of one or more .raw.xz image files and
    sends them off to cloud services for registration. The upload
    jobs threadpool must be passed as `pool`."""

    log.info('Starting upload process')

    services = []

    tmpdir = tempfile.mkdtemp()
    log.info(" Preparing temporary directory for download: %s" % tmpdir)

    for url in urls:
        # Downloading the images in the tmp directory
        out, err = download_image(url, tmpdir)

        # EC2 upload
        log.info("  Preparing to upload %r" % url)
        for vt in virt_types_from_url(url):
            services.append(EC2Service(url, tmpdir, virt_type=vt,
                                       vol_type='standard'))
            services.append(EC2Service(url, tmpdir, virt_type=vt,
                                       vol_type='gp2'))

    pool.map(lambda s: s.upload(compose_meta), services)

    log.info(" Cleaning up tmp directories: %s" % tmpdir)
    shutil.rmtree(tmpdir)
