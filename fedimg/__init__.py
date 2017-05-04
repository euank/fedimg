# This file is part of fedimg.
# Copyright (C) 2014 Red Hat, Inc.
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
#           Sayan Chowdhury <sayanchowdhury@fedoraproject.org>

import ConfigParser


# Read in config file
config = ConfigParser.RawConfigParser()
config.read('/etc/fedimg.cfg')

CLEAN_UP_ON_FAILURE = config.get('general', 'clean_up_on_failure')
DELETE_IMAGES_ON_FAILURE = config.get('general', 'delete_images_on_failure')

# AMAZON WEB SERVICES (EC2)
AWS_ACCESS_ID = config.get('aws', 'access_id')
AWS_SECRET_KEY = config.get('aws', 'secret_key')
AWS_KEYNAME = config.get('aws', 'keyname')
AWS_KEYPATH = config.get('aws', 'keypath')
AWS_PUBKEYPATH = config.get('aws', 'pubkeypath')
AWS_REGIONS = config.get('aws', 'regions')
AWS_IAM_PROFILE = config.get('aws', 'iam_profile')
AWS_VOL_SIZE = config.get('aws', 'vol_size')
AWS_BUCKET_NAME = config.get('aws', 'bucket_name')

# RACKSPACE
RACKSPACE_USER = config.get('rackspace', 'username')
RACKSPACE_API_KEY = config.get('rackspace', 'api_key')

# GCE
GCE_EMAIL = config.get('gce', 'email')
GCE_KEYPATH = config.get('gce', 'keypath')
GCE_PROJECT_ID = config.get('gce', 'project_id')

# HP
HP_USER = config.get('hp', 'username')
HP_PASSWORD = config.get('hp', 'password')
HP_TENANT = config.get('hp', 'tenant')

