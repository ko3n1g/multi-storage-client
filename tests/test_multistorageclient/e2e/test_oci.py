# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import test_multistorageclient.e2e.common as common
import multistorageclient as msc
from multistorageclient.types import PreconditionFailedError


@pytest.mark.parametrize("profile_name", ["test-oci"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_oci_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-oci"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_oci_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-oci"])
def test_oci_conditional_put(profile_name):
    """Test conditional PUT operations in OCI using if-match and if-none-match conditions."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # https://docs.oracle.com/en-us/iaas/tools/python/2.150.0/api/object_storage/client/oci.object_storage.ObjectStorageClient.html?highlight=put_object#oci.object_storage.ObjectStorageClient.put_object
    # OCI uses PreconditionFailedError for both if_none_match="*" and if_match failures
    # and does not support if_none_match with specific etag
    common.test_conditional_put(
        storage_provider=client._storage_provider,
        if_none_match_error_type=PreconditionFailedError,
        if_match_error_type=PreconditionFailedError,
        if_none_match_specific_error_type=None,
    )
