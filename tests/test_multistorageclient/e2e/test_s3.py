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

import multistorageclient as msc
import test_multistorageclient.e2e.common as common
from multistorageclient.types import PreconditionFailedError


@pytest.mark.parametrize("profile_name", ["test-s3-iad", "test-s3-iad-base-path-with-prefix"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_conditional_put(profile_name):
    """Test conditional PUT operations in S3 using if-match and if-none-match conditions."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # S3 uses PreconditionFailedError for both if_none_match="*" and if_match failures
    # and NotImplementedError for if_none_match with specific etag
    common.test_conditional_put(
        storage_provider=client._storage_provider,
        if_none_match_error_type=PreconditionFailedError,
        if_match_error_type=PreconditionFailedError,
        if_none_match_specific_error_type=NotImplementedError,
    )


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_attributes(profile_name):
    """Test S3 attributes functionality - storing custom metadata with msc_ prefix."""
    profile = profile_name
    common.test_attributes(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_shortcuts_rust(profile_name):
    profile = profile_name
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_storage_client_rust(profile_name):
    profile = profile_name
    common.test_storage_client(profile)
