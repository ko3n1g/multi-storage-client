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


@pytest.mark.parametrize("profile_name", ["test-swift-pdx", "test-swift-pdx-base-path-with-prefix"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx"])
def test_swift_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)
