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

import pickle
from typing import Type

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory], [tempdatastore.TemporaryAWSS3Bucket]],
)
def test_pickle_file_open(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={"profiles": {profile: temp_data_store.profile_config_dict()}}, profile=profile
            )
        )

        file_path = "file.txt"
        file_content_length = 17
        file_body_bytes = b"\x00" * file_content_length

        # Open a file for writes (bytes).
        with storage_client.open(path=file_path, mode="wb") as file:
            assert not file.readable()
            assert file.writable()
            file.write(file_body_bytes)
            assert file.tell() == file_content_length

        # Check if the file's persisted.
        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.content_length == file_content_length

        # Open the file for reads (bytes).
        with storage_client.open(path=file_path, mode="rb", buffering=0) as file:
            assert file.readall() == file_body_bytes

        # Test pickling of client object.
        client_pickle = pickle.dumps(storage_client)
        client_unpickled = pickle.loads(client_pickle)

        # Open the file for reads (bytes) and read via pickled client.
        with client_unpickled.open(path=file_path) as file:
            assert file.read() == file_body_bytes
