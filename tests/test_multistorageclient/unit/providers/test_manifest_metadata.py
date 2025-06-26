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

import copy
import os
import tempfile
import time

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.manifest_metadata import (
    DEFAULT_MANIFEST_BASE_DIR,
)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "replace_base_path"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory, False],
        [tempdatastore.TemporaryAWSS3Bucket, False],
        [tempdatastore.TemporaryPOSIXDirectory, True],
        [tempdatastore.TemporaryAWSS3Bucket, True],
    ],
)
def test_manifest_metadata(temp_data_store_type: type[tempdatastore.TemporaryDataStore], replace_base_path: bool):
    with temp_data_store_type() as temp_data_store:
        data_profile = "data"
        data_with_manifest_profile = "data_with_manifest"

        data_profile_config_dict = temp_data_store.profile_config_dict()

        base_path = ""
        if replace_base_path:
            base_path = data_profile_config_dict["storage_provider"]["options"]["base_path"].removeprefix("/")

        data_with_manifest_profile_config_dict = copy.deepcopy(data_profile_config_dict) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": os.path.join(base_path, DEFAULT_MANIFEST_BASE_DIR),
                    "writable": True,
                },
            }
        }
        if replace_base_path:
            data_with_manifest_profile_config_dict["storage_provider"]["options"]["base_path"] = "/"

        storage_client_config_dict = {
            "profiles": {
                data_profile: data_profile_config_dict,
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
            }
        }

        file_path = os.path.join(base_path, "dir/file.txt")
        file_content_length = 1
        file_body_bytes = b"\x00" * file_content_length

        # Create the data storage client.
        data_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_profile)
        )
        assert data_storage_client._metadata_provider is None

        # Create the data with manifest storage client.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )
        assert data_with_manifest_storage_client._metadata_provider is not None

        # Check if the manifest metadata tracks no files.
        assert len(list(data_with_manifest_storage_client.list())) == 0
        assert data_with_manifest_storage_client.is_empty(path="dir")

        # Write a file.
        data_with_manifest_storage_client.write(path=file_path, body=file_body_bytes)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        assert data_with_manifest_storage_client.is_empty(path="dir")

        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Check if the manifest is persisted.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1
        assert not data_with_manifest_storage_client.is_empty(path=os.path.join(base_path, "dir"))

        # Check the file metadata.
        file_info = data_with_manifest_storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == file_content_length
        assert file_info.type == "file"
        assert file_info.last_modified is not None

        file_info_list = list(data_with_manifest_storage_client.list(prefix=base_path))
        assert len(file_info_list) == 1
        listed_file_info = file_info_list[0]
        assert listed_file_info is not None
        assert listed_file_info.key.endswith(file_path)
        assert listed_file_info.content_length == file_info.content_length
        assert listed_file_info.type == file_info.type
        assert listed_file_info.last_modified == file_info.last_modified

        # Check that info() detects directories too.
        for dir_path in ["dir", "dir/"]:
            dir_info = data_with_manifest_storage_client.info(path=os.path.join(base_path, dir_path), strict=False)
            assert dir_info.type == "directory"
            assert dir_info.key == os.path.join(base_path, "dir/")
            assert dir_info.content_length == 0

        # But "di" is not a valid directory, even though it is a valid prefix.
        with pytest.raises(FileNotFoundError):
            data_with_manifest_storage_client.info(path="di", strict=False)

        # Delete the file.
        data_with_manifest_storage_client.delete(path=file_path)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0

        # Upload the file.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_body_bytes)
            temp_file.close()
            data_with_manifest_storage_client.upload_file(remote_path=file_path, local_path=temp_file.name)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Check the file metadata.
        file_info = data_with_manifest_storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == file_content_length
        assert file_info.type == "file"

        # Copy the file.
        file_copy_path = os.path.join(base_path, "copy-" + file_path)
        data_with_manifest_storage_client.copy(src_path=file_path, dest_path=file_copy_path)
        assert len(data_with_manifest_storage_client.glob(pattern=file_copy_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_copy_path)) == 1

        # Check the file copy metadata.
        file_copy_info = data_with_manifest_storage_client.info(path=file_copy_path)
        assert file_copy_info is not None
        assert file_copy_info.key.endswith(file_copy_path)
        assert file_copy_info.content_length == file_content_length
        assert file_copy_info.type == "file"

        # Delete the file and its copy.
        for path in [file_path, file_copy_path]:
            data_with_manifest_storage_client.delete(path=path)
        data_with_manifest_storage_client.commit_metadata()

        # Write files.
        file_directory = os.path.join(base_path, "directory")
        file_count = 10
        for i in range(file_count):
            data_storage_client.write(path=os.path.join("directory", f"{i}.txt"), body=file_body_bytes)
        assert len(list(data_with_manifest_storage_client.list(prefix=file_directory + "/"))) == 0

        data_with_manifest_storage_client.commit_metadata(prefix=f"{file_directory}/")
        assert len(list(data_with_manifest_storage_client.list(prefix=file_directory + "/"))) == file_count

        # Test listing with directories
        with_dirs = list(data_with_manifest_storage_client.list(prefix=base_path, include_directories=True))
        assert len(with_dirs) == 1
        assert with_dirs[0].key == file_directory + "/"


def test_nonexistent_and_read_only():
    with tempdatastore.TemporaryPOSIXDirectory() as temp_data_store:
        data_with_manifest_profile = "data_with_manifest"
        data_with_read_only_manifest_profile = "data_with_read_only_manifest"

        data_with_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                },
            }
        }
        data_with_read_only_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": False,
                },
            }
        }

        storage_client_config_dict = {
            "profiles": {
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
                data_with_read_only_manifest_profile: data_with_read_only_manifest_profile_config_dict,
            }
        }

        file_path = "file.txt"
        file_body_bytes = b"\x00"

        # Create the data with manifest storage client.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )

        # Write a file.
        data_with_manifest_storage_client.write(path=file_path, body=file_body_bytes)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Create the data with read-only manifest storage client.
        data_with_read_only_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_read_only_manifest_profile
            )
        )

        # Attempt an overwrite.
        with pytest.raises(FileExistsError):
            data_with_read_only_manifest_storage_client.write(path=file_path, body=file_body_bytes)

        # Attempt a write.
        with pytest.raises(RuntimeError):
            data_with_read_only_manifest_storage_client.write(path=f"nonexistent-{file_path}", body=file_body_bytes)

        # Attempt a non-existent delete.
        with pytest.raises(FileNotFoundError):
            data_with_read_only_manifest_storage_client.delete(path=f"nonexistent-{file_path}")

        # Attempt a delete.
        with pytest.raises(RuntimeError):
            data_with_read_only_manifest_storage_client.delete(path=file_path)


@pytest.mark.parametrize(
    argnames="temp_data_store_type",
    argvalues=[
        tempdatastore.TemporaryAWSS3Bucket,
    ],
)
def test_autocommit(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        manifest_profile = "manifest"

        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        manifest_profile: {
                            **temp_data_store.profile_config_dict(),
                            "metadata_provider": {
                                "type": "manifest",
                                "options": {
                                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                                    "writable": True,
                                },
                            },
                            "autocommit": {
                                "interval_minutes": 0.05,  # 0.05 minutes = 3 seconds
                                "at_exit": False,
                            },
                        }
                    }
                },
                profile=manifest_profile,
            )
        )

        file_count = 10
        for i in range(file_count):
            fname = f"folder/filename-{i}.txt"
            if not storage_client.is_file(fname):
                storage_client.write(fname, f"contents for {i}")

        # Wait 3 seconds for the autocommit to commit the files.
        time.sleep(5)

        assert len(list(storage_client.list(prefix="folder/"))) == file_count
        for i in range(file_count):
            fname = f"folder/filename-{i}.txt"
            assert storage_client.is_file(fname)
            assert storage_client.open(fname, mode="r").read() == f"contents for {i}"
