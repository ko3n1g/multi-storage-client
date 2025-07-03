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
import time

import pytest

import multistorageclient as msc
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from test_multistorageclient.unit.utils import config, tempdatastore


def get_file_timestamp(uri: str) -> float:
    client, path = msc.resolve_storage_client(uri)
    response = client.info(path=path)
    return response.last_modified.timestamp()


def create_local_test_dataset(target_profile: str, expected_files: dict) -> None:
    """Creates test files based on expected_files dictionary."""
    target_client, target_path = msc.resolve_storage_client(target_profile)
    for rel_path, content in expected_files.items():
        path = os.path.join(target_path, rel_path)
        target_client.write(path, content.encode("utf-8"))


def verify_sync_and_contents(target_url: str, expected_files: dict):
    """Verifies that all expected files exist in the target storage and their contents are correct."""
    for file, expected_content in expected_files.items():
        target_file_url = os.path.join(target_url, file)
        assert msc.is_file(target_file_url), f"Missing file: {target_file_url}"
        actual_content = msc.open(target_file_url).read().decode("utf-8")
        assert actual_content == expected_content, f"Mismatch in file {file}"
    # Ensure there is nothing in target that is not in expected_files
    target_client, target_path = msc.resolve_storage_client(target_url)
    for targetf in target_client.list(prefix=target_path):
        key = targetf.key[len(target_path) :].lstrip("/")
        assert key in expected_files


@pytest.mark.serial
@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "sync_kwargs"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, {}],  # Default settings
        [tempdatastore.TemporaryAWSS3Bucket, {"max_workers": 1}],  # Serial execution
        [tempdatastore.TemporaryAWSS3Bucket, {"max_workers": 4}],  # Parallel with 4 workers
    ],
)
def test_sync_function(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
    sync_kwargs: dict,
):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    # set environment variables to control multiprocessing
    os.environ["MSC_NUM_PROCESSES"] = str(sync_kwargs.get("max_workers", 1))

    obj_profile = "s3-sync"
    local_profile = "local"
    second_profile = "second"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as second_local_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        with_manifest_profile_config_dict = copy.deepcopy(second_local_data_store.profile_config_dict()) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                },
            }
        }

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                    second_profile: with_manifest_profile_config_dict,
                }
            }
        )

        target_msc_url = f"msc://{obj_profile}/synced-files"
        source_msc_url = f"msc://{local_profile}"
        second_msc_url = f"msc://{second_profile}/some"

        # Create local dataset
        expected_files = {
            "dir1/file0.txt": "a" * 100,
            "dir1/file1.txt": "b" * 100,
            "dir1/file2.txt": "c" * 100,
            "dir2/file0.txt": "d" * 100,
            "dir2/file1.txt": "e" * 100,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 100,
            "dir3/file1.txt": "h" * 100,
            "dir3/file2.txt": "i" * 100,
        }
        create_local_test_dataset(source_msc_url, expected_files)
        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Deleting file at target and syncing again")
        msc.delete(os.path.join(target_msc_url, "dir1/file0.txt"))
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Syncing again and verifying timestamps")
        timestamps_before = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        timestamps_after = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        assert timestamps_before == timestamps_after, "Timestamps changed on second sync."

        print("Adding new files and syncing again")
        new_files = {"dir1/new_file.txt": "n" * 100}
        create_local_test_dataset(source_msc_url, expected_files=new_files)
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        expected_files.update(new_files)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Modifying one of the source files, but keeping size the same, and verifying it's copied.")
        modified_files = {"dir1/file0.txt": "z" * 100}
        create_local_test_dataset(source_msc_url, expected_files=modified_files)
        expected_files.update(modified_files)
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=source_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=target_msc_url, target_url=target_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=os.path.join(source_msc_url, "extra"))

        print("Syncing from object to a second posix file location using ManifestProvider.")
        msc.sync(source_url=target_msc_url, target_url=second_msc_url)
        verify_sync_and_contents(target_url=second_msc_url, expected_files=expected_files)

        print("Deleting all the files at the target and going again.")
        for key in expected_files.keys():
            msc.delete(os.path.join(target_msc_url, key))

        print("Syncing using prefixes to just copy one subfolder.")
        msc.sync(source_url=os.path.join(source_msc_url, "dir2"), target_url=os.path.join(target_msc_url, "dir2"))
        sub_expected_files = {k: v for k, v in expected_files.items() if k.startswith("dir2")}
        verify_sync_and_contents(target_url=target_msc_url, expected_files=sub_expected_files)

        msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        print("Deleting files at the source and syncing again, verify deletes at target.")
        keys_to_delete = [k for k in expected_files.keys() if k.startswith("dir2")]
        # Delete keys at the source.
        for key in keys_to_delete:
            expected_files.pop(key)
            msc.delete(os.path.join(source_msc_url, key))

        # Sync from source to target and expect deletes to happen at the target.
        msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Delete all remaining keys at source and verify the deletes propagate to target.
        for key in expected_files.keys():
            msc.delete(os.path.join(source_msc_url, key))
        msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)
        verify_sync_and_contents(target_url=target_msc_url, expected_files={})


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_from(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{local_profile}/folder"
        target_msc_url = f"msc://{obj_profile}/synced-files"

        # Create local dataset
        expected_files = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.txt": "b" * 200,
            "dir1/file2.txt": "c" * 1000,
            "dir2/file0.txt": "d" * 1,
            "dir2/file1.txt": "e" * 5,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 10000,
            "dir3/file1.txt": "h" * 800,
            "dir3/file2.txt": "i" * 512,
        }
        create_local_test_dataset(source_msc_url, expected_files)
        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        # The leading "/" is implied, but a rendundant one should be handled okay.
        target_client.sync_from(source_client, "/folder/", "/synced-files/")

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)
