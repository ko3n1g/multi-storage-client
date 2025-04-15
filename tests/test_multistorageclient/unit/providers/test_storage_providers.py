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

import functools
import os
import pytest
import tempfile
import uuid

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from typing import Type

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient.types import PreconditionFailedError


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.parametrize(argnames=["with_cache"], argvalues=[[True], [False]])
def test_storage_providers(temp_data_store_type: Type[tempdatastore.TemporaryDataStore], with_cache: bool):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        if with_cache:
            config_dict["cache"] = {"size_mb": 5000}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        file_extension = ".txt"
        # add a random string to the file path below so concurrent tests don't conflict
        file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"suffix{file_extension}"]
        file_path = os.path.join(*file_path_fragments)
        file_body_bytes = b"\x00"
        file_body_string = file_body_bytes.decode()

        # Write a file.
        storage_client.write(path=file_path, body=file_body_bytes)

        # Check the file contents.
        assert storage_client.read(path=file_path) == file_body_bytes

        # Check the file metadata.
        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == len(file_body_bytes)
        assert file_info.type == "file"
        assert file_info.last_modified is not None
        for lead in ["", "/"]:
            assert storage_client.is_file(path=f"{lead}{file_path}")
            assert not storage_client.is_file(path=lead)
            assert not storage_client.is_file(path=f"{lead}{file_path_fragments[0]}-nonexistent")
            assert not storage_client.is_file(path=f"{lead}{file_path_fragments[0]}")

        assert len(list(storage_client.list(prefix=file_path_fragments[0]))) == 1
        file_info_list = list(storage_client.list(prefix=os.path.join(*file_path_fragments[:2])))
        assert len(file_info_list) == 1
        listed_file_info = file_info_list[0]
        assert listed_file_info is not None
        assert listed_file_info.key.endswith(file_path)
        assert listed_file_info.content_length == file_info.content_length
        assert listed_file_info.type == file_info.type
        # There's some timestamp precision differences. Truncate to second.
        assert listed_file_info.last_modified.replace(microsecond=0) == file_info.last_modified.replace(microsecond=0)

        # Glob the file.
        assert len(storage_client.glob(pattern=f"*{file_extension}-nonexistent")) == 0
        assert len(storage_client.glob(pattern=os.path.join("**", f"*{file_extension}-nonexistent"))) == 0
        assert len(storage_client.glob(pattern=os.path.join("**", f"*{file_extension}"))) == 1
        assert storage_client.glob(pattern=os.path.join("**", f"*{file_extension}"))[0] == file_path

        # Check the infix directory metadata.
        for tail in ["", "/"]:
            directory_path = os.path.join(*file_path_fragments[:2])
            directory_info = storage_client.info(path=f"{directory_path}{tail}")
            assert directory_info is not None
            assert directory_info.key.endswith(f"{directory_path}/")
            assert directory_info.type == "directory"

        # List the infix directory.
        assert len(list(storage_client.list(prefix=f"{file_path_fragments[0]}/", include_directories=True))) == 1

        # Delete the file.
        storage_client.delete(path=file_path)

        # Upload + download the file.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_body_bytes)
            temp_file.close()
            storage_client.upload_file(remote_path=file_path, local_path=temp_file.name)
        assert storage_client.is_file(path=file_path)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            storage_client.download_file(remote_path=file_path, local_path=temp_file.name)
            assert os.path.getsize(temp_file.name) == len(file_body_bytes)

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for writes + reads (bytes).
        with storage_client.open(path=file_path, mode="wb") as file:
            file.write(file_body_bytes)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="rb") as file:
            assert file.read() == file_body_bytes

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for writes + reads (string).
        with storage_client.open(path=file_path, mode="w") as file:
            file.write(file_body_string)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="r") as file:
            assert file.read() == file_body_string

        # Copy the file.
        file_copy_path_fragments = ["copy", *file_path_fragments]
        file_copy_path = os.path.join(*file_copy_path_fragments)
        storage_client.copy(src_path=file_path, dest_path=file_copy_path)
        assert storage_client.read(path=file_copy_path) == file_body_bytes

        # Delete the file and its copy.
        for path in [file_path, file_copy_path]:
            storage_client.delete(path=path)
        assert len(list(storage_client.list(prefix=file_path_fragments[0]))) == 0
        assert len(list(storage_client.list(prefix=file_copy_path_fragments[0]))) == 0

        # Open the file for appends (bytes).
        with storage_client.open(path=file_path, mode="ab") as file:
            file.write(file_body_bytes)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="rb") as file:
            assert file.read() == file_body_bytes

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for appends (string).
        with storage_client.open(path=file_path, mode="a") as file:
            file.write(file_body_string)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="r") as file:
            assert file.read() == file_body_string

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for writes + reads (bytes).
        if storage_client._storage_provider._provider_name == "gcs":
            # GCS simulator does not support multipart uploads
            large_file_body_bytes = b"\x00" * MEMORY_LOAD_LIMIT
        else:
            large_file_body_bytes = b"\x00" * (MEMORY_LOAD_LIMIT + 1)
        with storage_client.open(path=file_path, mode="wb") as file:
            file.write(large_file_body_bytes)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="rb") as file:
            content = b""
            for chunk in iter(functools.partial(file.read, (MEMORY_LOAD_LIMIT // 2)), b""):
                content += chunk
            assert len(content) == len(large_file_body_bytes)

        # Delete the file.
        storage_client.delete(path=file_path)

        # Write files.
        file_numbers = range(1, 3)
        for i in file_numbers:
            storage_client.write(path=f"{i}{file_extension}", body=file_body_bytes)

        # List the files (paginated).
        for i in file_numbers:
            files = list(
                storage_client.list(prefix="", start_after=f"{i - 1}{file_extension}", end_at=f"{i}{file_extension}")
            )
            assert len(files) == 1
            assert files[0].key.endswith(f"{i}{file_extension}")

        # Delete the files.
        for i in file_numbers:
            storage_client.delete(path=f"{i}{file_extension}")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.parametrize(argnames=["with_cache"], argvalues=[[True], [False]])
def test_storage_providers_list_directories(
    temp_data_store_type: Type[tempdatastore.TemporaryDataStore], with_cache: bool
):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        # Create empty directories
        storage_client.write(path="dir1/", body=b"")
        assert storage_client.info(path="dir1").type == "directory"
        assert storage_client.info(path="dir1").content_length == 0

        # List directories
        directories = list(storage_client.list(prefix="", include_directories=True))
        assert len(directories) == 1
        assert directories[0].key == "dir1"
        assert directories[0].type == "directory"

        directories = list(storage_client.list(prefix="", include_directories=False))
        assert len(directories) == 0


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_put_object_with_etag_metadata(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))
        storage_provider = storage_client._storage_provider

        # Test file details
        bucket = config_dict["profiles"][profile]["storage_provider"]["options"]["base_path"]
        key = "test_etag.txt"  # Use just the key part
        file_path = f"{bucket}/{key}"
        file_body = b"test content"
        test_etag = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 hash of empty string

        # Write file with metadata containing etag
        metadata = {"etag": test_etag}
        storage_provider._put_object(path=file_path, body=file_body, metadata=metadata)

        # Verify file exists and content is correct
        assert storage_provider._get_object(path=file_path) == file_body

        # Get file metadata and verify etag
        file_info = storage_provider._get_object_metadata(path=file_path)
        assert file_info is not None
        # Skip metadata verification for POSIX if extended attributes are not supported
        if storage_provider._provider_name != "file" or hasattr(os, "setxattr"):
            assert file_info.metadata["etag"] == test_etag

        # Clean up
        storage_provider._delete_object(path=file_path)
        with pytest.raises(FileNotFoundError):
            storage_provider._get_object(path=file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_delete_object_with_etag(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))
        storage_provider = storage_client._storage_provider

        # Test file details
        bucket = config_dict["profiles"][profile]["storage_provider"]["options"]["base_path"]
        key = "test_delete_etag.txt"
        file_path = f"{bucket}/{key}"
        file_body = b"test content"

        # Write file first to get its actual ETag
        storage_provider._put_object(path=file_path, body=file_body)
        file_info = storage_provider._get_object_metadata(path=file_path)
        actual_etag = file_info.etag

        # Test successful deletion with matching etag
        storage_provider._delete_object(path=file_path, if_match=actual_etag)
        with pytest.raises(FileNotFoundError):
            storage_provider._get_object(path=file_path)

        # Write file again with different etag
        storage_provider._put_object(path=file_path, body=file_body)
        file_info = storage_provider._get_object_metadata(path=file_path)
        actual_etag = file_info.etag

        # Test deletion with mismatched etag
        mismatched_etag = "different_etag_value"
        if storage_provider._provider_name == "gcs":
            # Skip mismatched ETag test for GCS since fake-gcs-server doesn't support precondition checks
            pass
        elif storage_provider._provider_name == "azure":
            # Azure raises PreconditionFailedError with 412 status code
            with pytest.raises(PreconditionFailedError, match="412"):
                storage_provider._delete_object(path=file_path, if_match=mismatched_etag)
            assert storage_provider._get_object(path=file_path) == file_body
        else:  # S3 and SwiftStack (both use s3.py)
            # skip mismatched etag test for S3 and SwiftStack, since MinIO server doesn't support precondition deletes with etags
            pass

        # Test unconditional deletion (no etag provided)
        storage_provider._delete_object(path=file_path)
        with pytest.raises(FileNotFoundError):
            storage_provider._get_object(path=file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_posix_xattr_metadata(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))
        storage_provider = storage_client._storage_provider

        # Test file details
        bucket = config_dict["profiles"][profile]["storage_provider"]["options"]["base_path"]
        key = "test_xattr.txt"
        file_path = f"{bucket}/{key}"
        file_body = b"test content"
        test_metadata = {
            "etag": "d41d8cd98f00b204e9800998ecf8427e",
            "content-type": "text/plain",
            "custom-key": "custom-value",
        }

        # Write file with metadata
        storage_provider._put_object(path=file_path, body=file_body, metadata=test_metadata)

        # Verify file exists and content is correct
        assert storage_provider._get_object(path=file_path) == file_body

        # Get file metadata
        file_info = storage_provider._get_object_metadata(path=file_path)
        assert file_info is not None

        # Check if xattrs are supported on this system
        xattrs_supported = hasattr(os, "setxattr") and hasattr(os, "getxattr")
        if xattrs_supported:
            # Verify all metadata was stored correctly
            assert file_info.metadata is not None
            for key, value in test_metadata.items():
                assert file_info.metadata[key] == value
        else:
            # If xattrs are not supported, metadata should be None
            assert file_info.metadata is None

        # Clean up
        storage_provider._delete_object(path=file_path)
        with pytest.raises(FileNotFoundError):
            storage_provider._get_object(path=file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
def test_put_object_with_conditional_params(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    """
    Test put_object with if_match and if_none_match parameters.
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))
        storage_provider = storage_client._storage_provider

        # Test file details
        bucket = config_dict["profiles"][profile]["storage_provider"]["options"]["base_path"]
        key = "test_conditional.txt"
        file_path = f"{bucket}/{key}"
        file_body = b"test content"
        updated_body = b"updated content"

        # Test if_none_match="*" - should succeed if object doesn't exist
        if storage_provider._provider_name in ["s3", "swiftstack"]:
            # For S3, SwiftStack, and OCI, test if_none_match="*"
            storage_provider._put_object(path=file_path, body=file_body, if_none_match="*")
            assert storage_provider._get_object(path=file_path) == file_body

            # Test if_none_match="*" - should fail if object exists
            with pytest.raises(PreconditionFailedError):
                storage_provider._put_object(path=file_path, body=updated_body, if_none_match="*")
            assert storage_provider._get_object(path=file_path) == file_body
        else:
            # For providers that don't support if_none_match="*", just create the object
            storage_provider._put_object(path=file_path, body=file_body)

        # Get the actual etag for the object
        metadata = storage_provider._get_object_metadata(path=file_path)
        assert metadata.etag is not None

        # Test if_match with matching etag - should succeed
        storage_provider._put_object(path=file_path, body=updated_body, if_match=metadata.etag)
        assert storage_provider._get_object(path=file_path) == updated_body

        # Test if_match with incorrect etag
        mismatched_etag = "different_etag_value"

        # testing string to int conversion for gcs, this should fail because gcs expects a numeric generation number
        if storage_provider._provider_name == "gcs":
            # GCS requires numeric generation numbers for etags
            with pytest.raises(RuntimeError, match="Failed to PUT object"):
                storage_provider._put_object(path=file_path, body=file_body, if_match=mismatched_etag)
            assert storage_provider._get_object(path=file_path) == updated_body

        # Test if_match with incorrect etag, gcs will convert this to a numeric generation number, others will just
        # treat it as a string
        mismatched_etag = "1234567890"
        with pytest.raises(PreconditionFailedError, match="412"):
            storage_provider._put_object(path=file_path, body=file_body, if_match=mismatched_etag)
        assert storage_provider._get_object(path=file_path) == updated_body
