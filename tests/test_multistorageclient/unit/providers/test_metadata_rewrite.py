# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import pytest
import tempfile
from typing import Dict, Optional, Iterator, Type, List, Tuple
import uuid

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import MetadataProvider, ObjectMetadata

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore


class UuidMetadataProvider(MetadataProvider):
    def __init__(self):
        # Remap the paths to random uuid filenames
        self._path_to_uuid: Dict[str, str] = {}
        self._uuid_to_info: Dict[str, ObjectMetadata] = {}
        self._pending_adds: Dict[str, ObjectMetadata] = {}
        self._pending_deletes: List[str] = []

    def list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        assert not include_directories
        sorted_paths = sorted(self._path_to_uuid.keys())
        for path in sorted_paths:
            if start_after is not None and path < start_after:
                continue
            if end_at is not None and path > end_at:
                return

            u = self._path_to_uuid[path]
            if path.startswith(prefix):
                yield self._uuid_to_info[u]

    def get_object_metadata(self, path: str) -> ObjectMetadata:
        u = self._path_to_uuid.get(path)
        if u is None:
            raise FileNotFoundError(f"Object {path} does not exist.")
        return self._uuid_to_info[u]

    def glob(self, pattern: str) -> List[str]:
        return [path for path in self._path_to_uuid.keys() if fnmatch.fnmatch(path, pattern)]

    def realpath(self, path: str) -> Tuple[str, bool]:
        u = self._path_to_uuid.get(path)
        if u is None:
            return str(uuid.uuid4()), False
        return u, True

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        # Keep a dictionary of pending adds
        if path in self._pending_adds:
            raise ValueError(f"Object {path} already exists")
        self._pending_adds[path] = metadata

    def remove_file(self, path: str) -> None:
        if path not in self._path_to_uuid:
            raise ValueError(f"Object {path} does not exist")
        self._pending_deletes.append(path)

    def commit_updates(self) -> None:
        # Move entries in pending_adds to path_to_uuid and remove entries in pending_deletes from path_to_uuid
        for path in self._pending_deletes:
            u = self._path_to_uuid.pop(path)
            del self._uuid_to_info[u]
        for vpath, metadata in self._pending_adds.items():
            u = metadata.key
            metadata.key = vpath
            self._path_to_uuid[vpath] = u
            self._uuid_to_info[u] = metadata
        self._pending_adds.clear()
        self._pending_deletes.clear()

    def is_writable(self) -> bool:
        return True


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory], [tempdatastore.TemporaryAWSS3Bucket]],
)
def test_uuid_metadata_provider(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        data_with_uuid_profile = "uuid"

        data_profile_config_dict = temp_data_store.profile_config_dict()

        storage_client_config_dict = {
            "profiles": {
                data_with_uuid_profile: data_profile_config_dict,
            }
        }

        # Create the storage clients
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_with_uuid_profile)
        )
        storage_client._metadata_provider = UuidMetadataProvider()

        # Dictionary of filepaths to content
        content_dict = {
            "file1.txt": b"hello world",
            "dir1/file2.txt": b"hello world",
            "dir2/file3.txt": b"different content",
            "dir1/file4.txt": b"hello world",
            "dir2/file5.txt": b"different content",
            "dir2/file6.txt": b"different content",
            "dir2/dir3/file7.txt": b"different content",
            "dir2/dir3/file8.txt": b"different content",
            "dir2/dir3/file9.txt": b"different content",
        }
        for path, content in content_dict.items():
            storage_client.write(path, content)
        storage_client.commit_updates()

        assert set([f.key for f in storage_client.list(prefix="")]) == set(content_dict.keys())

        # Verify content
        for path, content in content_dict.items():
            metadata = storage_client.info(path)
            assert metadata.key == path
            assert metadata.content_length == len(content)
            assert metadata.last_modified is not None
            assert metadata.type == "file"

            print(f"Verifying {path}")
            assert storage_client.read(path) == content

            with tempfile.NamedTemporaryFile() as temp_file:
                storage_client.download_file(path, temp_file.name)
                with open(temp_file.name, "rb") as f:
                    assert f.read() == content

            # Test is_file
            assert storage_client.is_file(path)

            print(f"Verifying {path} with open()")
            with storage_client.open(path, "rb") as f:
                assert f.read() == content

        assert storage_client.is_empty("not_a_directory")
        assert not storage_client.is_empty("dir1/")

        # Test glob with *.txt returns all files with .txt extension
        assert sorted(storage_client.glob("*.txt")) == sorted(
            [path for path in content_dict.keys() if path.endswith(".txt")]
        )

        # Test copy and upload_file APIs.
        storage_client.copy("file1.txt", "file1_copy.txt")
        assert not storage_client.is_file("file1_copy.txt")

        upload_filename = "uploaded_file.txt"
        upload_content = "This is a fixed content file."
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as temp_file:
            temp_file.write(upload_content)
            temp_file.flush()
            storage_client.upload_file(upload_filename, temp_file.name)

        storage_client.commit_updates()

        assert storage_client.is_file("file1_copy.txt")
        assert storage_client.is_file(upload_filename)

        content_dict["file1_copy.txt"] = content_dict["file1.txt"]
        content_dict[upload_filename] = upload_content.encode("utf-8")

        for path, content in content_dict.items():
            print(f"Verifying {path}")
            metadata = storage_client.info(path)
            assert metadata.key == path
            assert metadata.content_length == len(content)

            assert storage_client.read(path) == content

        # Test delete API
        storage_client.delete("file1_copy.txt")
        storage_client.commit_updates()

        assert not storage_client.is_file("file1_copy.txt")
