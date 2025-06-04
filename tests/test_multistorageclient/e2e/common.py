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

import os
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable
from typing import TypeVar

import pytest

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL, SourceVersionCheckMode

T = TypeVar("T")
MB = 1024 * 1024


def wait(
    waitable: Callable[[], T],
    should_wait: Callable[[T], bool],
    max_attempts: int = 3,
    attempt_interval_seconds: int = 1,
) -> T:
    """
    Wait for the return value of a function ``waitable`` to satisfy a wait condition.

    Defaults to 60 attempts at 1 second intervals.

    For handling storage services with eventually consistent operations.
    """
    assert max_attempts >= 1
    assert attempt_interval_seconds >= 0

    for attempt in range(max_attempts):
        value = waitable()
        if should_wait(value) and attempt < max_attempts - 1 and attempt_interval_seconds > 0:
            time.sleep(attempt_interval_seconds)
        else:
            return value

    raise AssertionError(f"Waitable didn't return a desired value within {max_attempts} attempt(s)!")


def len_should_wait(expected_len: int) -> Callable[[Iterable], bool]:
    """
    Returns a wait condition on the length of an iterable return value.

    For list and glob operations.
    """
    return lambda value: len(list(value)) != expected_len


def delete_files(storage_client: msc.StorageClient, prefix: str) -> None:
    for object in storage_client.list(prefix=prefix):
        storage_client.delete(object.key)


def verify_shortcuts(profile: str, prefix: str) -> None:
    body = b"A" * (4 * MB)

    object_count = 5
    for i in range(object_count):
        with msc.open(f"msc://{profile}/{prefix}/data-{i}.bin", "wb") as fp:
            fp.write(body)

    results = wait(
        waitable=lambda: msc.glob(f"msc://{profile}/{prefix}/**/*.bin"),
        should_wait=len_should_wait(expected_len=object_count),
    )

    for res in results:
        with msc.open(res, "rb") as fp:
            assert fp.read(10) == b"A" * 10


def verify_storage_provider(storage_client: msc.StorageClient, prefix: str) -> None:
    body = b"A" * (16 * MB)
    text = '{"text":"✅ Unicode Test ✅"}'

    # write file
    filename = f"{prefix}/testfile.bin"
    storage_client.write(filename, body)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=1))

    # is file
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(prefix)
    assert not storage_client.is_file("not-exist-prefix")

    # glob
    assert len(storage_client.glob("*.py")) == 0
    assert storage_client.glob(f"{prefix}/*.bin")[0] == filename
    assert len(storage_client.glob(f"{prefix}/*.bin")) == 1

    # verify file is written
    assert storage_client.read(filename) == body
    info = storage_client.info(filename)
    assert info is not None
    assert info.key.endswith(filename)
    assert info.content_length == len(body)
    assert info.type == "file"
    assert info.last_modified is not None

    info_list = list(storage_client.list(filename))
    assert len(info_list) == 1
    listed_info = info_list[0]
    assert listed_info is not None
    assert listed_info.key.endswith(filename)
    assert listed_info.content_length == info.content_length
    assert listed_info.type == info.type
    # There's some timestamp precision differences. Truncate to second.
    assert listed_info.last_modified.replace(microsecond=0) == info.last_modified.replace(microsecond=0)

    # upload
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(body)
    temp_file.seek(0)
    temp_file.flush()
    storage_client.upload_file(filename, temp_file.name)
    os.unlink(temp_file.name)

    # download
    # Create a tmpdir base dir but not the full path to test if storage provider creates the path
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir_name = tmpdir  # Get the filename
        temp_file_path = os.path.join(temp_dir_name, "downloads/data", "downloaded.bin")
        storage_client.download_file(filename, temp_file_path)
        assert os.path.getsize(temp_file_path) == len(body)

    # open file
    with storage_client.open(filename, "wb") as fp:
        fp.write(body)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=1))

    with storage_client.open(filename, "rb") as fp:
        content = fp.read()
        assert content == body
        assert isinstance(content, bytes)

    # delete file
    storage_client.delete(filename)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=0))

    # large file
    body_large = b"*" * (32 * MB)
    with storage_client.open(filename, "wb") as fp:
        fp.write(body_large)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=1))

    with storage_client.open(filename, "rb") as fp:
        read_size = 4 * MB
        content = fp.read(read_size)
        assert len(content) == read_size

        content += fp.read(read_size)
        assert len(content) == 2 * read_size

        content += fp.read(read_size)
        content += fp.read()
        assert len(content) == len(body_large)
        assert isinstance(content, bytes)

    # delete file
    storage_client.delete(filename)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=0))

    # unicode file
    filename = f"{prefix}/testfile.txt"
    with storage_client.open(filename, "w") as fp:
        fp.write(text)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=1))

    with storage_client.open(filename, "r") as fp:
        content = fp.read()
        assert content == text
        assert isinstance(content, str)

    # delete file
    storage_client.delete(filename)

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=0))

    # test directories
    storage_client.write(f"{prefix}/dir1/dir2/", b"")
    assert storage_client.info(path=f"{prefix}/dir1/dir2").type == "directory"
    assert storage_client.info(path=f"{prefix}/dir1/dir2").content_length == 0

    directories = list(storage_client.list(prefix=f"{prefix}/dir1/", include_directories=True))
    assert len(directories) == 1
    assert directories[0].key == f"{prefix}/dir1/dir2"
    assert directories[0].type == "directory"

    directories = list(storage_client.list(prefix=f"{prefix}/dir1/", include_directories=False))
    assert len(directories) == 0

    # delete file
    storage_client.delete(f"{prefix}/dir1/dir2/")

    wait(waitable=lambda: storage_client.list(prefix), should_wait=len_should_wait(expected_len=1))


def test_shortcuts(profile: str):
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    prefix = f"files-{uuid.uuid4()}"
    try:
        verify_shortcuts(profile, prefix)
    finally:
        delete_files(client, prefix)


def test_storage_client(profile: str):
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    prefix = f"files-{uuid.uuid4()}"
    try:
        verify_storage_provider(client, prefix)
    finally:
        delete_files(client, prefix)


def test_conditional_put(
    storage_provider,
    if_none_match_error_type,
    if_match_error_type,
    if_none_match_specific_error_type=None,
    supports_if_none_match_star=True,
):
    """Test conditional PUT operations using if-match and if-none-match conditions.

    Args:
        storage_provider: The storage provider to test
        if_none_match_error_type: The error type expected when if_none_match="*" fails
        if_match_error_type: The error type expected when if_match fails
        if_none_match_specific_error_type: The error type expected when if_none_match with specific etag fails
        supports_if_none_match_star: Whether the provider supports if_none_match="*" condition
    """
    key = f"test-conditional-put-{uuid.uuid4()}"
    data = b"test data"

    try:
        # First test if_none_match="*" - this should either succeed or raise NotImplementedError
        if supports_if_none_match_star:
            # For providers that support if_none_match="*", try to create the object
            storage_provider.put_object(key, data, if_none_match="*")

            # Now test if_none_match="*" on existing object - should fail
            with pytest.raises(if_none_match_error_type):
                storage_provider.put_object(key, data, if_none_match="*")
        else:
            # For providers that don't support if_none_match="*", it should raise NotImplementedError
            with pytest.raises(if_none_match_error_type):
                storage_provider.put_object(key, data, if_none_match="*")

            # Create the object unconditionally for subsequent tests
            storage_provider.put_object(key, data)

        # Get the etag of the existing object
        metadata = storage_provider.get_object_metadata(key)
        etag = metadata.etag

        # Test if_match with correct etag
        storage_provider.put_object(key, data, if_match=etag)

        # Test if_match with wrong etag
        with pytest.raises(if_match_error_type):
            storage_provider.put_object(key, data, if_match="1234567890")

        # Test if_none_match with specific etag if supported, runs only for gcs and azure
        if if_none_match_specific_error_type is not None:
            # Use a new key for this test case
            key = f"test-conditional-put-specific-{uuid.uuid4()}"
            # First put to get generation number
            storage_provider.put_object(key, data)
            metadata = storage_provider.get_object_metadata(key)
            etag = metadata.etag
            # Put with same generation should fail
            with pytest.raises(if_none_match_specific_error_type):
                storage_provider.put_object(key, b"new data", if_none_match=etag)

    finally:
        # Clean up
        try:
            storage_provider.delete_object(key)
        except Exception:
            pass


def test_open_with_source_version_check(profile: str):
    """Test the open method with different source version check modes using an actual S3 profile."""
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    storage_provider = client._storage_provider
    key = f"test-source-version-check-{uuid.uuid4()}"
    content1 = b"test content for cache"
    content2 = b"modified content for cache"

    try:
        # Write initial content
        storage_provider.put_object(key, content1)
        initial_etag = storage_provider.get_object_metadata(key).etag
        if initial_etag is None:
            pytest.skip("Backend does not support etag; skipping cache/versioning test.")

        # Read with ENABLE mode - should get updated content
        with msc.open(f"{MSC_PROTOCOL}{profile}/{key}", "rb", check_source_version=SourceVersionCheckMode.ENABLE) as f:
            assert f.read() == content1

        # Modify file content
        storage_provider.put_object(key, content2)
        new_etag = storage_provider.get_object_metadata(key).etag
        assert new_etag != initial_etag, "etag should change after file modification"

        # Read with DISABLE mode - should get old content from cache
        with msc.open(f"{MSC_PROTOCOL}{profile}/{key}", "rb", check_source_version=SourceVersionCheckMode.DISABLE) as f:
            assert f.read() == content1  # Should read from cache, not see the modification

        # Read with ENABLE mode - should get new content
        with msc.open(f"{MSC_PROTOCOL}{profile}/{key}", "rb", check_source_version=SourceVersionCheckMode.ENABLE) as f:
            assert f.read() == content2  # Should update cache

    finally:
        # Clean up
        try:
            storage_provider.delete_object(key)
        except Exception:
            pass
