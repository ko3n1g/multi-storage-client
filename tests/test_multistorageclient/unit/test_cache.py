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
import shutil
import time
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient.cache import DEFAULT_CACHE_REFRESH_INTERVAL, CacheManager
from multistorageclient.caching.cache_config import (
    CacheConfig,
    EvictionPolicyConfig,
)
from multistorageclient.config import StorageClientConfig


@pytest.fixture
def profile_name():
    return "test-cache"


@pytest.fixture
def cache_config(tmpdir):
    """Fixture for CacheConfig object."""
    return CacheConfig(size="10M", use_etag=False, location=str(tmpdir))


@pytest.fixture
def cache_config_with_etag(tmpdir):
    """Fixture for CacheConfig object with etag support enabled."""
    return CacheConfig(size="10M", use_etag=True, location=str(tmpdir))


@pytest.fixture
def cache_manager(profile_name, cache_config):
    """Fixture for CacheManager object."""
    return CacheManager(profile=profile_name, cache_config=cache_config)


@pytest.fixture
def cache_manager_with_etag(profile_name, cache_config_with_etag):
    """Fixture for CacheManager object with etag support enabled."""
    return CacheManager(profile=profile_name, cache_config=cache_config_with_etag)


def test_cache_config_size_bytes(cache_config):
    """Test that CacheConfig size_bytes converts MB to bytes correctly."""
    assert cache_config.size_bytes() == 10 * 1024 * 1024  # 10 MB


def test_cache_manager_read_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    cache_manager.set("bucket/test_file.txt", str(file))
    assert cache_manager.read("bucket/test_file.txt") == b"cached data"

    cache_manager.set("bucket/test_file.bin", b"binary data")
    assert cache_manager.read("bucket/test_file.bin") == b"binary data"


def test_cache_manager_preserves_directory_structure(profile_name, tmpdir, cache_manager):
    """Test that CacheManager preserves directory structure in the cache."""
    # Create test files in different directories with more diverse paths

    test_uuid = str(uuid.uuid4())

    # Generate unique file namestest
    files = {
        "folder1/file1.txt": "data1",
        "folder1/subfolder/file2.txt": "data2",
        "folder2/file3.txt": "data3",
        "folder3/folder4/file4.txt": "data4",
        "folder3/folder4/subfolder/file5.txt": "data5",
        "folder3/folder4/subfolder/deep/file6.txt": "data6",
        "root_file.txt": "data7",
        "folder4/empty_folder/file7.txt": "data8",
    }

    # Store files directly in cache
    for path, content in files.items():
        cache_manager.set(f"bucket/{test_uuid}/{path}", content.encode())

    # Verify each file exists in cache with correct directory structure and content
    for path, content in files.items():
        # Read from cache
        cached_data = cache_manager.read(f"bucket/{test_uuid}/{path}")
        assert cached_data == content.encode(), f"Content mismatch for {path}"

        # Verify file exists in cache
        cache_path = os.path.join(tmpdir, profile_name, f"bucket/{test_uuid}/{path}")
        assert os.path.exists(cache_path), f"File not found in cache: {path}"

    # Get all directories in the cache
    cache_root = os.path.join(tmpdir, profile_name)
    all_dirs = set()
    for root, dirs, _ in os.walk(cache_root):
        for dir_name in dirs:
            # Skip lock files
            if not dir_name.startswith("."):
                rel_path = os.path.relpath(os.path.join(root, dir_name), cache_root)
                all_dirs.add(rel_path)

    # Expected directory structure
    expected_dirs = {
        "bucket",
        os.path.join("bucket", test_uuid),
        os.path.join(f"bucket/{test_uuid}", "folder1"),
        os.path.join(f"bucket/{test_uuid}", "folder1", "subfolder"),
        os.path.join(f"bucket/{test_uuid}", "folder2"),
        os.path.join(f"bucket/{test_uuid}", "folder3"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4", "subfolder"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4", "subfolder", "deep"),
        os.path.join(f"bucket/{test_uuid}", "folder4"),
        os.path.join(f"bucket/{test_uuid}", "folder4", "empty_folder"),
    }

    assert all_dirs == expected_dirs, (
        f"Unexpected directories found in cache. Got: {all_dirs}, Expected: {expected_dirs}"
    )

    # Verify that all files are accessible through the cache manager
    for path in files.keys():
        assert cache_manager.contains(f"bucket/{test_uuid}/{path}"), f"Cache manager should contain {path}"


def test_cache_manager_read_file_with_etag(profile_name, tmpdir, cache_manager_with_etag):
    """Test that CacheManager can read a file from the cache with etag in the key."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    # Test with etag in the key
    key = f"bucket/{test_uuid}/test_file.txt"
    source_version = "etag123"
    cache_manager_with_etag.set(key, str(file), source_version=source_version)
    assert cache_manager_with_etag.read(key, source_version=source_version) == b"cached data"

    # Test with binary data and etag
    key_bin = f"bucket/{test_uuid}/test_file.bin"
    source_version = "etag456"
    cache_manager_with_etag.set(key_bin, b"binary data", source_version=source_version)
    assert cache_manager_with_etag.read(key_bin, source_version=source_version) == b"binary data"

    # Verify that the file is stored with the etag in the path
    expected_path = os.path.join(tmpdir, profile_name, key)
    assert os.path.exists(expected_path), f"File should exist at {expected_path}"

    # Test that reading without etag returns None
    key_without_etag = f"bucket/{test_uuid}/test_file.txt"
    assert cache_manager_with_etag.read(key_without_etag) is None


def test_cache_manager_read_delete_file_with_etag(profile_name, tmpdir, cache_manager_with_etag):
    """Test that CacheManager can read and delete a file from the cache with etag in the key."""

    test_uuid = str(uuid.uuid4())
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    key = f"bucket/{test_uuid}/test_file.txt"
    source_version = "etag123"

    with cache_manager_with_etag.acquire_lock(key):
        cache_manager_with_etag.set(key, str(file), source_version=source_version)

    # Verify the lock file is in the same directory as the file
    lock_path = os.path.join(tmpdir, profile_name, os.path.dirname(key), f".{os.path.basename(key)}.lock")
    assert os.path.exists(lock_path)

    # Verify we can read the file
    assert cache_manager_with_etag.read(key, source_version=source_version) == b"cached data"

    # Delete the file
    cache_manager_with_etag.delete(key)

    # Verify the file and its lock are deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, key))
    assert not os.path.exists(lock_path)

    # Test that reading after delete returns None
    assert cache_manager_with_etag.read(key, source_version=source_version) is None


def test_cache_manager_read_delete_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    key = f"bucket/{test_uuid}/test_file.txt"

    with cache_manager.acquire_lock(key):
        cache_manager.set(key, str(file))

    # Verify the lock file is in the same directory
    lock_path = os.path.join(tmpdir, profile_name, os.path.dirname(key), f".{os.path.basename(key)}.lock")
    assert os.path.exists(lock_path)

    assert cache_manager.read(key) == b"cached data"

    cache_manager.delete(key)

    # Verify the file and its lock are deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, key))
    assert not os.path.exists(lock_path)


def test_cache_manager_open_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can open a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    key = f"bucket/{test_uuid}/test_file.txt"

    cache_manager.set(key, str(file))

    with cache_manager.open(key, "r") as result:
        assert result.read() == "cached data"
        assert result.name == os.path.join(tmpdir, profile_name, key)

    with cache_manager.open(key, "rb") as result:
        assert result.read() == b"cached data"
        assert result.name == os.path.join(tmpdir, profile_name, key)


def test_cache_manager_refresh_cache(tmpdir):
    """Test that cache refresh works correctly."""
    # Use a separate cache directory for this test
    cache_dir = os.path.join(str(tmpdir), "refresh_test")
    os.makedirs(cache_dir, exist_ok=True)

    cache_config = CacheConfig(size="10M", use_etag=False, location=cache_dir)
    cache_manager = CacheManager(profile="refresh_test", cache_config=cache_config)

    data_10mb = b"*" * 10 * 1024 * 1024
    for i in range(20):
        file_name = f"bucket/test_{i:04d}.bin"
        cache_manager.set(file_name, data_10mb)

    # Force refresh by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)

    cache_manager.refresh_cache()
    assert cache_manager.cache_size() <= 10 * 1024 * 1024

    # Clean up
    shutil.rmtree(cache_dir)


def test_cache_manager_metrics(profile_name, tmpdir, cache_manager):
    # Mock the metrics helper in the backend
    cache_manager._metrics_helper = MagicMock()

    test_uuid = str(uuid.uuid4())
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    cache_manager.set(f"bucket/{test_uuid}/test_file.txt", str(file))
    cache_manager._metrics_helper.increase.assert_called_with(operation="SET", success=True)

    cache_manager.read(f"bucket/{test_uuid}/test_file.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="READ", success=True)

    cache_manager.read(f"bucket/{test_uuid}/test_file_not_exist.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="READ", success=False)

    cache_manager.open(f"bucket/{test_uuid}/test_file.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="OPEN", success=True)

    cache_manager.open(f"bucket/{test_uuid}/test_file_not_exist.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="OPEN", success=False)


@pytest.fixture
def lru_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "lru_cache")
    return CacheConfig(
        size="10M", use_etag=False, location=cache_dir, eviction_policy=EvictionPolicyConfig(policy="LRU")
    )


def test_lru_eviction_policy(profile_name, lru_cache_config):
    # Create the CacheManager with the provided lru_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=lru_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)

    # Access file1 to make it the most recently used
    cache_manager.read(f"{test_uuid}/file1")  # force update ts
    time.sleep(1)

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    time.sleep(1)  # Ensure time difference for LRU
    # Record the current last_refresh_time and set it to past to force refresh
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    # Verify that refresh occurred by checking last_refresh_time was updated
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 is still in the cache (LRU policy)
    assert cache_manager.contains(f"{test_uuid}/file1"), "Most recently used file should be kept"

    # Verify that the least recently used file (file2 or file3) has been evicted
    assert not cache_manager.contains(f"{test_uuid}/file2") or not cache_manager.contains(f"{test_uuid}/file3"), (
        "Least recently used file should be evicted"
    )


@pytest.fixture
def fifo_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "fifo_cache")
    return CacheConfig(
        size="10M", use_etag=False, location=cache_dir, eviction_policy=EvictionPolicyConfig(policy="FIFO")
    )


def test_fifo_eviction_policy(profile_name, fifo_cache_config):
    # Create the CacheManager with the provided fifo_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=fifo_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB - First in
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB - Second in
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read(f"{test_uuid}/file3")  # Access the newest file
    cache_manager.read(f"{test_uuid}/file2")  # Access the middle file
    cache_manager.read(f"{test_uuid}/file1")  # Access the oldest file

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB - Fourth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 (first in) has been evicted
    assert not cache_manager.contains(f"{test_uuid}/file1"), "First file in should be evicted (FIFO)"

    # Verify that later files are still in the cache
    assert cache_manager.contains(f"{test_uuid}/file2"), "Second file in should be kept"
    assert cache_manager.contains(f"{test_uuid}/file3"), "Third file in should be kept"
    assert cache_manager.contains(f"{test_uuid}/file4"), "Newly added file should be in the cache"

    # Add one more file to verify FIFO continues to work
    cache_manager.set(f"{test_uuid}/file5", b"e" * 3 * 1024 * 1024)  # 3 MB - Fifth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file2 (now the oldest) is evicted
    assert not cache_manager.contains(f"{test_uuid}/file2"), "Second file in should now be evicted"
    assert cache_manager.contains(f"{test_uuid}/file3"), "Third file in should still be kept"
    assert cache_manager.contains(f"{test_uuid}/file4"), "Fourth file in should still be kept"
    assert cache_manager.contains(f"{test_uuid}/file5"), "Most recently added file should be in the cache"


@pytest.fixture
def random_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "random_cache")
    return CacheConfig(
        size="10M", use_etag=False, location=cache_dir, eviction_policy=EvictionPolicyConfig(policy="RANDOM")
    )


def test_random_eviction_policy(profile_name, random_cache_config):
    """Test the random eviction policy of the cache manager.

    This test verifies that the cache manager correctly implements random eviction when the cache is full.
    The test follows these steps:
    1. Creates a cache with a 10MB limit
    2. Adds three files of 3MB each (total 9MB)
    3. Adds a fourth file to trigger eviction
    4. Verifies that:
       - Exactly one file is evicted
       - Total cache size stays within limits

    The test ensures that:
    - The cache respects its size limit
    - Eviction occurs when needed
    - The random eviction policy works as expected
    - Cache operations maintain consistency

    :param profile_name: The name of the cache profile to use
    :param random_cache_config: Cache configuration with random eviction policy
    """
    # Clean the entire cache directory
    cache_dir = random_cache_config.location
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir)

    # Create the CacheManager with the provided random_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=random_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB

    # Verify initial state
    assert cache_manager.contains(f"{test_uuid}/file1")
    assert cache_manager.contains(f"{test_uuid}/file2")
    assert cache_manager.contains(f"{test_uuid}/file3")

    # Force a refresh to ensure cache state is up to date
    cache_manager.refresh_cache()

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    # Force refresh to trigger eviction by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # Verify that exactly one file was evicted (could be any of the files)
    all_files = [f"{test_uuid}/file1", f"{test_uuid}/file2", f"{test_uuid}/file3", f"{test_uuid}/file4"]
    remaining_files = sum(1 for f in all_files if cache_manager.contains(f))
    assert remaining_files == 3, "Exactly one file should be evicted"

    # Verify total cache size
    total_size = 0
    for f in all_files:
        if cache_manager.contains(f):
            data = cache_manager.read(f)
            if data is not None:  # Handle potential None return from read()
                total_size += len(data)
    assert total_size <= 10 * 1024 * 1024, "Total cache size should not exceed 10MB"


def verify_cache_operations(cache_manager):
    # Add files to the cache (each file is 3 MB)
    test_uuid = str(uuid.uuid4())
    key1 = f"{test_uuid}/test_file1"
    key2 = f"{test_uuid}/test_file2"
    key3 = f"{test_uuid}/test_file3"
    cache_manager.set(key1, b"a" * 1 * 1024 * 1024, source_version="etag1")  # 1 MB - First in
    cache_manager.set(key2, b"b" * 1 * 1024 * 1024, source_version="etag2")  # 1 MB - Second in
    cache_manager.set(key3, b"c" * 1 * 1024 * 1024, source_version="etag3")  # 1 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read(key3, source_version="etag3")  # Access the newest file
    cache_manager.read(key2, source_version="etag2")  # Access the middle file
    cache_manager.read(key1, source_version="etag1")  # Access the oldest file

    # Verify that later files are still in the cache with correct ETags
    assert cache_manager.contains(key1, source_version="etag1"), "Second file in should be kept"
    assert cache_manager.contains(key2, source_version="etag2"), "Third file in should be kept"
    assert cache_manager.contains(key3, source_version="etag3"), "Newly added file should be in the cache"


def create_legacy_cache_config(profile_config, tmpdir):
    """Helper function to create legacy cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {"size_mb": 10, "use_etag": False, "location": str(tmpdir), "eviction_policy": "fifo"},
    }


def create_new_cache_config(profile_config, tmpdir):
    """Helper function to create new cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {"size": "10M", "use_etag": False, "eviction_policy": {"policy": "random", "refresh_interval": 300}},
    }


def create_mixed_cache_config(profile_config, tmpdir):
    """Helper function to create mixed cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {"size_mb": 10, "use_etag": False, "eviction_policy": {"policy": "random", "refresh_interval": 300}},
    }


def create_incorrect_size_cache_config(profile_config, tmpdir):
    """Helper function to create incorrect size cache config."""
    return {"profiles": {"s3-local": profile_config}, "cache": {"size": "one-thousand-gigabytes"}}


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, create_legacy_cache_config],
        [tempdatastore.TemporaryAWSS3Bucket, create_new_cache_config],
    ],
    ids=["legacy_config", "new_config"],
)
def test_storage_provider_cache_configs(config_creator, temp_data_store_type, tmpdir):
    """Test that both legacy and new cache config formats work correctly."""
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)
        if config_creator == create_legacy_cache_config:
            with pytest.raises(RuntimeError, match="Failed to validate the config file"):
                StorageClientConfig.from_dict(config_dict)
        else:
            storage_config = StorageClientConfig.from_dict(config_dict)
            real_storage_provider = storage_config.storage_provider
            for obj in real_storage_provider.list_objects(prefix="test_"):
                real_storage_provider.delete_object(obj.key)
            cache_manager = storage_config.cache_manager
            verify_cache_operations(cache_manager)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator", "expected_error", "error_message"],
    argvalues=[
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_mixed_cache_config,
            ValueError,
            "The 'size_mb' property is no longer supported",
        ],
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_incorrect_size_cache_config,
            RuntimeError,
            "Failed to validate the config file",
        ],
    ],
    ids=["mixed_config", "incorrect_size"],
)
def test_storage_provider_invalid_cache_configs(
    config_creator, temp_data_store_type, expected_error, error_message, tmpdir
):
    """
    Test that invalid cache configurations raise appropriate errors.

    This test verifies that:
    1. Mixing old and new cache config formats raises a ValueError
    2. Using an incorrect size format raises a RuntimeError
    """
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)
        with pytest.raises(expected_error, match=error_message):
            StorageClientConfig.from_dict(config_dict)


@pytest.fixture
def storage_provider_partial_cache_config(tmpdir):
    """
    New cache config format
    """

    # Create a config dictionary with profile and cache configuration
    def _config_builder(profile_config):
        return {
            "profiles": {"s3-local": profile_config},
            "cache": {"size": "100M", "eviction_policy": {"policy": "fifo"}},
        }

    return _config_builder


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_storage_provider_partial_cache_config(storage_provider_partial_cache_config, temp_data_store_type):
    with temp_data_store_type() as temp_store:
        config_dict = storage_provider_partial_cache_config(temp_store.profile_config_dict())
        storage_config = StorageClientConfig.from_dict(config_dict)
        real_storage_provider = storage_config.storage_provider
        cache_manager = storage_config.cache_manager

        # Clean up any existing objects in the bucket with test prefix
        for obj in real_storage_provider.list_objects(prefix="test_"):
            real_storage_provider.delete_object(obj.key)

        # Access the CacheManager
        verify_cache_operations(cache_manager)

        cache_config = storage_config.cache_config
        assert cache_config is not None
        assert cache_config.size == "100M"
        assert cache_config.location is not None and isinstance(cache_config.location, str)
        assert cache_config.eviction_policy.policy == "fifo"
        assert cache_config.eviction_policy.refresh_interval == DEFAULT_CACHE_REFRESH_INTERVAL
        assert cache_config.use_etag
        assert isinstance(cache_manager, CacheManager)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "expected_error"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, None],  # S3 should work
        [tempdatastore.TemporarySwiftStackBucket, None],  # SwiftStack (S8K) should work
        [tempdatastore.TemporaryAzureBlobStorageContainer, ValueError],  # Azure should fail
        [tempdatastore.TemporaryGoogleCloudStorageBucket, ValueError],  # GCS should fail
    ],
    ids=["s3", "swiftstack", "azure", "gcs"],
)
@pytest.fixture
def no_eviction_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "no_eviction_cache")
    return CacheConfig(
        size="3M", use_etag=False, location=cache_dir, eviction_policy=EvictionPolicyConfig(policy="NO_EVICTION")
    )


def test_no_eviction_policy(profile_name, no_eviction_cache_config):
    """Test the NO_EVICTION eviction policy of the cache manager.

    This test verifies that when NO_EVICTION eviction policy is set:
    1. No files are evicted even when cache size limit is exceeded
    2. All files remain in cache regardless of size
    3. Cache refresh does not trigger eviction
    4. No eviction thread is created
    5. No lock file is created

    :param profile_name: The name of the cache profile to use
    :param no_eviction_cache_config: Cache configuration with NO_EVICTION eviction policy
    """
    # Clean the entire cache directory
    cache_dir = no_eviction_cache_config.location
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir)

    # Create the CacheManager with the provided no_eviction_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=no_eviction_cache_config)

    # Verify no eviction thread is created
    assert not hasattr(cache_manager, "_eviction_thread"), "No eviction thread should be created for NONE policy"
    assert not hasattr(cache_manager, "_eviction_thread_running"), (
        "No eviction thread running flag should exist for NO_EVICTION policy"
    )

    test_uuid = str(uuid.uuid4())
    # Add first 3 files to the cache (each file is 1 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file2", b"b" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file3", b"c" * 1 * 1024 * 1024)  # 1 MB

    # Verify first 3 files are in cache
    for i in range(1, 4):
        assert cache_manager.contains(f"{test_uuid}/file{i}"), f"File {i} should be in cache"

    # Add 2 more files to exceed cache size limit
    cache_manager.set(f"{test_uuid}/file4", b"d" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file5", b"e" * 1 * 1024 * 1024)  # 1 MB

    # Verify no lock file is created
    lock_file_path = os.path.join(cache_dir, ".cache_refresh.lock")
    assert not os.path.exists(lock_file_path), "No lock file should be created for NONE policy"

    # Force refresh to trigger eviction by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # Verify all 5 files are still in cache after refresh
    for i in range(1, 6):
        assert cache_manager.contains(f"{test_uuid}/file{i}"), f"File {i} should not be evicted"

    # Verify total cache size exceeds the limit
    total_size = 0
    for i in range(1, 6):
        data = cache_manager.read(f"{test_uuid}/file{i}")
        if data is not None:
            total_size += len(data)
    assert total_size > 3 * 1024 * 1024, "Total cache size should exceed 3MB limit with NONE policy"

    # Verify no eviction thread was created during the test
    assert not hasattr(cache_manager, "_eviction_thread"), "No eviction thread should be created during test"
    assert not hasattr(cache_manager, "_eviction_thread_running"), (
        "No eviction thread running flag should exist during test"
    )
