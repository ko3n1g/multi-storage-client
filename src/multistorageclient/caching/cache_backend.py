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

import logging
import os
import stat
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Optional, Union

import xattr
from filelock import BaseFileLock, FileLock, Timeout

from ..instrumentation.utils import CacheManagerMetricsHelper
from ..types import SourceVersionCheckMode, StorageProvider
from .cache_config import CacheConfig
from .cache_item import CacheItem
from .eviction_policy import FIFO, LRU, NO_EVICTION, RANDOM, EvictionPolicyFactory


class _DummyLock:
    """A non-blocking dummy lock that always reports as unlocked."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CacheBackend(ABC):
    """
    Abstract base class for cache storage backends.

    This class defines the interface for cache storage backends, providing methods for
    reading, writing, and managing cached data.
    """

    def __init__(
        self,
        profile: str,
        cache_config: CacheConfig,
        storage_provider: Optional[StorageProvider] = None,
    ):
        """
        Initializes the :py:class:`CacheBackend` with the given profile and configuration.

        :param profile: The profile name for the cache.
        :param cache_config: The cache configuration settings.
        :param storage_provider: Optional storage provider for backend operations.
        """
        self._profile = profile
        self._cache_config = cache_config
        self._cache_refresh_interval = cache_config.eviction_policy.refresh_interval
        self._last_refresh_time = datetime.now()
        self._storage_provider = storage_provider

    @abstractmethod
    def use_etag(self) -> bool:
        """Check if etag is used in the cache config."""
        pass

    @abstractmethod
    def get_max_cache_size(self) -> int:
        """Return the cache size in bytes from the cache config."""
        pass

    @abstractmethod
    def _get_cache_dir(self) -> str:
        """Return the path to the local cache directory."""
        pass

    @abstractmethod
    def _get_cache_file_path(self, key: str) -> str:
        """Return the path to the local cache file for the given key."""
        pass

    @abstractmethod
    def read(self, key: str) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists."""
        pass

    @abstractmethod
    def open(
        self, key: str, mode: str = "rb", check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT
    ) -> Optional[Any]:
        """Open a file from the cache and return the file object."""
        pass

    @abstractmethod
    def set(self, key: str, source: Union[str, bytes]) -> None:
        """Store a file in the cache."""
        pass

    @abstractmethod
    def contains(self, key: str, check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT) -> bool:
        """Check if the cache contains a file corresponding to the given key."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a file from the cache."""
        pass

    @abstractmethod
    def cache_size(self) -> int:
        """Return the current size of the cache in bytes."""
        pass

    @abstractmethod
    def refresh_cache(self) -> bool:
        """Scan the cache directory and evict cache entries."""
        pass

    @abstractmethod
    def acquire_lock(self, key: str) -> BaseFileLock:
        """Create a FileLock object for a given key."""
        pass

    @abstractmethod
    def _check_if_eviction_policy_is_valid(self, eviction_policy: str) -> bool:
        """Check if the eviction policy is valid for this backend."""
        pass

    def _split_key(self, key: str) -> tuple[str, Optional[str]]:
        """Split the key into path and etag.

        :param key: The key to split.
        :return: A tuple containing the path and etag.
        """
        if ":" in key:
            path, etag = key.split(":", 1)
        else:
            path, etag = key, None
        return path, etag

    def get_cache_key(self, file_name: str) -> str:
        """Get the cache key for the given file name. Split the key into path and etag if it contains a colon.

        :param file_name: The file name to get the cache key for.
        :return: The cache key for the given file name.
        """
        return self._split_key(file_name)[0]

    def _should_refresh_cache(self) -> bool:
        """Check if enough time has passed since the last refresh."""
        now = datetime.now()
        return (now - self._last_refresh_time).seconds > self._cache_refresh_interval


class FileSystemBackend(CacheBackend):
    """
    A concrete implementation of the :py:class:`CacheBackend` that stores cache data in the local filesystem.
    """

    DEFAULT_FILE_LOCK_TIMEOUT = 600

    def __init__(
        self,
        profile: str,
        cache_config: CacheConfig,
        storage_provider: Optional[StorageProvider] = None,
    ):
        """
        Initializes the :py:class:`FileSystemBackend` with the given profile and configuration.

        :param profile: The profile name for the cache.
        :param cache_config: The cache configuration settings.
        :param storage_provider: Optional storage provider (not used in filesystem backend).
        """
        super().__init__(profile, cache_config, storage_provider)
        self._max_cache_size = cache_config.size_bytes()
        self._last_refresh_time = datetime.now()
        self._metrics_helper = CacheManagerMetricsHelper()

        # Create cache directory if it doesn't exist, this is used to download files
        self._cache_dir = os.path.abspath(cache_config.backend.cache_path)
        self._cache_path = os.path.join(self._cache_dir, self._profile)
        os.makedirs(self._cache_path, exist_ok=True)

        # Check if eviction policy is valid for this backend
        if not self._check_if_eviction_policy_is_valid(cache_config.eviction_policy.policy):
            raise ValueError(f"Invalid eviction policy: {cache_config.eviction_policy.policy}")

        self._eviction_policy = EvictionPolicyFactory.create(cache_config.eviction_policy.policy)

        # Create a lock file for cache refresh operations
        self._cache_refresh_lock_file = FileLock(
            os.path.join(self._cache_path, ".cache_refresh.lock"), timeout=self.DEFAULT_FILE_LOCK_TIMEOUT
        )

        # Populate cache with existing files in the cache directory
        self.refresh_cache()

    def _check_if_eviction_policy_is_valid(self, eviction_policy: str) -> bool:
        """Check if the eviction policy is valid for this backend.

        :param eviction_policy: The eviction policy to check.
        :return: True if the policy is valid, False otherwise.
        """
        return eviction_policy.lower() in {LRU, FIFO, RANDOM, NO_EVICTION}

    def get_file_size(self, file_path: str) -> Optional[int]:
        """Get the size of the file in bytes.

        Args:
            file_path: Path to the file

        Returns:
            Optional[int]: Size of the file in bytes, or None if file doesn't exist
        """
        try:
            return os.path.getsize(file_path)
        except OSError:
            return None

    def delete_file(self, file_path: str) -> None:
        """Delete a file from the cache directory.

        Args:
            file_path: Path to the file relative to cache directory
        """
        try:
            # Construct absolute path using cache directory as base
            abs_path = os.path.join(self._get_cache_dir(), file_path)
            os.unlink(abs_path)

            # Handle lock file - keep it in same directory as the file
            lock_name = f".{os.path.basename(file_path)}.lock"
            lock_path = os.path.join(os.path.dirname(abs_path), lock_name)
            os.unlink(lock_path)
        except OSError:
            pass

    def evict_files(self) -> None:
        """
        Evict cache entries based on the configured eviction policy.
        """
        logging.debug("\nStarting evict_files...")
        cache_items: list[CacheItem] = []

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                # Skip lock files and hidden files
                if file_name.endswith(".lock") or file_name.startswith("."):
                    continue
                try:
                    if os.path.isfile(file_path):
                        # Get the relative path from the cache directory
                        rel_path = os.path.relpath(file_path, self._cache_path)
                        cache_item = CacheItem.from_path(file_path, rel_path)
                        if cache_item and cache_item.file_size:
                            logging.debug(f"Found file: {rel_path}, size: {cache_item.file_size}")
                            cache_items.append(cache_item)
                except OSError:
                    # Ignore if file has already been evicted
                    pass

        logging.debug(f"\nFound {len(cache_items)} files before sorting")

        # Sort items according to eviction policy
        cache_items = self._eviction_policy.sort_items(cache_items)
        logging.debug("\nFiles after sorting by policy:")
        for item in cache_items:
            logging.debug(f"File: {item.file_path}")

        # Rebuild the cache
        cache = OrderedDict()
        cache_size = 0
        for item in cache_items:
            # Use the relative path from cache directory
            rel_path = os.path.relpath(item.file_path, self._cache_path)
            cache[rel_path] = item.file_size
            cache_size += item.file_size
        logging.debug(f"Total cache size: {cache_size}, Max allowed: {self._max_cache_size}")

        # Evict old files if necessary in case the existing files exceed cache size
        while cache_size > self._max_cache_size:
            # Pop the first item in the OrderedDict (according to policy's sorting)
            oldest_file, file_size = cache.popitem(last=False)
            cache_size -= file_size
            logging.debug(f"Evicting file: {oldest_file}, size: {file_size}")
            self.delete_file(oldest_file)

        logging.debug("\nFinal cache contents:")
        for file_path in cache.keys():
            logging.debug(f"Remaining file: {file_path}")

    def use_etag(self) -> bool:
        """Check if etag is used in the cache config."""
        return self._cache_config.use_etag

    def get_max_cache_size(self) -> int:
        """Return the cache size in bytes from the cache config."""
        return self._max_cache_size

    def _get_cache_dir(self) -> str:
        """Return the path to the local cache directory."""
        return os.path.join(self._cache_dir, self._profile)

    def _get_cache_file_path(self, key: str) -> str:
        """Return the path to the local cache file for the given key."""
        cache_key = self.get_cache_key(key)
        return os.path.join(self._cache_dir, self._profile, cache_key)

    def read(self, key: str) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists."""
        success = True
        try:
            try:
                if self.contains(key):
                    # Handle both key formats: with and without colon
                    key, _ = self._split_key(key)
                    file_path = self._get_cache_file_path(key)
                    with open(file_path, "rb") as fp:
                        data = fp.read()
                    # Update access time based on eviction policy
                    self._update_access_time(file_path)
                    return data
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="READ", success=success)

    def open(
        self, key: str, mode: str = "rb", check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT
    ) -> Optional[Any]:
        """Open a file from the cache and return the file object."""
        success = True
        try:
            try:
                if self.contains(key, check_source_version):
                    # Handle both key formats: with and without colon
                    key, _ = self._split_key(key)
                    file_path = self._get_cache_file_path(key)
                    # Update access time based on eviction policy
                    self._update_access_time(file_path)
                    return open(file_path, mode)
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="OPEN", success=success)

    def set(self, key: str, source: Union[str, bytes]) -> None:
        """Store a file in the cache."""
        success = True
        try:
            path, etag = self._split_key(key)

            file_path = self._get_cache_file_path(key)
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if isinstance(source, str):
                # Move the file to the cache directory
                os.rename(src=source, dst=file_path)
                # Only allow the owner to read and write the file
                os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IWUSR)
            else:
                # Create a temporary file and move the file to the cache directory
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, dir=os.path.dirname(file_path), prefix="."
                ) as temp_file:
                    temp_file_path = temp_file.name
                    temp_file.write(source)
                os.rename(src=temp_file_path, dst=file_path)
                # Only allow the owner to read and write the file
                os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IWUSR)

            # Set extended attribute (e.g., ETag)
            if etag:
                try:
                    xattr.setxattr(file_path, "user.etag", etag.encode("utf-8"))
                except OSError as e:
                    logging.warning(f"Failed to set xattr on {file_path}: {e}")

            # update access time if applicable
            self._update_access_time(file_path)

            # Refresh cache after a few minutes
            if self._should_refresh_cache():
                thread = threading.Thread(target=self.refresh_cache)
                thread.daemon = True
                thread.start()
        except Exception:
            success = False
            raise
        finally:
            self._metrics_helper.increase(operation="SET", success=success)

    def contains(self, key: str, check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT) -> bool:
        """Check if the cache contains a file corresponding to the given key."""
        try:
            # Parse key and etag
            path, source_etag = self._split_key(key)

            # Get cache path
            file_path = self._get_cache_file_path(key)

            # If file doesn't exist, return False
            if not os.path.exists(file_path):
                return False

            # If etag checking is disabled, return True if file exists
            if check_source_version == SourceVersionCheckMode.INHERIT:
                if not self.use_etag():
                    return True
            elif check_source_version == SourceVersionCheckMode.DISABLE:
                return True

            # Verify etag matches if checking is enabled
            try:
                xattr_value = xattr.getxattr(file_path, "user.etag")
                stored_etag = xattr_value.decode("utf-8")
                return stored_etag is not None and stored_etag == source_etag
            except OSError:
                # If xattr fails, assume etag doesn't match
                return False

        except Exception as e:
            logging.error(f"Error checking cache: {e}")
            return False

    def delete(self, key: str) -> None:
        """Delete a file from the cache."""
        try:
            key, _ = self._split_key(key)
            self.delete_file(key)
        finally:
            self._metrics_helper.increase(operation="DELETE", success=True)

    def cache_size(self) -> int:
        """Return the current size of the cache in bytes."""
        file_size = 0

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                if os.path.isfile(file_path) and not file_name.endswith(".lock"):
                    size = self.get_file_size(file_path)
                    if size:
                        file_size += size

        return file_size

    def refresh_cache(self) -> bool:
        """Scan the cache directory and evict cache entries."""
        try:
            # Skip eviction if policy is NO_EVICTION
            if self._cache_config.eviction_policy.policy.lower() == NO_EVICTION:
                self._last_refresh_time = datetime.now()
                return True

            # If the process acquires the lock, then proceed with the cache eviction
            with self._cache_refresh_lock_file.acquire(blocking=False):
                self.evict_files()
                self._last_refresh_time = datetime.now()
                return True
        except Timeout:
            # If the process cannot acquire the lock, ignore and wait for the next turn
            pass

        return False

    def acquire_lock(self, key: str) -> BaseFileLock:
        """Create a FileLock object for a given key."""
        key, _ = self._split_key(key)

        file_dir = os.path.dirname(os.path.join(self._get_cache_dir(), key))

        # Create lock file in the same directory as the file
        lock_name = f".{os.path.basename(key)}.lock"
        lock_file = os.path.join(file_dir, lock_name)
        return FileLock(lock_file, timeout=self.DEFAULT_FILE_LOCK_TIMEOUT)

    def _update_access_time(self, file_path: str) -> None:
        """Update access time to current time for LRU policy.

        Only updates atime, preserving mtime for FIFO ordering.
        This is used to track when files are accessed for LRU eviction.

        :param file_path: Path to the file to update access time.
        """
        current_time = time.time()
        try:
            # Only update atime, preserve mtime for FIFO ordering
            stat = os.stat(file_path)
            os.utime(file_path, (current_time, stat.st_mtime))
        except (OSError, FileNotFoundError):
            # File might be deleted by another process or have permission issues
            # Just continue without updating the access time
            pass


class StorageProviderBackend(CacheBackend):
    """
    A concrete implementation of the :py:class:`CacheBackend` that uses a storage provider for operations.
    """

    def __init__(
        self,
        profile: str,
        cache_config: CacheConfig,
        storage_provider: Optional[StorageProvider] = None,
    ):
        """
        Initializes the :py:class:`StorageProviderBackend` with the given profile and configuration.

        :param profile: The profile name for the cache.
        :param cache_config: The cache configuration settings.
        :param storage_provider: The storage provider to use for operations.

        :raises ValueError: If storage_provider is None.
        """
        if storage_provider is None:
            raise ValueError("StorageProviderBackend requires a storage provider")

        super().__init__(profile, cache_config, storage_provider)
        self._metrics_helper = CacheManagerMetricsHelper()
        self._max_cache_size = cache_config.size_bytes()

        # Check if eviction policy is valid for this backend
        if not self._check_if_eviction_policy_is_valid(cache_config.eviction_policy.policy):
            raise ValueError(f"Invalid eviction policy: {cache_config.eviction_policy.policy}")

        self._eviction_policy = EvictionPolicyFactory.create(cache_config.eviction_policy.policy)
        self._refresh_lock = threading.Lock()  # Local lock for refresh operations

    def _check_if_eviction_policy_is_valid(self, eviction_policy: str) -> bool:
        """Check if the eviction policy is valid for this backend.

        :param eviction_policy: The eviction policy to check.
        :return: True if the policy is valid, False otherwise.

        NOTE: In future, we may support other eviction policies (FIFO, RANDOM), but for now, we only support NO_EVICTION
        """
        return eviction_policy.lower() in {NO_EVICTION}

    @property
    def last_refresh_time(self) -> datetime:
        """Get the last refresh time."""
        return self._last_refresh_time

    @last_refresh_time.setter
    def last_refresh_time(self, value: datetime) -> None:
        """Set the last refresh time."""
        self._last_refresh_time = value

    def use_etag(self) -> bool:
        """Check if etag is used in the cache config."""
        return self._cache_config.use_etag

    def get_max_cache_size(self) -> int:
        """Return the cache size in bytes from the cache config."""
        return self._max_cache_size

    def _get_cache_dir(self) -> str:
        """Return the path to the s3 express cache directory."""
        cache_dir = self._cache_config.backend.cache_path + "/" + self._profile
        return cache_dir

    def _get_cache_file_path(self, key: str) -> str:
        """Return the path to the cache file for the given key."""
        cache_key = self.get_cache_key(key)
        return f"{self._get_cache_dir()}/{cache_key}"

    def read(self, key: str) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists."""
        success = True
        try:
            try:
                if self.contains(key):
                    key, _ = key.split(":")
                    cache_path = f"{self._get_cache_dir()}/{key}"
                    return self._storage_provider.get_object(cache_path)  # type: ignore
            except Exception:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="READ", success=success)

    def open(
        self, key: str, mode: str = "rb", check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT
    ) -> Optional[Any]:
        """Open a file from the cache and return the file object."""
        success = True

        try:
            try:
                if self.contains(key, check_source_version):
                    path, _ = key.split(":")
                    cache_path = f"{self._get_cache_dir()}/{path}"
                    data = self._storage_provider.get_object(cache_path)  # type: ignore
                    if "b" in mode:
                        file_obj = BytesIO(data if isinstance(data, bytes) else data.encode())
                    else:
                        file_obj = StringIO(data.decode() if isinstance(data, bytes) else data)

                    file_obj.name = cache_path
                    return file_obj
            except Exception:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="OPEN", success=success)

    def set(self, key: str, source: Union[str, bytes]) -> None:
        """Store a file in the cache."""
        success = True
        try:
            # Handle key with or without ETag
            path, etag = self._split_key(key)

            cache_path = f"{self._get_cache_dir()}/{path}"
            if isinstance(source, str):
                new_file_size = os.path.getsize(source)
                with open(source, "rb") as f:
                    data = f.read()
            else:
                data = source
                new_file_size = len(data)

            if new_file_size > self._max_cache_size:
                raise ValueError(
                    f"File size ({new_file_size} bytes) exceeds maximum cache size ({self._max_cache_size} bytes)"
                )

            # Store the object in S3 Express with etag metadata
            metadata = {"etag": etag} if etag else None
            # this metadata is used by cache backend to trace the etag of the source file
            # it is not the same metadata for the object in the actual bucket
            # for the actual file metadata, a separate call will needed to be made to the actual bucket
            self._storage_provider.put_object(path=cache_path, body=data, attributes=metadata)  # type: ignore

            self._last_refresh_time = datetime.now()

            # we are aiming for lazy refresh, so we only refresh the cache when necessary
            if self._should_refresh_cache():
                thread = threading.Thread(target=self.refresh_cache)
                thread.start()

        except Exception:
            success = False
        finally:
            self._metrics_helper.increase(operation="SET", success=success)

    def contains(self, key: str, check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT) -> bool:
        """Check if the cache contains a file corresponding to the given key.

        When use_etag=False, we only check if the file exists in cache.
        When use_etag=True, we also verify that the etag matches.
        """
        try:
            # Parse key and etag
            path, source_etag = self._split_key(key)
            # Get cache path and metadata
            cache_path = f"{self._get_cache_dir()}/{path}"
            metadata = self._storage_provider.get_object_metadata(cache_path)  # type: ignore

            # If no metadata, file doesn't exist
            if not metadata:
                return False

            # If etag checking is disabled, return True if file exists
            if check_source_version == SourceVersionCheckMode.INHERIT:
                if not self.use_etag():
                    return True
            elif check_source_version == SourceVersionCheckMode.DISABLE:
                return True

            # Verify etag matches if checking is enabled
            if metadata.metadata:
                stored_etag = metadata.metadata.get("etag")

                result = stored_etag is not None and stored_etag == source_etag
                return result

            return False

        except FileNotFoundError:
            return False
        except Exception as e:
            logging.error(f"Error checking cache: {e}")
            return False

    def delete(self, key: str) -> None:
        """Delete a file from the cache."""
        success = True
        try:
            path, _ = self._split_key(key)
            cache_path = f"{self._get_cache_dir()}/{path}"
            self._storage_provider.delete_object(cache_path)  # type: ignore
        except Exception:
            success = False
        finally:
            self._metrics_helper.increase(operation="DELETE", success=success)

    def cache_size(self) -> int:
        """Return the current size of the cache in bytes."""
        try:
            cache_dir_url = self._get_cache_dir()
            files = self._storage_provider.list_objects(cache_dir_url)  # type: ignore
            total_size = 0
            for obj in files:
                total_size += obj.content_length

            return total_size
        except Exception as e:
            logging.error(f"Error calculating cache size: {e}")
            return 0

    def refresh_cache(self) -> bool:
        """Periodic cache maintenance."""
        return True

    def _trigger_eviction(self) -> None:
        """Trigger cache eviction."""
        try:
            cache_dir_url = self._get_cache_dir()
            files = list(self._storage_provider.list_objects(cache_dir_url))  # type: ignore
            cache_items = []
            current_size = 0

            for obj in files:
                try:
                    if obj.content_length:
                        # Get the key from the ObjectMetadata object
                        obj_key = obj.key
                        if obj_key is None:
                            raise ValueError(f"Object key is None for {obj}")
                        if obj_key:
                            metadata = self._storage_provider.get_object_metadata(obj_key)  # type: ignore

                            if metadata.metadata and metadata.metadata.get("to-be-deleted"):
                                self._storage_provider.delete_object(obj_key)  # type: ignore
                                continue

                            current_size += obj.content_length
                            cache_items.append(
                                CacheItem(
                                    file_path=obj_key,
                                    file_size=obj.content_length,
                                    atime=0,
                                    mtime=metadata.last_modified.timestamp(),
                                    hashed_key=obj_key,
                                )
                            )
                except Exception:
                    continue

            if current_size <= self._max_cache_size:
                return

            cache_items = self.eviction_policy.sort_items(cache_items)  # type: ignore
            for item in cache_items:
                if current_size <= self._max_cache_size:
                    break
                try:
                    # s3express does not support object-tagging,
                    # so we will have to fall back on deleting the object at higher intervals
                    # like every 6 hours or a day or so
                    self._storage_provider.delete_object(item.file_path)  # type: ignore
                    current_size -= item.file_size
                except Exception:
                    continue
        except Exception as e:
            logging.error(f"Failed to evict files: {e}")

    def acquire_lock(self, key: str) -> BaseFileLock:
        """Create a dummy lock object for a given key."""
        return _DummyLock()  # type: ignore[return-value]
