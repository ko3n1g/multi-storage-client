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
import multiprocessing
import os
import queue
import tempfile
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, Optional, Union, cast

from .config import StorageClientConfig
from .constants import MEMORY_LOAD_LIMIT
from .file import ObjectFile, PosixFile
from .instrumentation.utils import instrumented
from .progress_bar import ProgressBar
from .providers.posix_file import PosixFileStorageProvider
from .retry import retry
from .types import MSC_PROTOCOL, ObjectMetadata, Range, SourceVersionCheckMode
from .utils import NullStorageClient, calculate_worker_processes_and_threads, join_paths

logger = logging.Logger(__name__)


class _SyncOp(Enum):
    ADD = "add"
    DELETE = "delete"
    STOP = "stop"


@instrumented
class StorageClient:
    """
    A client for interacting with different storage providers.
    """

    _config: StorageClientConfig

    def __init__(self, config: StorageClientConfig):
        """
        Initializes the :py:class:`StorageClient` with the given configuration.

        :param config: The configuration object for the storage client.
        """
        self._initialize_providers(config)

    def _initialize_providers(self, config: StorageClientConfig) -> None:
        self._config = config
        self._credentials_provider = self._config.credentials_provider
        self._storage_provider = self._config.storage_provider
        self._metadata_provider = self._config.metadata_provider
        self._cache_config = self._config.cache_config
        self._retry_config = self._config.retry_config
        self._cache_manager = self._config.cache_manager

    def _build_cache_path(self, path: str) -> str:
        """
        Build cache path with or without etag.
        """
        cache_path = f"{path}:{None}"

        if self._metadata_provider:
            if self._cache_manager and self._cache_manager.use_etag():
                metadata = self._metadata_provider.get_object_metadata(path)
                cache_path = f"{path}:{metadata.etag}"
        else:
            if self._cache_manager and self._cache_manager.use_etag():
                metadata = self._storage_provider.get_object_metadata(path)
                cache_path = f"{path}:{metadata.etag}"

        return cache_path

    def _is_cache_enabled(self) -> bool:
        return self._cache_manager is not None and not self._is_posix_file_storage_provider()

    def _is_posix_file_storage_provider(self) -> bool:
        return isinstance(self._storage_provider, PosixFileStorageProvider)

    def is_default_profile(self) -> bool:
        """
        Return True if the storage client is using the default profile.
        """
        return self._config.profile == "default"

    @property
    def profile(self) -> str:
        return self._config.profile

    @retry
    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """
        Reads an object from the storage provider at the specified path.

        :param path: The path of the object to read.
        :return: The content of the object.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")

        # Never cache range-read requests
        if byte_range:
            return self._storage_provider.get_object(path, byte_range=byte_range)

        # Read from cache if the file exists
        if self._is_cache_enabled():
            assert self._cache_manager is not None
            cache_path = self._build_cache_path(path)
            data = self._cache_manager.read(cache_path)

            if data is None:
                data = self._storage_provider.get_object(path)
                self._cache_manager.set(cache_path, data)

            return data

        return self._storage_provider.get_object(path, byte_range=byte_range)

    def info(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored at the specified path.

        :param path: The path to the object for which metadata or information is being retrieved.
        :param strict: If True, performs additional validation to determine whether the path refers to a directory.

        :return: A dictionary containing metadata about the object.
        """
        if not self._metadata_provider:
            return self._storage_provider.get_object_metadata(path, strict=strict)

        # For metadata_provider, first check if the path exists as a file, then fallback to detecting if path is a directory.
        try:
            return self._metadata_provider.get_object_metadata(path)
        except FileNotFoundError:
            # Try listing from the parent to determine if path is a valid directory
            parent = os.path.dirname(path.rstrip("/")) + "/"
            parent = "" if parent == "/" else parent
            target = path.rstrip("/") + "/"

            try:
                entries = self._metadata_provider.list_objects(parent, include_directories=True)
                for entry in entries:
                    if entry.key == target and entry.type == "directory":
                        return entry
            except Exception:
                pass
            raise  # Raise original FileNotFoundError

    @retry
    def download_file(self, remote_path: str, local_path: str) -> None:
        """
        Downloads a file from the storage provider to the local file system.

        :param remote_path: The path of the file in the storage provider.
        :param local_path: The local path where the file should be downloaded.
        """

        if self._metadata_provider:
            real_path, exists = self._metadata_provider.realpath(remote_path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{remote_path}' was not found by metadata provider.")
            metadata = self._metadata_provider.get_object_metadata(remote_path)
            self._storage_provider.download_file(real_path, local_path, metadata)
        else:
            self._storage_provider.download_file(remote_path, local_path)

    @retry
    def upload_file(self, remote_path: str, local_path: str, attributes: Optional[dict[str, str]] = None) -> None:
        """
        Uploads a file from the local file system to the storage provider.

        :param remote_path: The path where the file should be stored in the storage provider.
        :param local_path: The local path of the file to upload.
        :param attributes: The attributes to add to the file.
        """
        virtual_path = remote_path
        if self._metadata_provider:
            remote_path, exists = self._metadata_provider.realpath(remote_path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )
        self._storage_provider.upload_file(remote_path, local_path, attributes)
        if self._metadata_provider:
            metadata = self._storage_provider.get_object_metadata(remote_path)
            self._metadata_provider.add_file(virtual_path, metadata)

    @retry
    def write(self, path: str, body: bytes, attributes: Optional[dict[str, str]] = None) -> None:
        """
        Writes an object to the storage provider at the specified path.

        :param path: The path where the object should be written.
        :param body: The content to write to the object.
        :param attributes: The attributes to add to the file.
        """
        virtual_path = path
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )
        self._storage_provider.put_object(path, body, attributes=attributes)
        if self._metadata_provider:
            # TODO(NGCDP-3016): Handle eventual consistency of Swiftstack, without wait.
            metadata = self._storage_provider.get_object_metadata(path)
            self._metadata_provider.add_file(virtual_path, metadata)

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copies an object from source to destination in the storage provider.

        :param src_path: The virtual path of the source object to copy.
        :param dest_path: The virtual path of the destination.
        """
        virtual_dest_path = dest_path
        if self._metadata_provider:
            src_path, exists = self._metadata_provider.realpath(src_path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{src_path}' was not found.")

            dest_path, exists = self._metadata_provider.realpath(dest_path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_dest_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )

        self._storage_provider.copy_object(src_path, dest_path)
        if self._metadata_provider:
            metadata = self._storage_provider.get_object_metadata(dest_path)
            self._metadata_provider.add_file(virtual_dest_path, metadata)

    def delete(self, path: str, recursive: bool = False) -> None:
        """
        Deletes an object from the storage provider at the specified path.

        :param path: The virtual path of the object to delete.
        :param recursive: Whether to delete objects in the path recursively.
        """
        if recursive:
            self.sync_from(
                NullStorageClient(),
                path,
                path,
                delete_unmatched_files=True,
                num_worker_processes=1,
                description="Deleting",
            )
            # If this is a posix storage provider, we need to also delete remaining directory stubs.
            if self._is_posix_file_storage_provider():
                posix_storage_provider = cast(PosixFileStorageProvider, self._storage_provider)
                posix_storage_provider.rmtree(path)
            return

        virtual_path = path
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{virtual_path}' was not found.")
            self._metadata_provider.remove_file(virtual_path)

        # Delete the cached file if it exists
        if self._is_cache_enabled():
            assert self._cache_manager is not None
            self._cache_manager.delete(virtual_path)

        self._storage_provider.delete_object(path)

    def glob(
        self,
        pattern: str,
        include_url_prefix: bool = False,
    ) -> list[str]:
        """
        Matches and retrieves a list of objects in the storage provider that
        match the specified pattern.

        :param pattern: The pattern to match object paths against, supporting wildcards (e.g., ``*.txt``).
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.

        :return: A list of object paths that match the pattern.
        """
        if self._metadata_provider:
            results = self._metadata_provider.glob(pattern)
        else:
            results = self._storage_provider.glob(pattern)

        if include_url_prefix:
            results = [join_paths(f"{MSC_PROTOCOL}{self._config.profile}", path) for path in results]

        return results

    def list(
        self,
        prefix: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        include_url_prefix: bool = False,
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified prefix.

        :param prefix: The prefix to list objects under.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.

        :return: An iterator over objects.
        """
        if self._metadata_provider:
            objects = self._metadata_provider.list_objects(prefix, start_after, end_at, include_directories)
        else:
            objects = self._storage_provider.list_objects(prefix, start_after, end_at, include_directories)

        for object in objects:
            if include_url_prefix:
                object.key = join_paths(f"{MSC_PROTOCOL}{self._config.profile}", object.key)
            yield object

    def open(
        self,
        path: str,
        mode: str = "rb",
        buffering: int = -1,
        encoding: Optional[str] = None,
        disable_read_cache: bool = False,
        memory_load_limit: int = MEMORY_LOAD_LIMIT,
        atomic: bool = True,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        attributes: Optional[dict[str, str]] = None,
    ) -> Union[PosixFile, ObjectFile]:
        """
        Returns a file-like object from the storage provider at the specified path.

        :param path: The path of the object to read.
        :param mode: The file mode, only "w", "r", "a", "wb", "rb" and "ab" are supported.
        :param buffering: The buffering mode. Only applies to PosixFile.
        :param encoding: The encoding to use for text files.
        :param disable_read_cache: When set to True, disables caching for the file content.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
        :param memory_load_limit: Size limit in bytes for loading files into memory. Defaults to 512MB.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
        :param atomic: When set to True, the file will be written atomically (rename upon close).
            This parameter is only applicable to PosixFile in write mode.
        :param check_source_version: Whether to check the source version of cached objects.
        :param attributes: The attributes to add to the file.  This parameter is only applicable when the mode is "w" or "wb" or "a" or "ab".
        :return: A file-like object (PosixFile or ObjectFile) for the specified path.
        """
        if self._is_posix_file_storage_provider():
            return PosixFile(
                self, path=path, mode=mode, buffering=buffering, encoding=encoding, atomic=atomic, attributes=attributes
            )
        else:
            if atomic is False:
                logger.warning("Non-atomic writes are not supported for object storage providers.")

            return ObjectFile(
                self,
                remote_path=path,
                mode=mode,
                encoding=encoding,
                disable_read_cache=disable_read_cache,
                memory_load_limit=memory_load_limit,
                check_source_version=check_source_version,
                attributes=attributes,
            )

    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified path points to a file (rather than a directory or folder).

        :param path: The path to check.

        :return: ``True`` if the path points to a file, ``False`` otherwise.
        """
        if self._metadata_provider:
            _, exists = self._metadata_provider.realpath(path)
            return exists
        return self._storage_provider.is_file(path)

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        """
        Commits any pending updates to the metadata provider. No-op if not using a metadata provider.

        :param prefix: If provided, scans the prefix to find files to commit.
        """
        if self._metadata_provider:
            if prefix:
                # The virtual path for each item will be the physical path with
                # the base physical path removed from the beginning.
                physical_base, _ = self._metadata_provider.realpath("")
                physical_prefix, _ = self._metadata_provider.realpath(prefix)
                for obj in self._storage_provider.list_objects(prefix=physical_prefix):
                    virtual_path = obj.key[len(physical_base) :].lstrip("/")
                    self._metadata_provider.add_file(virtual_path, obj)
            self._metadata_provider.commit_updates()

    def is_empty(self, path: str) -> bool:
        """
        Checks whether the specified path is empty. A path is considered empty if there are no
        objects whose keys start with the given path as a prefix.

        :param path: The path to check. This is typically a prefix representing a directory or folder.

        :return: ``True`` if no objects exist under the specified path prefix, ``False`` otherwise.
        """
        if self._metadata_provider:
            objects = self._metadata_provider.list_objects(path)
        else:
            objects = self._storage_provider.list_objects(path)

        try:
            return next(objects) is None
        except StopIteration:
            pass
        return True

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state["_credentials_provider"]
        del state["_storage_provider"]
        del state["_metadata_provider"]
        del state["_cache_manager"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        config = state["_config"]
        self._initialize_providers(config)

    def sync_from(
        self,
        source_client: "StorageClient",
        source_path: str = "",
        target_path: str = "",
        delete_unmatched_files: bool = False,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
    ) -> None:
        """
        Syncs files from the source storage client to "path/".

        :param source_client: The source storage client.
        :param source_path: The path to sync from.
        :param target_path: The path to sync to.
        :param delete_unmatched_files: Whether to delete files at the target that are not present at the source.
        :param description: Description of sync process for logging purposes.
        :param num_worker_processes: The number of worker processes to use.
        """
        source_path = source_path.lstrip("/")
        target_path = target_path.lstrip("/")

        if source_client == self and (source_path.startswith(target_path) or target_path.startswith(source_path)):
            raise ValueError("Source and target paths cannot overlap on same StorageClient.")

        logger.debug(f"Starting sync operation {description}")

        # Attempt to balance the number of worker processes and threads.
        num_worker_processes, num_worker_threads = calculate_worker_processes_and_threads(num_worker_processes)

        if num_worker_processes == 1:
            file_queue = queue.Queue(maxsize=100000)
            result_queue = queue.Queue()
        else:
            manager = multiprocessing.Manager()
            file_queue = manager.Queue(maxsize=100000)
            result_queue = manager.Queue()

        progress = ProgressBar(desc=description, show_progress=True, total_items=0)

        def match_file_metadata(source_info: ObjectMetadata, target_info: ObjectMetadata) -> bool:
            # If target and source have valid etags defined, use etag and file size to compare.
            if source_info.etag and target_info.etag:
                return source_info.etag == target_info.etag and source_info.content_length == target_info.content_length
            # Else, check file size is the same and the target's last_modified is newer than the source.
            return (
                source_info.content_length == target_info.content_length
                and source_info.last_modified <= target_info.last_modified
            )

        def producer():
            """Lists source files and adds them to the queue."""
            source_iter = iter(source_client.list(prefix=source_path))
            target_iter = iter(self.list(prefix=target_path))
            total_count = 0

            source_file = next(source_iter, None)
            target_file = next(target_iter, None)

            while source_file or target_file:
                # Update progress and count each pair (or single) considered for syncing
                if total_count % 1000 == 0:
                    progress.update_total(total_count)
                total_count += 1

                if source_file and target_file:
                    source_key = source_file.key[len(source_path) :].lstrip("/")
                    target_key = target_file.key[len(target_path) :].lstrip("/")

                    if source_key < target_key:
                        file_queue.put((_SyncOp.ADD, source_file))
                        source_file = next(source_iter, None)
                    elif source_key > target_key:
                        if delete_unmatched_files:
                            file_queue.put((_SyncOp.DELETE, target_file))
                        else:
                            progress.update_progress()
                        target_file = next(target_iter, None)  # Skip unmatched target file
                    else:
                        # Both exist, compare metadata
                        if not match_file_metadata(source_file, target_file):
                            file_queue.put((_SyncOp.ADD, source_file))
                        else:
                            progress.update_progress()

                        source_file = next(source_iter, None)
                        target_file = next(target_iter, None)
                elif source_file:
                    file_queue.put((_SyncOp.ADD, source_file))
                    source_file = next(source_iter, None)
                else:
                    if delete_unmatched_files:
                        file_queue.put((_SyncOp.DELETE, target_file))
                    else:
                        progress.update_progress()
                    target_file = next(target_iter, None)

            progress.update_total(total_count)

            for _ in range(num_worker_threads * num_worker_processes):
                file_queue.put((_SyncOp.STOP, None))  # Signal consumers to stop

        producer_thread = threading.Thread(target=producer, daemon=True)
        producer_thread.start()

        def _result_consumer():
            # Pull from result_queue to collect pending updates from each multiprocessing worker.
            while True:
                op, target_file_path, physical_metadata = result_queue.get()
                if op == _SyncOp.STOP:
                    break

                if self._metadata_provider:
                    if op == _SyncOp.ADD:
                        # Use realpath() to get physical path so metadata provider can
                        # track the logical/physical mapping.
                        phys_path, _ = self._metadata_provider.realpath(target_file_path)
                        physical_metadata.key = phys_path
                        self._metadata_provider.add_file(target_file_path, physical_metadata)
                    elif op == _SyncOp.DELETE:
                        self._metadata_provider.remove_file(target_file_path)
                    else:
                        raise RuntimeError(f"Unknown operation: {op}")
                progress.update_progress()

        result_consumer_thread = threading.Thread(target=_result_consumer, daemon=True)
        result_consumer_thread.start()

        if num_worker_processes == 1:
            # Single process does not require multiprocessing.
            _sync_worker_process(
                source_client, source_path, self, target_path, num_worker_threads, file_queue, result_queue
            )
        else:
            with multiprocessing.Pool(processes=num_worker_processes) as pool:
                pool.apply(
                    _sync_worker_process,
                    args=(source_client, source_path, self, target_path, num_worker_threads, file_queue, result_queue),
                )

        producer_thread.join()

        result_queue.put((_SyncOp.STOP, None, None))
        result_consumer_thread.join()

        self.commit_metadata()
        progress.close()
        logger.debug(f"Completed sync operation {description}")


def _sync_worker_process(
    source_client: StorageClient,
    source_path: str,
    target_client: StorageClient,
    target_path: str,
    num_worker_threads: int,
    file_queue: queue.Queue,
    result_queue: Optional[queue.Queue],
):
    """Helper function for sync_from, defined at top-level for multiprocessing."""

    def _sync_consumer() -> None:
        """Processes files from the queue and copies them."""
        while True:
            op, file_metadata = file_queue.get()
            if op == _SyncOp.STOP:
                break

            source_key = file_metadata.key[len(source_path) :].lstrip("/")
            target_file_path = os.path.join(target_path, source_key)

            if op == _SyncOp.ADD:
                logger.debug(f"sync {file_metadata.key} -> {target_file_path}")
                if file_metadata.content_length < MEMORY_LOAD_LIMIT:
                    file_content = source_client.read(file_metadata.key)
                    target_client.write(target_file_path, file_content)
                else:
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        temp_filename = temp_file.name

                    try:
                        source_client.download_file(file_metadata.key, temp_filename)
                        target_client.upload_file(target_file_path, temp_filename)
                    finally:
                        os.remove(temp_filename)  # Ensure the temporary file is removed
            elif op == _SyncOp.DELETE:
                logger.debug(f"rm {file_metadata.key}")
                target_client.delete(file_metadata.key)
            else:
                raise ValueError(f"Unknown operation: {op}")

            if result_queue:
                if op == _SyncOp.ADD:
                    # add tuple of (virtual_path, physical_metadata) to result_queue
                    if target_client._metadata_provider:
                        physical_metadata = target_client._metadata_provider.get_object_metadata(
                            target_file_path, include_pending=True
                        )
                    else:
                        physical_metadata = None
                    result_queue.put((op, target_file_path, physical_metadata))
                elif op == _SyncOp.DELETE:
                    result_queue.put((op, target_file_path, None))
                else:
                    raise RuntimeError(f"Unknown operation: {op}")

    # Worker process that spawns threads to handle syncing.
    with ThreadPoolExecutor(max_workers=num_worker_threads) as executor:
        futures = [executor.submit(_sync_consumer) for _ in range(num_worker_threads)]
        for future in futures:
            future.result()  # Ensure all threads complete
