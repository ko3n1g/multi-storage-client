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
import tempfile
import uuid
from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, Dict, Optional

import azure.storage.blob
import boto3
import google.auth.credentials
import google.cloud.storage


# Python's `tempfile` but for data stores.
#
# Backed by local storage services.


class TemporaryDataStore(AbstractContextManager):
    """
    This class creates a temporary data store. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    #: Profile configuration dictionary.
    _profile_config_dict: Dict[str, Any]

    def profile_config_dict(self) -> Dict[str, Any]:
        """
        Return a multi-storage client profile configuration dictionary for the temporary data store.
        """
        return copy.deepcopy(self._profile_config_dict)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> bool:
        self.cleanup()
        return False

    @abstractmethod
    def cleanup(self) -> None:
        """
        The temporary data store can be explicitly cleaned up by calling the ``cleanup()`` method.
        """
        pass


class TemporaryPOSIXDirectory(TemporaryDataStore):
    """
    This class creates a temporary POSIX directory. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    #: Directory.
    _directory: tempfile.TemporaryDirectory

    def __init__(self):
        # Backed by Python's `tempfile`.
        #
        # https://docs.python.org/3/library/tempfile.html
        self._directory = tempfile.TemporaryDirectory()

        self._profile_config_dict = {
            "storage_provider": {"type": "file", "options": {"base_path": self._directory.name}}
        }

    def cleanup(self) -> None:
        self._directory.cleanup()


class TemporaryAWSS3Bucket(TemporaryDataStore):
    """
    This class creates a temporary AWS S3 bucket. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    #: Bucket name.
    _bucket_name: str
    #: S3 client.
    _client: Any

    def __init__(self):
        self._bucket_name = str(uuid.uuid4())

        # Backed by MinIO.
        #
        # https://min.io/docs/minio/linux/index.html
        endpoint_url = "http://localhost:9000"
        access_key = "minioadmin"
        secret_key = "minioadmin"

        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

        self._client.create_bucket(Bucket=self._bucket_name)

        self._profile_config_dict = {
            "storage_provider": {
                "type": "s3",
                "options": {"endpoint_url": endpoint_url, "base_path": self._bucket_name},
            },
            "credentials_provider": {
                "type": "S3Credentials",
                "options": {"access_key": access_key, "secret_key": secret_key},
            },
        }

    def cleanup(self) -> None:
        try:
            for obj in self._client.list_objects_v2(Bucket=self._bucket_name).get("Contents", []):
                self._client.delete_object(Bucket=self._bucket_name, Key=obj["Key"])
            self._client.delete_bucket(Bucket=self._bucket_name)
        except self._client.exceptions.NoSuchBucket:
            pass


class TemporaryAzureBlobStorageContainer(TemporaryDataStore):
    """
    This class creates a temporary Azure Blob Storage container. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    #: Container name.
    _container_name: str
    #: Blob service client.
    _client: azure.storage.blob.BlobServiceClient

    def __init__(self):
        self._container_name = str(uuid.uuid4())

        # Backed by Azurite.
        #
        # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
        account = "devstoreaccount1"
        endpoint_url = f"http://localhost:10000/{account}"
        connection_string = ";".join(
            [
                "DefaultEndpointsProtocol=http",
                f"AccountName={account}",
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
                f"BlobEndpoint={endpoint_url}",
                f"QueueEndpoint=http://localhost:10001/{account}",
                f"TableEndpoint=http://localhost:10002/{account}",
            ]
        )

        self._client = azure.storage.blob.BlobServiceClient.from_connection_string(conn_str=connection_string)

        self._client.create_container(name=self._container_name)

        self._profile_config_dict = {
            "storage_provider": {
                "type": "azure",
                "options": {"endpoint_url": endpoint_url, "base_path": self._container_name},
            },
            "credentials_provider": {
                "type": "AzureCredentials",
                "options": {"connection": connection_string},
            },
        }

    def cleanup(self) -> None:
        container_client = self._client.get_container_client(container=self._container_name)
        if container_client.exists():
            for blob in container_client.list_blobs():
                container_client.delete_blob(blob=blob)
            container_client.delete_container()


class TemporaryGoogleCloudStorageBucket(TemporaryDataStore):
    """
    This class creates a temporary Google Cloud Storage bucket. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    #: Bucket name.
    _bucket_name: str
    #: Google Cloud Storage client.
    _client: google.cloud.storage.Client

    def __init__(self):
        self._bucket_name = str(uuid.uuid4())

        # Backed by fake-gcs-server.
        #
        # https://github.com/fsouza/fake-gcs-server
        project_id = "local"
        endpoint_url = "http://localhost:4443"

        self._client = google.cloud.storage.Client(
            project=project_id,
            credentials=google.auth.credentials.AnonymousCredentials(),
            client_options={"api_endpoint": endpoint_url},
        )

        self._client.create_bucket(bucket_or_name=self._bucket_name)

        self._profile_config_dict = {
            "storage_provider": {
                "type": "gcs",
                "options": {"project_id": project_id, "endpoint_url": endpoint_url, "base_path": self._bucket_name},
            }
        }

    def cleanup(self) -> None:
        bucket = self._client.bucket(bucket_name=self._bucket_name)
        if bucket.exists():
            for blob in self._client.list_blobs(bucket_or_name=self._bucket_name):
                bucket.delete_blob(blob_name=blob.name)
            bucket.delete()


class TemporarySwiftStackBucket(TemporaryAWSS3Bucket):
    """
    This class creates a temporary SwiftStack bucket. The resulting object can be used as a context manager.
    On completion of the context or destruction of the temporary data store object,
    the newly created temporary data store and all its contents are removed.
    """

    def __init__(self):
        super().__init__()
        self._profile_config_dict["storage_provider"]["type"] = "s8k"
