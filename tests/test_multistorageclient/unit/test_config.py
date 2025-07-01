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
import pickle
import sys
import tempfile
from typing import cast

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.config import SimpleProviderBundle, _find_config_file_paths
from multistorageclient.providers import (
    ManifestMetadataProvider,
    PosixFileStorageProvider,
    S3StorageProvider,
    StaticS3CredentialsProvider,
)
from multistorageclient.types import StorageProviderConfig


def test_json_config() -> None:
    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                }
            }
        }
    }"""
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_yaml_config() -> None:
    config = StorageClientConfig.from_yaml(
        """
        # YAML Example
        profiles:
          # Profile name
          default:
            # POSIX file
            storage_provider:
              type: file
              options:
                base_path: /
        """
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_override_default_profile() -> None:
    with pytest.raises(ValueError) as ex:
        StorageClientConfig.from_json(
            """{
            "profiles": {
                "default": {
                    "storage_provider": {
                        "type": "s3",
                        "options": {
                            "base_path": "mybucket"
                        }
                    }
                }
            }
        }"""
        )

    assert ex.match('Cannot override "default" profile with storage provider type "s3"; expected "file".')


def test_credentials_provider() -> None:
    os.environ["S3_ACCESS_KEY"] = "my_key"
    os.environ["S3_SECRET_KEY"] = "my_secret"
    json_config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {
                        "access_key": "${S3_ACCESS_KEY}",
                        "secret_key": "${S3_SECRET_KEY}"
                    }
                }
            }
        }
    }"""
    )

    yaml_config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            credentials_provider:
              type: S3Credentials
              options:
                access_key: ${S3_ACCESS_KEY}
                secret_key: ${S3_SECRET_KEY}
        """
    )

    assert json_config._config_dict == yaml_config._config_dict

    storage_client = StorageClient(yaml_config)
    assert isinstance(storage_client._credentials_provider, StaticS3CredentialsProvider)
    assert storage_client._credentials_provider._access_key == "my_key"
    assert storage_client._credentials_provider._secret_key == "my_secret"


def test_load_extensions() -> None:
    sys.path.append(os.path.dirname(__file__))
    from test_multistorageclient.unit.utils.mocks import (
        TestCredentialsProvider,
        TestMetadataProvider,
    )

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            credentials_provider:
              type: test_multistorageclient.unit.utils.mocks.TestCredentialsProvider
            metadata_provider:
              type: test_multistorageclient.unit.utils.mocks.TestMetadataProvider
        """
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)


def test_load_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))
    from test_multistorageclient.unit.utils.mocks import (
        TestCredentialsProvider,
        TestMetadataProvider,
    )

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          test-provider-bundle:
            provider_bundle:
              type: test_multistorageclient.unit.utils.mocks.TestProviderBundle
        """,
        profile="test-provider-bundle",
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_load_direct_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))
    from test_multistorageclient.unit.utils.mocks import (
        TestCredentialsProvider,
        TestMetadataProvider,
    )

    bundle = SimpleProviderBundle(
        storage_provider_config=StorageProviderConfig(type="file", options={"base_path": "/"}),
        credentials_provider=TestCredentialsProvider(),
        metadata_provider=TestMetadataProvider(),
    )
    config = StorageClientConfig.from_provider_bundle(config_dict={}, provider_bundle=bundle)

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)

    # Expect an error if pickling this storage_client because it cannot be
    # recreated in another process.
    with pytest.raises(ValueError):
        pickled_client = pickle.dumps(storage_client)
        _ = pickle.loads(pickled_client)


def test_swiftstack_storage_provider() -> None:
    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "swift_profile": {
                "storage_provider": {
                    "type": "s8k",
                    "options": {
                        "base_path": "/",
                        "endpoint_url": "https://pdx.s8k.io",
                        "region_name": "us-east-1"
                    }
                }
            }
        }
    }""",
        profile="swift_profile",
    )

    assert isinstance(config.storage_provider, S3StorageProvider)


def test_manifest_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))

    json_config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/some_base_path"
                    }
                },
                "metadata_provider": {
                    "type": "manifest",
                    "options": {
                        "manifest_path": ".msc_manifests"
                    }
                }
            }
        }
    }"""
    )

    yaml_config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /some_base_path
            metadata_provider:
              type: manifest
              options:
                manifest_path: .msc_manifests
        """
    )

    assert json_config._config_dict == yaml_config._config_dict

    storage_client = StorageClient(yaml_config)
    assert isinstance(storage_client._metadata_provider, ManifestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_manifest_type_unrecognized() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_base_path
                metadata_provider:
                  type: file
                  options:
                    manifest_path: .msc_manifests
            """
        )

    assert "Expected a fully qualified class name" in str(e), f"Unexpected error message: {str(e)}"


def test_storage_provider_profile_unrecognized() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_base_path
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
                    storage_provider_profile: non-existent-profile
            """
        )

    assert "Profile 'non-existent-profile' referenced by storage_provider_profile does not exist" in str(e), (
        f"Unexpected error message: {str(e)}"
    )


def test_storage_provider_profile_with_manifest() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              profile-manifest:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_manifest_base_path
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
              profile-data:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_other_base_path/data
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
                    storage_provider_profile: profile-manifest
            """,
            profile="profile-data",
        )

    assert "Profile 'profile-manifest' cannot have a metadata provider when used for manifests" in str(e), (
        f"Unexpected error message: {str(e)}"
    )


def test_load_retry_config() -> None:
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            retry:
              attempts: 4
              delay: 0.5
        """
    )

    storage_client = StorageClient(config)
    assert storage_client._retry_config.attempts == 4
    assert storage_client._retry_config.delay == 0.5

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
        """
    )

    storage_client = StorageClient(config)
    assert storage_client._retry_config.attempts == 3
    assert storage_client._retry_config.delay == 1.0

    with pytest.raises(ValueError) as e:
        config = StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /
                retry:
                  attempts: 0
                  delay: 0.5
            """
        )

    assert "Attempts must be at least 1." in str(e), f"Unexpected error message: {str(e)}"


def test_s3_storage_provider_on_public_bucket() -> None:
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          s3_public_profile:
            storage_provider:
              type: s3
              options:
                base_path: public-bucket
                region_name: us-west-2
                signature_version: UNSIGNED
        """,
        profile="s3_public_profile",
    )
    assert isinstance(config.storage_provider, S3StorageProvider)


def test_ais_storage_provider_passthrough_options() -> None:
    profile = "data"
    StorageClient(
        config=StorageClientConfig.from_dict(
            config_dict={
                "profiles": {
                    profile: {
                        "storage_provider": {
                            "type": "ais",
                            "options": {
                                "base_path": "bucket",
                                "endpoint": "http://127.0.0.1:51080",
                                # Passthrough options.
                                "timeout": (1.0, 2.0),
                                "retry": {
                                    "total": 2,
                                    "connect": 1,
                                    "read": 1,
                                    "redirect": 1,
                                    "status": 1,
                                    "other": 0,
                                    "allowed_methods": {"GET", "PUT", "POST"},
                                    "status_forcelist": {
                                        "429",
                                        "500",
                                        "501",
                                        "502",
                                        "503",
                                        "504",
                                    },
                                },
                            },
                        }
                    }
                }
            },
            profile=profile,
        )
    )


def test_azure_storage_provider_passthrough_options() -> None:
    profile = "data"
    StorageClient(
        config=StorageClientConfig.from_dict(
            config_dict={
                "profiles": {
                    profile: {
                        "storage_provider": {
                            "type": "azure",
                            "options": {
                                "base_path": "bucket",
                                "endpoint_url": "http://localhost:10000/devstoreaccount1",
                                # Passthrough options.
                                "retry_total": 2,
                                "retry_connect": 1,
                                "retry_read": 1,
                                "retry_status": 1,
                                "connection_timeout": 1,
                                "read_timeout": 1,
                            },
                        }
                    }
                }
            },
            profile=profile,
        )
    )


def test_oci_storage_provider_passthrough_options() -> None:
    with (
        tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as oci_config_file,
        tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as oci_key_file,
    ):
        # Placeholder PEM file from `openssl genrsa -out oci_api_key.pem 2048`.
        #
        # https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#apisigningkey_topic_How_to_Generate_an_API_Signing_Key_Mac_Linux
        oci_key_file_body = "\n".join(
            [
                "-----BEGIN PRIVATE KEY-----",
                "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCneEzaM/KC4cAd",
                "31AGTeHj+EwVic84K98uIiw85HBIv0ORIrX510oPleRkK1ElOHAS5bCJ20+UBKXB",
                "URS9pU3d8UhBLpGyRhH9/L2k+4DEKOjuXLCRZ16r/AA3GPumKdY/OmgY1h2cJWxW",
                "KQgkIfcQxFb1SVvDnmSWVyEjf313T+f63AWHqXZrIn+R66Le9MqJed0ChG4dklQa",
                "+kXE2AKGoH3JmGQ18XHcygKq1s/BzO2g+bIdeFi/EoZqybAfuCQjssA2zhaykU+F",
                "ODYrExqst5mVgn8QJIJR2BY4zrlbPY/mv9Tb3HkZTWGgnxpdN4rLNCNZeZaw5OpY",
                "8c60WULDAgMBAAECggEAIoKOy7RCuCfPEBjRg8sOzox/GT0hv4CC6B3QoeetH8CS",
                "KtlNSKPNtjJ8MwweF55useY1H+NanbTrd0+/B2mGB0NOUWhIS8VWtdEcP2A4Y7PO",
                "dDgThpMXljdC0BfM26vpY3QkuWF+DoxDq+merNt27zSWestYJpKARd7EjG0cLLar",
                "x0BiUu4nOC3mIAw+lwo43PeF1pCvzuytGPbXDkluuGzEC5VxC129Swgg10IN7JQp",
                "p7awROqykZYgEbrOh+IWBUG7TXqR5c6qGs/jC9FnMoX3zI/U0G0b4oJE/NRyAl23",
                "DEm3i7xXLFBrtvVKjKm+bfBOcHkNWNPCRl3EdIvfoQKBgQDnkUnnbZhvAoa+dbbT",
                "iHUIPOiHc5MQAJNRJ1KBexmjVBJpZliRuSXT5ctC/D5wnzzJh+vzOw8essOiGLzP",
                "6Zve2uqqTD4gcyaKRf+kc5x64gYQCitjA08WVpCXkNuYDRlQs55WFAVzIDPLo+Jk",
                "XQsnIhEpJfY0jA+FggDuYsi30wKBgQC5I7cNQ1MWalmhV03M+2Gjy59AZuEFQVNT",
                "v7LCxOj8FF4wIe/VzvRcHcCE0QVr9Mhl1uQUr74QeOmd5uULYULsc0Q8hojFPU1j",
                "9L5EpbsmTfXUVZtwSRO+OD0Y9J5JTkxoG0nplroskuqJZLEBaIUyNe1rbeNhMsh7",
                "pCg7IW3jUQKBgEQTAAjavQ8VTQs8i6yP1ue/EBSRs0/m+2fGCYkq6RSMqIT3o13j",
                "ce1jBmgAw1JUXYhZPtHYMM+zebNzVj5AzKOs84NwumrLry7C+S4dFolBXMrmUm7f",
                "ECbe9862tPd0ElcZFpjzdc6sTs20td8PQzIT37ua/0/fRMjYuPFbdOolAoGBAJMX",
                "yibyd4AWrPGf8INMsk21yOgdFOjc9vxSEQ/n7IfjEtZBEFEaJVFOnheoDhuwlss6",
                "yWmaG3Lw7gNzYEUDWG2OQwenh+DVjLg+yjC2UBPl2suB3IaAuPvnqLs8Fsp9N/16",
                "uOWqyG4Dp+3TH0LULQcwi1pQK1idRWXejcw1Ch6RAoGAXpDFjN5lDuismPoYcTrD",
                "1zwHjK0rsQIsbIqj1APosuZiEfdwB7uRw59omvE1rvhHBn+wMRcRM+Hz5aPKUzMY",
                "hZ1f3HOEN33OfSBpFjopgcl07JDaJ0/Yaxtti7DpriWxouweXD+08/R1k6aQVzvp",
                "mMEsnbNZO07g0D3mFCRUDY4=",
                "-----END PRIVATE KEY-----",
            ]
        )
        oci_key_file.write(oci_key_file_body)
        oci_key_file.close()
        oci_config_file_body = "\n".join(
            [
                "[DEFAULT]",
                "user=ocid1.user.oc1..unique-id",
                # Placeholder PEM file fingerprint from `openssl rsa -pubout -outform DER -in oci_api_key.pem | openssl md5 -c`.
                #
                # https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#four
                "fingerprint=26:b1:9b:2b:9b:9d:ec:57:32:bd:a5:1d:24:21:ec:68",
                f"key_file={oci_key_file.name}",
                "tenancy=ocid1.tenancy.oc1..unique-id",
                "region=us-ashburn-1",
            ]
        )
        oci_config_file.write(oci_config_file_body)
        oci_config_file.close()
        os.environ["OCI_CONFIG_FILE"] = oci_config_file.name

        profile = "data"
        StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        profile: {
                            "storage_provider": {
                                "type": "oci",
                                "options": {
                                    "base_path": "bucket",
                                    "namespace": "oci-namespace",
                                    # Passthrough options.
                                    "retry_strategy": {
                                        "max_attempts_check": True,
                                        "service_error_check": True,
                                        "total_elapsed_time_check": True,
                                        "max_attempts": 2,
                                        "total_elapsed_time_seconds": 1,
                                        "service_error_retry_config": {429: ["TooManyRequests"]},
                                        "service_error_retry_on_any_5xx": True,
                                        "retry_base_sleep_time_seconds": 1,
                                        "retry_exponential_growth_factor": 2,
                                        "retry_max_wait_between_calls_seconds": 30,
                                        "decorrelated_jitter": 1,
                                        "backoff_type": "decorrelated_jitter",
                                    },
                                },
                            }
                        }
                    }
                },
                profile=profile,
            )
        )


def test_s3_storage_provider_passthrough_options() -> None:
    profile = "data"
    StorageClient(
        config=StorageClientConfig.from_dict(
            config_dict={
                "profiles": {
                    profile: {
                        "storage_provider": {
                            "type": "s3",
                            "options": {
                                "base_path": "bucket",
                                "endpoint_url": "https://s3.us-east-1.amazonaws.com",
                                # Passthrough options.
                                "request_checksum_calculation": "when_required",
                                "response_checksum_validation": "when_required",
                                "max_pool_connections": 1,
                                "connect_timeout": 1,
                                "read_timeout": 1,
                                "retries": {
                                    "total_max_attempts": 2,
                                    "max_attempts": 1,
                                    "mode": "adaptive",
                                },
                            },
                        }
                    }
                }
            },
            profile=profile,
        )
    )


def test_gcs_s3_storage_provider_passthrough_options() -> None:
    profile = "data"
    StorageClient(
        config=StorageClientConfig.from_dict(
            config_dict={
                "profiles": {
                    profile: {
                        "storage_provider": {
                            "type": "gcs_s3",
                            "options": {
                                "base_path": "bucket",
                                "endpoint_url": "https://storage.googleapis.com",
                                # Passthrough options.
                                "request_checksum_calculation": "when_required",
                                "response_checksum_validation": "when_required",
                                "max_pool_connections": 1,
                                "connect_timeout": 1,
                                "read_timeout": 1,
                                "retries": {
                                    "total_max_attempts": 2,
                                    "max_attempts": 1,
                                    "mode": "adaptive",
                                },
                            },
                        }
                    }
                }
            },
            profile=profile,
        )
    )


def test_s8k_storage_provider_passthrough_options() -> None:
    profile = "data"
    StorageClient(
        config=StorageClientConfig.from_dict(
            config_dict={
                "profiles": {
                    profile: {
                        "storage_provider": {
                            "type": "s8k",
                            "options": {
                                "base_path": "bucket",
                                "endpoint_url": "https://pdx.s8k.io",
                                # Passthrough options.
                                "request_checksum_calculation": "when_required",
                                "response_checksum_validation": "when_required",
                                "max_pool_connections": 1,
                                "connect_timeout": 1,
                                "read_timeout": 1,
                                "retries": {
                                    "total_max_attempts": 2,
                                    "max_attempts": 1,
                                    "mode": "adaptive",
                                },
                            },
                        }
                    }
                }
            },
            profile=profile,
        )
    )


def test_credentials_provider_with_base_path_endpoint_url() -> None:
    sys.path.append(os.path.dirname(__file__))
    from test_multistorageclient.unit.utils.mocks import (
        TestScopedCredentialsProvider,
    )

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          temp_creds_profile:
            storage_provider:
              type: s8k
              options:
                base_path: mybucket/myprefix
                endpoint_url: https://pdx.s8k.io
            credentials_provider:
              type: >-
                test_multistorageclient.unit.utils.mocks.TestScopedCredentialsProvider
              options:
                expiry: 1000
        """,
        profile="temp_creds_profile",
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestScopedCredentialsProvider)
    assert storage_client._credentials_provider._base_path == "mybucket/myprefix"
    assert storage_client._credentials_provider._endpoint_url == "https://pdx.s8k.io"
    assert storage_client._credentials_provider._expiry == 1000


def test_storage_options_does_not_override_creds_provider_options() -> None:
    sys.path.append(os.path.dirname(__file__))
    from test_multistorageclient.unit.utils.mocks import (
        TestScopedCredentialsProvider,
    )

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          temp_creds_profile:
            storage_provider:
              type: s8k
              options:
                base_path: mybucket/myprefix
                endpoint_url: https://pdx.s8k.io
                region_name: us-east-1
            credentials_provider:
              type: >-
                test_multistorageclient.unit.utils.mocks.TestScopedCredentialsProvider
              options:
                base_path: mybucket/myprefix/mysubprefix
                expiry: 1000
        """,
        profile="temp_creds_profile",
    )

    storage_client = StorageClient(config)
    assert storage_client._storage_provider._region_name == "us-east-1"
    assert isinstance(storage_client._credentials_provider, TestScopedCredentialsProvider)
    assert storage_client._credentials_provider._base_path == "mybucket/myprefix/mysubprefix"
    assert storage_client._credentials_provider._endpoint_url == "https://pdx.s8k.io"
    assert storage_client._credentials_provider._expiry == 1000


def test_legacy_cache_config():
    """Test that legacy cache config with size_mb and string eviction_policy raises schema validation error."""
    config_dict = {
        "profiles": {"test": {"storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}}}},
        "cache": {"location": "/tmp/msc_cache", "size_mb": 200000, "use_etag": True, "eviction_policy": "fifo"},
    }

    with pytest.raises(RuntimeError, match="Failed to validate the config file"):
        StorageClientConfig.from_dict(config_dict, "test")


def test_cache_config_defaults():
    """Test cache config with minimal configuration."""
    config_dict = {
        "profiles": {"test": {"storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}}}},
        "cache": {"size": "100M", "eviction_policy": {"policy": "fifo"}},
    }

    config = StorageClientConfig.from_dict(config_dict, "test")
    assert config.cache_config is not None
    assert config.cache_manager is not None

    # Verify default values
    assert config.cache_config.size == "100M"
    assert config.cache_config.use_etag is True  # Default value
    assert config.cache_config.eviction_policy.policy == "fifo"  # Default value
    assert config.cache_config.eviction_policy.refresh_interval == 300  # Default value


def test_invalid_cache_config():
    """Test invalid cache config combinations."""
    # Test invalid size format
    config_dict = {
        "profiles": {"test": {"storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}}}},
        "cache": {
            "size": "invalid",  # Invalid size format
            "use_etag": True,
            "location": "/tmp/msc_cache",
            "eviction_policy": {"policy": "lru", "refresh_interval": 300},
        },
    }

    with pytest.raises(RuntimeError, match="Failed to validate the config file"):
        StorageClientConfig.from_dict(config_dict, "test")

    # Relative location is not allowed
    config_dict = {
        "profiles": {"test": {"storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}}}},
        "cache": {
            "size": "200G",
            "use_etag": True,
            "location": "relative/path",
        },
    }

    with pytest.raises(ValueError, match="Cache location must be an absolute path: relative/path"):
        StorageClientConfig.from_dict(config_dict, "test")


def test_profile_name_with_underscore() -> None:
    """Test that profile names cannot start with an underscore."""
    with pytest.raises(RuntimeError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              _invalid_profile:
                storage_provider:
                  type: file
                  options:
                    base_path: /invalid_path
            """
        )

    assert "Failed to validate the config file" in str(e.value)


def test_path_mapping_section() -> None:
    """Test loading path_mapping section correctly."""
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
        path_mapping:
          /data/datasets/: msc://default/
          https://example.com/data/: msc://default/
        """
    )

    assert config and config._config_dict
    assert config._config_dict["path_mapping"] == {
        "/data/datasets/": "msc://default/",
        "https://example.com/data/": "msc://default/",
    }


def test_find_config_file_paths():
    os.environ["HOME"] = "/home/testuser"

    paths = _find_config_file_paths()

    expected_paths = (
        "/home/testuser/.msc_config.yaml",
        "/home/testuser/.msc_config.json",
        "/home/testuser/.config/msc/config.yaml",
        "/home/testuser/.config/msc/config.json",
        "/etc/xdg/msc/config.yaml",
        "/etc/xdg/msc/config.json",
        "/etc/msc_config.yaml",
        "/etc/msc_config.json",
    )

    assert paths == expected_paths

    os.environ["XDG_CONFIG_HOME"] = "/custom/config"
    os.environ["XDG_CONFIG_DIRS"] = "/opt/config:/usr/local/etc"

    paths = _find_config_file_paths()

    expected_paths = (
        "/custom/config/msc/config.yaml",
        "/custom/config/msc/config.json",
        "/home/testuser/.msc_config.yaml",
        "/home/testuser/.msc_config.json",
        "/home/testuser/.config/msc/config.yaml",
        "/home/testuser/.config/msc/config.json",
        "/opt/config/msc/config.yaml",
        "/opt/config/msc/config.json",
        "/usr/local/etc/msc/config.yaml",
        "/usr/local/etc/msc/config.json",
        "/etc/msc_config.yaml",
        "/etc/msc_config.json",
    )

    assert paths == expected_paths


def test_s3_storage_provider_with_rust_client() -> None:
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          s3_profile_with_rust:
            storage_provider:
              type: s3
              options:
                base_path: bucket
                region_name: us-west-2
                endpoint_url: http://localhost:10000
                rust_client: 
                  allow_http: true
            credentials_provider:
              type: S3Credentials
              options:
                access_key: my_key
                secret_key: my_secret
        """,
        profile="s3_profile_with_rust",
    )
    assert isinstance(config.storage_provider, S3StorageProvider)
    storage_provider = cast(S3StorageProvider, config.storage_provider)
    assert storage_provider._rust_client is not None

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          swiftstack_profile_with_rust:
            storage_provider:
              type: s8k
              options:
                base_path: bucket
                endpoint_url: https://pdx.s8k.io
                rust_client: {}
        """,
        profile="swiftstack_profile_with_rust",
    )
    assert isinstance(config.storage_provider, S3StorageProvider)
    storage_provider = cast(S3StorageProvider, config.storage_provider)
    assert storage_provider._rust_client is not None


def test_cache_backend_cache_path():
    """Test that cache_backend.cache_path is used as cache location when location is not defined."""
    config_dict = {
        "profiles": {
            "test": {
                "storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}},
            }
        },
        "cache": {"size": "100M", "cache_backend": {"cache_path": "/tmp/new_cache_path"}},
    }

    config = StorageClientConfig.from_dict(config_dict, "test")
    assert config.cache_config is not None
    assert config.cache_config.location == "/tmp/new_cache_path"


def test_cache_backend_cache_path_without_location():
    """Test that cache_backend.cache_path works even when location is not specified."""
    config_dict = {
        "profiles": {
            "test": {
                "storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}},
            }
        },
        "cache": {"size": "100M", "cache_backend": {"cache_path": "/tmp/cache_from_backend"}},
    }

    config = StorageClientConfig.from_dict(config_dict, "test")
    assert config.cache_config is not None
    assert config.cache_config.location == "/tmp/cache_from_backend"


def test_cache_location_precedence():
    """Test that location takes precedence over cache_backend.cache_path when both are defined."""
    config_dict = {
        "profiles": {
            "test": {
                "storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}},
            }
        },
        "cache": {
            "size": "100M",
            "location": "/tmp/location_path",
            "cache_backend": {"cache_path": "/tmp/backend_path"},
        },
    }

    config = StorageClientConfig.from_dict(config_dict, "test")
    assert config.cache_config is not None
    # location should take precedence over cache_backend.cache_path
    assert config.cache_config.location == "/tmp/location_path"


def test_cache_location_warning(caplog):
    """Test that a warning is logged when both location and cache_backend.cache_path are defined."""
    config_dict = {
        "profiles": {
            "test": {
                "storage_provider": {"type": "file", "options": {"base_path": "/tmp/test_storage"}},
            }
        },
        "cache": {
            "size": "100M",
            "location": "/tmp/location_path",
            "cache_backend": {"cache_path": "/tmp/backend_path"},
        },
    }

    config = StorageClientConfig.from_dict(config_dict, "test")
    assert config.cache_config is not None
    assert config.cache_config.location == "/tmp/location_path"

    # Check that warning was logged
    warning_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
    assert len(warning_messages) > 0
    assert any("Both 'location' and 'cache_backend.cache_path' are defined" in msg for msg in warning_messages)
