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

import pytest

from multistorageclient.config import STORAGE_PROVIDER_MAPPING
from multistorageclient.schema import validate_config


def test_validate_profiles():
    # Invalid: missing profiles
    with pytest.raises(RuntimeError):
        validate_config({})

    # Invalid: incorrect type
    with pytest.raises(RuntimeError):
        validate_config({"profiles": "incorrect type"})

    # Invalid: missing storage_provider
    with pytest.raises(RuntimeError):
        validate_config({"profiles": {"default": {}}})

    # Invalid: storage_provider and provider_bundle cannot exist at the same time
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": {
                        "storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}},
                        "provider_bundle": {"type": "module.MyProviderBundle", "options": {}},
                    }
                }
            }
        )

    # Valid configurations
    for provider in STORAGE_PROVIDER_MAPPING.keys():
        validate_config(
            {
                "profiles": {
                    "default": {"storage_provider": {"type": provider, "options": {"base_path": "bucket/prefix"}}}
                }
            }
        )

    # Valid configurations for s3 and swiftstack with Rust client options
    for provider in ("s3", "s8k"):
        validate_config(
            {
                "profiles": {
                    "default": {
                        "storage_provider": {
                            "type": provider,
                            "options": {
                                "base_path": "bucket/prefix",
                                "endpoint_url": "http://localhost:9000",
                                "rust_client": {
                                    "allow_http": True,
                                },
                            },
                        }
                    }
                }
            }
        )


def test_validate_cache():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Invalid: incorrect properties
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "my_prop1": False,
                    "my_prop2": "x",
                },
            }
        )

    # Valid configurations
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "cache": {"eviction_policy": {"policy": "no_eviction"}},
        }
    )

    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "cache": {
                "size": "50M",
                "use_etag": True,
                "location": "/path/to/cache",
                "eviction_policy": {
                    "policy": "FIFO",
                    "refresh_interval": 300,
                },
            },
        }
    )

    # Test invalid eviction policy format
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "eviction_policy": "no_eviction"  # String format is no longer supported
                },
            }
        )


def test_validate_opentelemetry():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Invalid: incorrect properties
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "opentelemetry": {"logs": {"exporter": {"type": "console"}}},
            }
        )

    # Valid configurations
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "opentelemetry": {"metrics": {"exporter": {"type": "console"}}},
        }
    )

    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "opentelemetry": {
                "traces": {"exporter": {"type": "otlp", "options": {"endpoint": "http://0.0.0.0:8888/otlp/v1/traces"}}}
            },
        }
    )

    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "opentelemetry": {
                "metrics": {"exporter": {"type": "console"}},
                "traces": {"exporter": {"type": "otlp", "options": {"endpoint": "http://0.0.0.0:8888/otlp/v1/traces"}}},
            },
        }
    )
