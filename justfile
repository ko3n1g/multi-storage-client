#
# Just configuration.
#
# https://just.systems/man/en
#

# Compiler targets.
compiler-targets := "aarch64-apple-darwin aarch64-unknown-linux-gnu x86_64-apple-darwin x86_64-unknown-linux-gnu"

# Default to the first Python binary on `PATH`.
python-binary := "python"

# List recipes.
help:
    just --list

# Prepare the toolchain.
prepare-toolchain:
    # Add compiler targets.
    rustup target add {{compiler-targets}}
    # Prepare the virtual environment.
    if [[ -z "${CI:-}" ]]; then uv sync --all-extras --python {{python-binary}}; else uv sync --all-extras --locked --python {{python-binary}}; fi

# Start the Python REPL.
start-repl: prepare-toolchain
    # Start the Python REPL.
    uv run python

# Run static analysis (format, lint, type check).
analyze: prepare-toolchain
    # Remove analysis artifacts.
    rm -rf .reports/ruff.json
    # Format.
    if [[ -z "${CI:-}" ]]; then ruff format; else ruff format --check; fi
    # Lint.
    if [[ -z "${CI:-}" ]]; then ruff check --fix; else ruff check --output-file .reports/ruff.json --output-format gitlab; fi
    # Type check.
    uv run pyright

# Stop storage systems.
stop-storage-systems:
    # Stop storage systems.
    #
    # Azurite's process commands are `node` instead of `azurite`. Find by port instead.
    for PID in $(lsof -i :10000-10002 -c fake-gcs-server -c minio -t); do kill -s KILL $PID; done
    # Remove sandbox directories.
    -rm -rf .{azurite,fake-gcs-server,minio}/sandbox

# Start storage systems.
start-storage-systems: stop-storage-systems
    # Create sandbox directories.
    mkdir --parents .{azurite,fake-gcs-server,minio}/sandbox

    # Ports used by storage systems:
    # - Azurite         -> 10000-10002
    # - fake-gcs-server -> 4443
    # - MinIO           -> 9000

    # Start Azurite.
    cd .azurite/sandbox && azurite --inMemoryPersistence --silent --skipApiVersionCheck &
    # Start fake-gcs-server.
    cd .fake-gcs-server/sandbox && TZ="UTC" fake-gcs-server -backend memory -log-level error -scheme http &
    # Start MinIO.
    cd .minio/sandbox && minio server --config ../minio.yaml --quiet &

    # Wait for Azurite.
    timeout 30s bash -c "until netcat --zero localhost 10000; do sleep 1; done"
    # Wait for fake-gcs-server.
    timeout 30s bash -c "until curl --fail --output /dev/null --silent http://localhost:4443/_internal/healthcheck; do sleep 1; done"
    # Wait for MinIO.
    timeout 30s bash -c "until curl --fail --output /dev/null --silent http://localhost:9000/minio/health/live; do sleep 1; done"

# Stop telemetry systems.
stop-telemetry-systems:
    # Stop telemetry systems.
    for PID in $(lsof -c grafana -c mimir -c tempo -t); do kill -s KILL $PID; done
    # Remove sandbox directories.
    -rm -rf .{grafana,mimir,tempo}/sandbox

# Start telemetry systems.
start-telemetry-systems: stop-telemetry-systems
    # Create sandbox directories.
    mkdir --parents .{grafana,mimir,tempo}/sandbox
    # Set up Grafana sandbox directory.
    #
    # Grafana expects some included data files to be present.
    mkdir --parents .grafana/sandbox/{data,logs,plugins}
    ln --symbolic $(dirname $(dirname $(command -v grafana)))/share/grafana/{conf,public} .grafana/sandbox
    # Set up Tempo sandbox directory.
    mkdir --parents .tempo/sandbox/{trace,wal}

    # Ports used by telemetry systems:
    # - Grafana -> 3000 (HTTP), Random (gRPC)
    # - Mimir   -> 7946 (Gossip), 8080 (HTTP), 9095 (gRPC)
    # - Tempo   -> 8081 (HTTP), 9096 (gRPC)

    # Start Grafana.
    cd .grafana/sandbox && grafana server --config ../grafana.ini &
    # Start Mimir.
    cd .mimir/sandbox && mimir -config.file ../mimir.yaml &
    # Start Tempo.
    cd .tempo/sandbox && tempo -config.file ../tempo.yaml &

    # Wait for Grafana.
    #
    # An error log about `stat /proc` failing is expected.
    timeout 30s bash -c "until curl --fail --output /dev/null --silent http://localhost:3000/api/health; do sleep 1; done"
    # Wait for Mimir.
    timeout 60s bash -c "until curl --fail --output /dev/null --silent http://localhost:8080/ready; do sleep 1; done"
    # Wait for Tempo.
    timeout 60s bash -c "until curl --fail --output /dev/null --silent http://localhost:8081/ready; do sleep 1; done"

# Run unit tests.
run-unit-tests: prepare-toolchain start-storage-systems && stop-storage-systems
    # Remove test artifacts.
    rm -rf .reports/unit
    # Unit test.
    if [[ -z "${CI:-}" ]]; then \
        NUMPROCESSES=auto; \
    else \
        NUMPROCESSES=0; \
    fi; \
    uv run pytest --cov --cov-report term --cov-report html --cov-report xml --durations 0 --durations-min 10 --junit-xml .reports/unit/pytest.xml --numprocesses $NUMPROCESSES

# Run load tests. For dummy load generation when experimenting with telemetry.
run-load-tests: prepare-toolchain start-storage-systems && stop-storage-systems
    # Load test.
    uv run pytest --minutes 30 tests/test_multistorageclient/load/local.py

# Create package archives.
package: prepare-toolchain
    # Remove package archives.
    rm -rf dist
    # Create a source distribution.
    uv build --sdist
    # Create platform-specific wheels.
    # Link Apple SDKs for cross-compilation, setup environment variables to correct wheel names.
    # https://github.com/rust-cross/cargo-zigbuild#caveats
    # https://github.com/PyO3/maturin/discussions/2586#discussioncomment-13095109
    # https://github.com/PyO3/maturin/blob/d95faa64f2c9971820314d228da9a7e71d2e4b87/src/build_context.rs#L1160
    for TARGET in {{compiler-targets}}; do \
        if [ "$TARGET" = "aarch64-apple-darwin" ]; then \
            MACOSX_DEPLOYMENT_TARGET=$APPLE_SDK_VERSION_AARCH64; \
            SDKROOT=$APPLE_SDK_AARCH64; \
        elif [ "$TARGET" = "x86_64-apple-darwin" ]; then \
            MACOSX_DEPLOYMENT_TARGET=$APPLE_SDK_VERSION_X86_64; \
            SDKROOT=$APPLE_SDK_X86_64; \
        else \
            MACOSX_DEPLOYMENT_TARGET=""; \
            SDKROOT=""; \
        fi; \
        env --unset _PYTHON_HOST_PLATFORM \
            MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET \
            SDKROOT=$SDKROOT \
            uv run maturin build --out dist --release --target $TARGET --zig; \
    done

# Build the documentation.
document: prepare-toolchain
    # Remove documentation artifacts.
    rm -rf docs/dist
    # Build the documentation website.
    uv run sphinx-build -b html docs/src docs/dist

# Release build.
build: analyze run-unit-tests package document

# Run E2E tests.
run-e2e-tests: prepare-toolchain
    # Remove test artifacts.
    rm -rf .reports/e2e
    # E2E test.
    #
    # The CI/CD runner setup only allows 4 cores per job.
    uv run pytest --durations 0 --durations-min 60 --junit-xml .reports/e2e/pytest.xml --numprocesses 4 tests/test_multistorageclient/e2e

# Run minimal verification without any optional dependencies.
run-minimal-verification:
    # Prepare the virtual environment without optional dependencies.
    if [[ -z "${CI:-}" ]]; then uv sync --python {{python-binary}}; else uv sync --locked --python {{python-binary}}; fi
    # Minimal verification.
    uv run pytest tests/test_multistorageclient/unit/test_minimal.py
