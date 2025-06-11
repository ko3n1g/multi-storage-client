#
# Just configuration.
#
# https://just.systems/man/en
#

# Default to the first Python binary on `PATH`.
python-binary := "python"

# List recipes.
help:
    just --list

# Prepare the virtual environment.
prepare-virtual-environment:
    # Prepare the virtual environment.
    if [[ -z "${CI:-}" ]]; then uv sync --all-extras --python {{python-binary}}; else uv sync --all-extras --locked --python {{python-binary}}; fi

# Start the Python REPL.
start-repl: prepare-virtual-environment
    # Start the Python REPL.
    uv run python

# Run static analysis (format, lint, type check).
analyze: prepare-virtual-environment
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
run-unit-tests: prepare-virtual-environment start-storage-systems && stop-storage-systems
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
run-load-tests: prepare-virtual-environment start-storage-systems && stop-storage-systems
    # Load test.
    uv run pytest --minutes 30 tests/test_multistorageclient/load/local.py

# Create package archives.
package: prepare-virtual-environment
    # Remove package archives.
    rm -rf dist
    # Create package archives.
    uv build

# Build the documentation.
document: prepare-virtual-environment
    # Remove documentation artifacts.
    rm -rf docs/dist
    # Build the documentation website.
    uv run sphinx-build -b html docs/src docs/dist

# Release build.
build: analyze run-unit-tests package document

# Run E2E tests.
run-e2e-tests: prepare-virtual-environment
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
