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
    uv sync --python {{python-binary}} --all-extras
    # Create the dependency license summary.
    uv run pip-licenses

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
    if [[ -z "${CI:-}" ]]; then ruff check --fix; else ruff check --output-format gitlab --output-file .reports/ruff.json; fi
    # Type check.
    uv run pyright

# Stop storage systems.
stop-storage-systems:
    # Stop storage systems.
    #
    # Azurite's process commands are `node` instead of `azurite`. Find by port instead.
    for PID in $(lsof -i :10000-10002 -c fake-gcs-server -c minio -t); do kill $PID; done
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
    timeout 10s bash -c "until netcat --zero localhost 10000; do sleep 1; done"
    # Wait for fake-gcs-server.
    timeout 10s bash -c "until curl --fail --output /dev/null --silent http://localhost:4443/_internal/healthcheck; do sleep 1; done"
    # Wait for MinIO.
    timeout 10s bash -c "until curl --fail --output /dev/null --silent http://localhost:9000/minio/health/live; do sleep 1; done"

# Stop telemetry systems.
stop-telemetry-systems:
    # Stop telemetry systems.
    for PID in $(lsof -c grafana -c mimir -c tempo -t); do kill $PID; done
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
    ln -s $(dirname $(dirname $(command -v grafana)))/share/grafana/{conf,public} .grafana/sandbox
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
    timeout 10s bash -c "until curl --fail --output /dev/null --silent http://localhost:3000/api/health; do sleep 1; done"
    # Wait for Mimir.
    timeout 30s bash -c "until curl --fail --output /dev/null --silent http://localhost:8080/ready; do sleep 1; done"
    # Wait for Tempo.
    timeout 30s bash -c "until curl --fail --output /dev/null --silent http://localhost:8081/ready; do sleep 1; done"

# Run unit tests.
run-unit-tests: prepare-virtual-environment start-storage-systems && stop-storage-systems
    # Remove test artifacts.
    rm -rf .reports/unit
    # Unit test.
    #
    # The CI/CD runner setup only allows 4 cores per job, so using 1 parent + 3 child processes.
    uv run pytest --junit-xml .reports/unit/pytest.xml --cov --cov-report term --cov-report html --cov-report xml --durations 0 --durations-min 10 --numprocesses 2

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
    uv run pytest --junit-xml .reports/e2e/pytest.xml tests/test_multistorageclient/e2e --durations 0 --durations-min 60 --numprocesses 4
