######################
Command-Line Interface
######################

After installing the ``multi-storage-client`` package (see :doc:`installation`), you can use the ``msc`` command to interact with your storage services.

Below are the available sub-commands under ``msc``.

********
msc help
********

The ``msc help`` command displays general help information and available commands. It can also be used to display help for a specific command.

.. code-block:: text
   :caption: General help output

   $ msc help
   usage: msc <command> [options] [parameters]
   To see help text, you can run:

   msc help
   msc help <command>

   commands:
   glob     Find files using Unix-style wildcard patterns with optional attribute filtering
   help     Display help for commands
   ls       List files and directories with optional attribute filtering
   rm       Delete files with a given prefix
   sync     Synchronize files from the source storage to the target storage


.. code-block:: text
  :caption: Command-specific help output

  $ msc help ls
  usage: msc ls [--attribute-filter-expression ATTRIBUTE_FILTER_EXPRESSION] [--recursive] [--human-readable] [--summarize] [--debug] [--limit LIMIT] [--show-attributes] path

  List files and directories at the specified path. Supports:
    1. Simple directory listings
    2. Attribute filtering
    3. Human readable sizes
    4. Summary information
    5. Metadata attributes display

  positional arguments:
    path                  The path to list (POSIX path or msc:// URL)

  options:
    --attribute-filter-expression ATTRIBUTE_FILTER_EXPRESSION, -e ATTRIBUTE_FILTER_EXPRESSION
                          Filter by attributes using a filter expression (e.g., 'model_name = "gpt" AND version > 1.0')
    --recursive           List contents recursively (default: list only first level)
    --human-readable      Displays file sizes in human readable format
    --summarize           Displays summary information (number of objects, total size)
    --debug               Enable debug output
    --limit LIMIT         Limit the number of results to display
    --show-attributes     Display metadata attributes dictionary as an additional column

  examples:
    # Basic directory listing
    msc ls "msc://profile/data/"
    msc ls "/path/to/files/"

    # Human readable sizes
    msc ls "msc://profile/models/" --human-readable

    # Show summary information
    msc ls "msc://profile/data/" --summarize

    # List with attribute filtering
    msc ls "msc://profile/models/" --attribute-filter-expression 'model_name = "gpt"'
    msc ls "msc://profile/data/" --attribute-filter-expression 'version >= 1.0 AND environment != "test"'

    # Limited results
    msc ls "msc://profile/data/" --limit 10

    # List contents recursively
    msc ls "msc://profile/data/" --recursive

    # Show metadata attributes
    msc ls "msc://profile/models/" --show-attributes
    msc ls "msc://profile/data/" --show-attributes --human-readable


******
msc ls
******

The ``msc ls`` command lists files and directories in a storage service. It supports various options for filtering and displaying the results.

.. code-block:: text
  :caption: List files

  $ msc ls msc://profile/data/
  Last Modified           Size  Name
  2025-04-15 00:22:40  5242880  msc://profile/data/data-5MB.bin
  2025-04-15 00:23:36     1496  msc://profile/data/model.pt

.. note::
   The ``--attribute-filter-expression`` option allows you to filter files based on their metadata attributes.

   **Supported Operators:**
     - Equality: ``=``, ``!=``
     - Comparison: ``>``, ``>=``, ``<``, ``<=``
     - Logical: ``AND``, ``OR``
     - Grouping: ``()``

   **Examples:**
     - ``model_name = "gpt"`` - Find files with model_name attribute equal to "gpt"
     - ``version >= 1.0`` - Find files with version 1.0 or higher
     - ``environment != "test"`` - Find files not in test environment
     - ``(model_name = "gpt" OR model_name = "bert") AND version > 1.0`` - Complex filter with logical operators

   **Numeric vs String Comparison:** For comparison operators (``>``, ``>=``, ``<``, ``<=``), the system first attempts numeric comparison. If that fails, it falls back to lexicographic string comparison.

   **Performance Considerations:** When using attribute filtering, the system makes additional HEAD requests to retrieve metadata for each file. This can increase latency, especially when working with many files.


********
msc glob
********

The ``msc glob`` command finds files in a storage service using Unix-style wildcard patterns.

.. code-block:: text
  :caption: Find files with a wildcard pattern

  $ msc glob "msc://profile/data/*.pt"
  msc://profile/data/model.pt

.. note::
   The ``msc glob`` command works by first listing all files in the specified directory using the equivalent of ``msc ls``, then applying the glob pattern as a post-filter to the results. This means that glob patterns are evaluated locally after retrieving the file listing from the storage service.


******
msc rm
******

The ``msc rm`` command deletes files in a storage service. It supports recursively deleting directories.

.. code-block:: text
  :caption: Delete files in dryrun mode

  $ msc rm --dryrun msc://profile/data
  
  Files that would be deleted:
    msc://profile/data/data-5MB.bin
    msc://profile/data/model.pt

  Total: 2 file(s)


********
msc sync
********

The ``msc sync`` command synchronizes files between storage locations. It can be used to upload files from the filesystem to object storage, download files from object storage to the filesystem, or transfer files between different object storage locations.

The sync operation compares files between source and target locations using metadata (etag, size, modification time) to determine if files need to be copied. Files are processed in parallel using multiple worker processes and threads for optimal performance.

.. code-block:: shell
  :caption: Basic sync usage

  $ msc sync msc://profile/data/ --target-url /path/to/local/dataset/

Upload files from the filesystem to object storage:

.. code-block:: shell

  $ msc sync /path/to/dataset --target-url msc://profile/prefix

Download files from object storage to the filesystem:

.. code-block:: shell

  $ msc sync msc://profile/prefix --target-url /path/to/dataset

Transfer files between different object storage locations:

.. code-block:: shell

  $ msc sync msc://profile1/prefix --target-url msc://profile2/prefix

Sync with cleanup (removes files in target not in source):

.. code-block:: shell

  $ msc sync msc://source-profile/data --target-url msc://target-profile/data --delete-unmatched-files

The sync operation uses a parallel processing architecture with producer/consumer threads and multiple worker processes to maximize throughput. It efficiently compares files using metadata and only transfers files that have changed or are missing.

For large files, the sync operation uses temporary files to avoid loading entire files into memory. Smaller files are transferred directly in memory for better performance.

.. note::
   The sync operation automatically handles metadata updates for the target storage client.


Fine-tuning Parallelism
=======================

MSC automatically determines optimal parallelism based on your system's CPU count, but you can fine-tune it using environment variables.

.. code-block:: shell
   :caption: Environment variables for parallelism

   # Set number of worker processes (default: min(8, CPU_count))
   $ export MSC_NUM_PROCESSES=4

   # Set threads per process (default: max(16, CPU_count/processes))
   $ export MSC_NUM_THREADS_PER_PROCESS=8

   # Run sync with custom parallelism
   $ msc sync msc://source-profile/data --target-url msc://target-profile/data

.. note::
  MSC uses a **producer-consumer pattern** with **multiprocessing** and **multithreading** to maximize throughput:

  1. **Producer Thread**: Compares source and target files, queues sync operations
  2. **Worker Processes**: Multiple processes handle file transfers (multiprocessing bypasses Python's GIL)
  3. **Worker Threads**: Each process spawns multiple threads for concurrent I/O operations
  4. **Consumer Thread**: Collects results and updates progress


Ray Integration
===============

MSC provides integration with `Ray <https://ray.io/>`_ for distributed computing capabilities, enabling you to scale sync operations across multiple machines in a cluster. This is particularly useful for large-scale data transfers that require significant computational resources.

**Prerequisites:**
   - Ray must be installed: ``pip install "multi-storage-client[ray]"``
   - A Ray cluster must be running and accessible

**Benefits of Ray Integration:**
   - **Distributed Processing:** Scale sync operations across multiple machines
   - **Fault Tolerance:** Ray provides automatic task retry and failure recovery
   - **Resource Management:** Efficient utilization of cluster resources
   - **Scalability:** Handle larger datasets by distributing work across nodes

**Usage:**

To use Ray for distributed sync operations, specify the Ray cluster address using the ``--ray-cluster`` option:

.. code-block:: shell
   :caption: Sync with Ray cluster

   # Start a local Ray cluster
   $ ray start --head --port=6379

   # Connect to a local Ray cluster
   $ msc sync msc://source-profile/data --ray-cluster 127.0.0.1:6379 --target-url msc://target-profile/data
