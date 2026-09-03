#!/bin/bash
set -euo pipefail

# This script is intended to be used as the common entrypoint for all service containers except the database.

# Create convenience symlinks to the data directory, if the relevant
# runtime env vars are populated.
# This provides the ability to selectively mount only the data subdirectories that are needed for a given service,
# while avoiding hardcoding the subpaths and (crucially) avoiding splitting the input/staging mount in a way that
# prevents the crawler from using os.rename().
if [[ -n "${DATA_ROOT:-}" && -n "${SOURCE_DATA_ROOT_SUBPATH:-}" ]]; then
  ln -sfn "${DATA_ROOT}/${SOURCE_DATA_ROOT_SUBPATH}" /input
fi

if [[ -n "${DATA_ROOT:-}" && -n "${STAGED_DATA_ROOT_SUBPATH:-}" ]]; then
  ln -sfn "${DATA_ROOT}/${STAGED_DATA_ROOT_SUBPATH}" /staging
fi

# exec (not just run) so conda run's process replaces this shell as PID 1's
# child — ensures SIGTERM/SIGINT from `docker stop` / `compose down` reach
# the actual application process directly, rather than timing out and
# escalating to SIGKILL against this wrapper script.
exec conda run --no-capture-output -n masschange "$@"