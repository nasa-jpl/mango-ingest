import os
import tempfile
from tempfile import mkdtemp
from typing import Sequence

from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY


def get_prepruned_parquet_path(partition_values: Sequence[str], src_parquet_root: str,
                               partition_key: str = PARQUET_TEMPORAL_PARTITION_KEY):

    temporary_index_dir = mkdtemp(prefix='gma-index-')
    for value in partition_values:
        subtree_name = f'{partition_key}={value}'
        src = os.path.join(src_parquet_root, subtree_name)
        dest = os.path.join(temporary_index_dir, subtree_name)
        os.symlink(src, dest, target_is_directory=True)

    return temporary_index_dir


def safely_remove_temporary_index(index_path: str):
    """Safely remove a directory full of symlinks, without touching the destinations"""
    # todo: find out if this is actually necessary, or if shutil.rmtree is safe to use here
    
    # Guard against deletion of non-temp dirs
    if not index_path.startswith(tempfile.gettempdir()):
        return
    
    for fn in os.scandir(index_path):
        fp = os.path.join(index_path, fn)
        os.unlink(fp)

    os.rmdir(index_path)
