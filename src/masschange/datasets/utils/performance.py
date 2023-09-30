import logging
import os
import shutil
import tempfile
from tempfile import mkdtemp
from typing import Sequence, Dict, List

from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY


def get_prepruned_parquet_path(partition_values: Sequence[str], src_parquet_root: str,
                               partition_key: str = PARQUET_TEMPORAL_PARTITION_KEY):
    """
    Given a top-level partition key and a sequence of match values, create a pseudo-index by taking all the matches
    and symlinking to them under a temp directory.  O(n) with the number of match values, rather than the number of
    extant files in the whole parquet dataset.
    """
    temporary_index_dir = mkdtemp(prefix='gma-index-')
    logging.debug(f'creating pseudo-index at {temporary_index_dir}')
    for value in partition_values:
        subtree_name = f'{partition_key}={value}'
        src = os.path.join(src_parquet_root, subtree_name)
        dest = os.path.join(temporary_index_dir, subtree_name)
        if os.path.exists(src):
            logging.debug(f'adding path {src} to pseudo-index')
            os.symlink(src, dest, target_is_directory=True)

    return temporary_index_dir

def get_prepruned_parquet_path_multilevel(src_parquet_root: str, partition_key_hierarchy: Sequence[str], partition_value_filters: Dict[str, List[str | int]]) -> str:
    """
    Similar behaviour to get_prepruned_parquet_path(), but supports matching at partitioning levels.

    ex. Given the following partitioning structure and desiring data from satellite with id=1, with decimation ratio
    1:20 and for temporal_partition_keys [600, 1200, 1800]:

        parquet_root/satellite_id=<value>/decimation_factor=<value>/temporal_partition_key=<value>

    Callers would supply:
        partition_key_hierarchy = ['satellite_id', 'decimation_factor', 'temporal_partition_key']
        desired_partition_values = {
          'satellite_id': ['1'],
          'decimation_factor': ['20'],
          'temporal_partition_key': ['600', '1200', '1800']
        }

    N.B.
     - partition_key_hierarchy is ordered and must provide a complete path from route to the last key on which to filter
     - partition_key_hierarchy must be equivalent to partition_value_filters.keys()
    """

    # check for coherence of partition_key_hierarchy and partition_value_filters
    if set(partition_key_hierarchy) != set(partition_value_filters.keys()):
        raise ValueError(f'partition_key_hierarchy and keys of partition_value_filters must be equivalent (got {sorted(partition_key_hierarchy)} and {sorted(partition_value_filters.keys())})')


    temporary_index_dir = mkdtemp(prefix='gma-index-')
    logging.debug(f'creating pseudo-index at {temporary_index_dir}')

    # generate all relative path permutations
    paths = ['.']
    for key, values in partition_value_filters.items():

        # Check
        prefix_paths = paths
        paths = []
        for prefix_path in prefix_paths:
            for value in values:
                paths.append(os.path.join(prefix_path, f'{key}={value}'))

    for path in paths:
        src = os.path.abspath(os.path.join(src_parquet_root, path))
        dest = os.path.abspath(os.path.join(temporary_index_dir, path))
        if os.path.exists(src):
            dest_parent_dir = os.path.split(dest)[0]
            os.makedirs(dest_parent_dir, exist_ok=True)
            logging.debug(f'adding path {src} to pseudo-index')
            os.symlink(src, dest, target_is_directory=True)

    return temporary_index_dir



def safely_remove_temporary_index(index_path: str):
    """Safely remove a directory full of symlinks, without touching the destinations"""
    # todo: find out if this is actually necessary, or if shutil.rmtree is safe to use here
    
    # Guard against deletion of non-temp dirs
    if not index_path.startswith(tempfile.gettempdir()):
        logging.warning(f'failed to remove pseudo-index at {index_path} - not a temp directory')
        return

    logging.debug(f'removing pseudo-index at {index_path}')

    for fn in os.scandir(index_path):
        fp = os.path.join(index_path, fn)
        os.unlink(fp)

    os.rmdir(index_path)


def safely_remove_multilevel_temporary_index(index_path: str):
    """Safely remove a directory full of symlinks, without touching the destinations"""
    # todo: find out if this is actually necessary, or if shutil.rmtree is safe to use here

    # Guard against deletion of non-temp dirs
    if not index_path.startswith(tempfile.gettempdir()):
        logging.warning(f'failed to remove pseudo-index at {index_path} - not a temp directory')
        return

    logging.debug(f'removing pseudo-index at {index_path}')

    # remove all symlinks in the index dir tree
    for root, _, fns in os.walk(index_path, followlinks=False):
        for fn in fns:
            target = os.path.join(root, fn)
            if os.path.islink(target):
                os.unlink(target)

    # then remove the tree itself
    shutil.rmtree(index_path)