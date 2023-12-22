# Apply preprocessing to imported/ingested data - typically this is decimation
import argparse
import os

from masschange.missions.datasets import GraceFO1ADataset
from masschange.ingest.utils import get_configured_logger
from masschange.ingest.utils.decimation import process

log = get_configured_logger()


def get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog='MassChange GRACE-FO_1A Data Preprocessor',
        description='Given GRACE-FO_1A data ingested into parquet, produce derived data (e.g. decimated data)'
    )

    ap.add_argument('data_root', help='the parquet root path for the GRACE-FO_1A data')

    return ap.parse_args()


if __name__ == '__main__':

    args = get_args()
    dataset_root_path = args.data_root
    # dataset_root_path = '/nomount/masschange/data_volume_mount/gracefo_1a/'
    log.info(f'preprocessing GRACE-FO_1A data under {dataset_root_path}')
    config = GraceFO1ADataset.get_config()
    dataset_subset_root_paths = [os.path.join(dataset_root_path, f'satellite_id={id}') for id in config.stream_ids]

    for dataset_subset_path in dataset_subset_root_paths:
        try:
            process(config.decimation_step_factors, config.base_hours_per_partition, config.partition_epoch_offset_hours, dataset_subset_path)
        except Exception as err:
            log.exception(f'preprocess.py failed unexpectedly with {err}')
            exit(1)
