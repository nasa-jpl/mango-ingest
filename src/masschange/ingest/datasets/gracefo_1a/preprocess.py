# Apply preprocessing to imported/ingested data - typically this is decimation
import argparse
import os

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

    #### EXTRACT TO DATASET-SPECIFIC CLASS/OBJECT ####
    # TODO: clarify the idea of dataset vs dataset subset - it's really murky at the moment
    satellite_ids = {1, 2}
    dataset_subset_root_paths = [os.path.join(dataset_root_path, f'satellite_id={id}') for id in satellite_ids]
    base_hours_per_partition = 24
    # factor to decimate by in each step. yields results in 1:20, 1:400, 1:8000, 1:24000
    decimation_step_factors = [20, 20, 20, 3]
    partition_epoch_offset_hours = 12
    #### END EXTRACT TO DATASET-SPECIFIC CLASS/OBJECT ####

    for dataset_subset_path in dataset_subset_root_paths:
        try:
            process(decimation_step_factors, base_hours_per_partition, partition_epoch_offset_hours, dataset_subset_path)
        except Exception as err:
            log.exception(f'preprocess.py failed unexpectedly with {err}')
            exit(1)
