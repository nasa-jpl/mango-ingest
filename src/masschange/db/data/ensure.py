import logging
import math

import psycopg2

from masschange.db.conn import get_db_cursor
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.dataset import Dataset
from masschange.db.data.caggs import get_extant_continuous_aggregates, delete_caggs, \
    get_continuous_aggregate_create_statements, refresh_continuous_aggregates

log = logging.getLogger()


def ensure_dataset_table_exists(dataset: Dataset) -> None:
    """
    Ensure that the table for this dataset exists, creating and configuring the table if it does not.
    """
    table_name = dataset.get_table_name()
    log.debug(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset.product.TIMESTAMP_COLUMN_NAME
    with get_db_cursor() as cur:
        try:
            sql = f"""
            {dataset.get_sql_table_create_statement()}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            """
            cur.execute(sql)
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass

    if dataset.is_time_series_dataset():
        chunk_time_interval_hours = math.ceil(dataset.product.get_chunk_time_interval().total_seconds() / 3600)

        with get_db_cursor() as cur:
            cur.execute(f"""select set_chunk_time_interval('{table_name}', interval '{chunk_time_interval_hours} hours');""")
            log.info(f'Set hypertable "{table_name}" chunk_time_interval to {chunk_time_interval_hours}hrs')

def ensure_dataset_caggs_exist(dataset: TimeSeriesDataset) -> None:
    """
    Ensure that the table for this dataset and instrument_id's data exists, creating the table and all necessary views if
    the table doesn't exist.  Does not check for or fix partial existence (i.e. table exists but views do not).
    """
    log.info(f'Ensuring expected continuous aggregates exist for dataset "{dataset.product.get_full_id()}"')

    expected_dataset_caggs = {dataset.get_table_or_view_name(level) for level in
                              dataset.product.get_available_aggregation_levels()}

    if dataset.product.get_full_id() == 'GRACEFO_OFFRED':
        extra_caggs = set(f'{base_cagg}{type_ext}' for base_cagg in expected_dataset_caggs for type_ext in ['float', 'int'])
        expected_dataset_caggs.update(extra_caggs)

    extant_dataset_caggs = get_extant_continuous_aggregates(dataset)

    if expected_dataset_caggs != extant_dataset_caggs:
        if expected_dataset_caggs.issubset(extant_dataset_caggs):
            extraneous_caggs = extant_dataset_caggs.difference(expected_dataset_caggs)
            log.info(f'Extraneous caggs found - deleting {sorted(extraneous_caggs)}')
            delete_caggs(extraneous_caggs)
        else:
            log.info(
                f'Regenerating all dataset caggs due to mismatch between expected/extant caggs (expected {sorted(expected_dataset_caggs)}, '
                f'got {sorted(extant_dataset_caggs)})')

            delete_caggs(extant_dataset_caggs)

            cagg_create_statements = [
                get_continuous_aggregate_create_statements(dataset, agg_level) for
                agg_level in
                dataset.product.get_available_aggregation_levels()]
            with get_db_cursor() as cur:
                sql = '\n'.join(cagg_create_statements)
                cur.execute(sql)
                log.info(
                    f'Created continous aggregates for dataset "{dataset.product.get_full_id()}", version "{str(dataset.version)}", instruments "{dataset.instrument_id}"')

            try:
                refresh_continuous_aggregates(dataset, enable_chunking=True)
            except Exception as err:
                log.error(f'Failed to refresh continuous aggregates for dataset {dataset.get_table_name()}: {err.__class__}: {err}')
