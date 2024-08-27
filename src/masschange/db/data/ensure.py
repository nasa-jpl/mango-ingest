import psycopg2

from masschange.dataproducts.db.utils import get_db_connection
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.db.data.caggs import get_extant_continuous_aggregates, delete_caggs, \
    get_continuous_aggregate_create_statements, refresh_continuous_aggregates
from masschange.db.ensure import log


def ensure_dataset_table_exists(dataset: TimeSeriesDataset) -> None:
    """
    Ensure that the table for this dataset exists, creating and configuring the table if it does not.
    """
    table_name = dataset.get_table_name()
    log.info(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset.product.TIMESTAMP_COLUMN_NAME
    with get_db_connection() as conn, conn.cursor() as cur:
        try:
            sql = f"""
            {dataset.get_sql_table_create_statement()}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            select set_chunk_time_interval('{table_name}', interval '24 hours');
            """
            cur.execute(sql)
            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


def ensure_dataset_caggs_exist(dataset: TimeSeriesDataset) -> None:
    """
    Ensure that the table for this dataset and instrument_id's data exists, creating the table and all necessary views if
    the table doesn't exist.  Does not check for or fix partial existence (i.e. table exists but views do not).
    """
    log.info(f'Ensuring expected continuous aggregates exist for dataset "{dataset.product.get_full_id()}"')

    expected_dataset_caggs = {dataset.get_table_or_view_name(level) for level in
                              dataset.product.get_available_aggregation_levels()}
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
            with get_db_connection() as conn, conn.cursor() as cur:
                sql = '\n'.join(cagg_create_statements)
                cur.execute(sql)
                conn.commit()
                log.info(
                    f'Created continous aggregates for dataset "{dataset.product.get_full_id()}", version "{str(dataset.version)}", instruments "{dataset.instrument_id}"')

            refresh_continuous_aggregates(dataset, enable_chunking=True)
