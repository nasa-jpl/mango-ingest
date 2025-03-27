import logging
from collections.abc import Collection
from typing import Dict, Set

import psycopg2

from masschange.api.tests.utils import permute_all_datasets
from masschange.db.conn import get_db_cursor


def fetch_bulk_metadata():
    """
    Normally, fetching dataset metadata requires one query per dataset, but that is unnecessarily slow if many
    datasets are desired.

    This optimized query returns metadata dicts for all datasets, keyed by dataset full-id

    """
    supported_properties = {'data_begin', 'data_end', 'last_updated'}

    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
SELECT mdp.name as product, mdv.name as version, mi.name as instrument, {','.join(sorted(supported_properties))}
FROM _meta_dataproducts_versions_instruments as mdpvi
JOIN _meta_dataproducts_versions mdv on mdv.id = mdpvi._meta_dataproducts_versions_id
JOIN _meta_dataproducts mdp on mdp.id = mdv._meta_dataproducts_id
JOIN _meta_instruments mi on mi.id = mdpvi._meta_instruments_id
                        """
            cur.execute(sql)
            results = cur.fetchall()
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            return None
    return {f"{row['product']}_{row['version']}_{row['instrument']}": row for row in results}


def fetch_bulk_channel_id_enums() -> Dict[str, Dict[str, Collection[str]]]:
    """
    Normally, fetching channel-id enum values requires one query per dataset, but that is unnecessarily slow if many
    datasets are desired.

    This optimized query returns channel-id enum value dicts for all datasets, keyed by dataset full-id, then field name

    Strictly-speaking, this is partially-redundant with fetch_bulk_metadata() and could be merged, but this is only worthwhile if
    there is a tangible performance impact observed.
    """

    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
    SELECT mdp.name as product_id, mdv.name as version_id, mi.name as instrument_id, field_name, value
    FROM _meta_datasets_channelidvalues as civ
        JOIN _meta_dataproducts_versions_instruments datasets on datasets.id = civ.dataset_id
        JOIN _meta_dataproducts_versions mdv on mdv.id = datasets._meta_dataproducts_versions_id
        JOIN _meta_dataproducts mdp on mdp.id = mdv._meta_dataproducts_id
        JOIN _meta_instruments mi on datasets._meta_instruments_id = mi.id;
                            """
            cur.execute(sql)
            result_rows = cur.fetchall()
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            return None

    results = {}
    # Initialise the results structure - this is necessary to ensure null-sets are created for datasets with no ingested
    # data (and thus, no enum value rows in the metadata table)
    for dataset in permute_all_datasets():
        dataset_id = f'{dataset.product.get_full_id()}_{dataset.version}_{dataset.instrument_id}'
        channel_id_fields = [f for f in dataset.product.get_available_fields() if f.is_channel_id_column]
        results[dataset_id] = {}
        for field in channel_id_fields:
            results[dataset_id][field.name] = set()

    for row in result_rows:
        dataset_id = f'{row["product_id"]}_{row["version_id"]}_{row["instrument_id"]}'
        field_name = row['field_name']
        value = row['value']

        channel_id_enums: Dict[str, Set[str]] = results[dataset_id]
        field_enum_values: Set[str] = channel_id_enums[field_name]
        field_enum_values.add(value)

    # Sort the enums for clean presentation in API response

    for dataset_id in results.keys():
        for field_name in results[dataset_id].keys():
            results[dataset_id][field_name] = sorted(results[dataset_id][field_name])
    return results
