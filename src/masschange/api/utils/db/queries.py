import logging
from collections.abc import Collection
from typing import Dict, Set

import psycopg2

from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.dataproductfield import DataProductField
from masschange.dataproducts.utils import\
    get_dataproducts
from masschange.db.conn import get_db_cursor


def fetch_dataset_bulk_metadata() -> Dict[str, Dict]:
    # TODO: incorporate CachedDatasetMetadata objects in output rather than using dicts
    """
    Normally, fetching dataset metadata requires one query per dataset, but that is unnecessarily slow if many
    datasets are desired.

    This optimized query returns metadata dicts for all datasets, keyed by dataset full-id

    """
    supported_properties = {'data_begin', 'data_end', 'last_updated'}

    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
SELECT mdp.name as product_id, mdv.name as version_id, mi.name as instrument_id, {','.join(sorted(supported_properties))}
FROM _meta_dataproducts_versions_instruments as mdpvi
JOIN _meta_dataproducts_versions mdv on mdv.id = mdpvi._meta_dataproducts_versions_id
JOIN _meta_dataproducts mdp on mdp.id = mdv._meta_dataproducts_id
JOIN _meta_instruments mi on mi.id = mdpvi._meta_instruments_id
                        """
            cur.execute(sql)
            results = cur.fetchall()
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            return {}
    return {f"{row['product_id']}_{row['version_id']}_{row['instrument_id']}": row for row in results}


def fetch_bulk_channel_id_enums() -> Dict[DataProduct, Dict[DataProductField, Collection[str]]]:
    # TODO: incorporate CachedDatasetMetadata objects in output rather than using dicts, and rename method to represent all product-level metadata

    """
    Normally, fetching channel-id enum values requires one query per product, but that is unnecessarily slow if many
    products are desired.

    This optimized query returns channel-id enum value dicts for all products, keyed by product full-id, then field name
    """

    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
    SELECT mdp.name as product_id, field_name, value
    FROM _meta_dataproducts_channelidvalues as civ
        JOIN _meta_dataproducts mdp on mdp.id = civ.dataproduct_id
                            """
            cur.execute(sql)
            result_rows = cur.fetchall()
        except Exception as err:
            logging.error(f'query failed with {err}: {sql}')
            raise err

    results = {}
    products_by_id = {p.get_full_id(): {
        'product': p,
        'fields': p.get_available_fields()
    } for p in get_dataproducts()}


    # Initialise the results structure - this is necessary to ensure null-sets are created for products with no ingested
    # data (and thus, no enum value rows in the metadata table)
    for product_info in products_by_id.values():
        product = product_info['product']
        product_fields = product_info['fields']
        channel_id_fields = [f for f in product_fields if f.is_channel_id_column]
        results[product] = {}
        for field in channel_id_fields:
            results[product][field] = set()

    for row in result_rows:
        product_id = row['product_id']
        product_info = products_by_id[product_id]
        product = product_info['product']
        product_fields = product_info['fields']
        field_name = row['field_name']
        field = next(f for f in product_fields if f.name == field_name)
        value = row['value']

        channel_id_enums: Dict[DataProductField, Set[str]] = results[product]
        field_enum_values: Set[str] = channel_id_enums[field]
        field_enum_values.add(value)

    # Sort the enums for clean presentation in API response

    for product in results.keys():
        for field in results[product].keys():
            results[product][field] = sorted(results[product][field])
    return results
