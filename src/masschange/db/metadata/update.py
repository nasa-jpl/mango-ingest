from datetime import datetime
from typing import Union

from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.db.conn import get_db_cursor
from masschange.utils.timespan import TimeSpan


def update_metadata(dataset: TimeSeriesDataset,
                    data_span: Union[TimeSpan, None] = None,
                    populate_versions=False,
                    accumulate_data_span: bool = True):
    """

    :param dataset: the dataset for which to update metadata
    :param data_span: the data span, if any, to update the data with
    :param populate_versions: not yet implemented - currently unclear what this was intended to accomplish
    :param accumulate_data_span: If True, update the metadata with the union of the provided data_span and the span
           already present in the db.  If False, overwrite the span with the provided data_span.
    :return:
    """
    if populate_versions:
        raise NotImplementedError(f'update_metadata() does not yet support populate_versions - go ahead and implement population of queries from available table names')

    # TODO: Consider cleaning up these calls - they're more readable, but the "retrieve" calls could be avoided by
    #  putting the conditions into the dataset UPDATE query. Negligible performance impact, so tabled for now.

    # Ensure product exists in db
    with get_db_cursor() as cur:
        sql = """
            INSERT INTO _meta_dataproducts
            VALUES (DEFAULT, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'name': dataset.product.get_full_id(), 'label': dataset.product.get_full_id()})

    # Ensure instrument exists in db
    with get_db_cursor() as cur:
        sql = """
            INSERT INTO _meta_instruments
            VALUES (DEFAULT, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'name': dataset.instrument_id, 'label': dataset.instrument_id})

    # Retrieve product row id
    with get_db_cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_dataproducts
            WHERE name = %(name)s;
        """
        cur.execute(sql, {'name': dataset.product.get_full_id()})
        data_product_db_id = cur.fetchone()[0]

    # Retrieve intrument row id
    with get_db_cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_instruments
            WHERE name = %(name)s;
        """
        cur.execute(sql, {'name': dataset.instrument_id})
        instrument_db_id = cur.fetchone()[0]

    # Ensure versioned product exists in db
    with get_db_cursor() as cur:
        sql = """
            INSERT INTO _meta_dataproducts_versions
            VALUES (DEFAULT, %(dataproduct)s, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'dataproduct': data_product_db_id, 'name': str(dataset.version), 'label': str(dataset.version)})

    # Retrieve versioned product row id
    with get_db_cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_dataproducts_versions
            WHERE _meta_dataproducts_id= %(dataproduct)s AND name = %(name)s;
        """
        cur.execute(sql, {'dataproduct': data_product_db_id, 'name': str(dataset.version)})
        dataproducts_versions_id = cur.fetchone()[0]

    # Ensure dataset exists in db
    with get_db_cursor() as cur:
        sql = """
            INSERT INTO _meta_dataproducts_versions_instruments
            (_meta_dataproducts_versions_id, _meta_instruments_id)
            VALUES (%(dataproduct_version)s, %(instrument)s)
            ON CONFLICT DO NOTHING;
        """
        cur.execute(sql,
                    {'dataproduct_version': dataproducts_versions_id, 'instrument': instrument_db_id})

    # Set dataset metadata according to arguments
    if data_span is not None:
        begin_comparison_identifier = 'data_begin' if accumulate_data_span else 'null'
        end_comparison_identifier = 'data_end' if accumulate_data_span else 'null'
        sql = f"""
                        UPDATE _meta_dataproducts_versions_instruments
                        SET data_begin = LEAST({begin_comparison_identifier}, CAST(%(data_begin)s AS TIMESTAMP)), 
                            data_end = GREATEST({end_comparison_identifier}, CAST(%(data_end)s AS TIMESTAMP)), 
                            last_updated = %(last_updated)s
                        WHERE _meta_dataproducts_versions_id = %(dataproduct_version)s AND _meta_instruments_id = %(instrument)s;
                    """

        with get_db_cursor() as cur:

            cur.execute(sql, {'dataproduct_version': dataproducts_versions_id, 'instrument': instrument_db_id, 'data_begin': data_span.begin, 'data_end': data_span.end, 'last_updated': datetime.now()})