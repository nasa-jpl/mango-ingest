import logging

from datetime import datetime, timedelta
from typing import List, Dict, Union, Iterable

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import cursor as Cursor

from masschange.api.errors import TooMuchDataRequestedError
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.db.conn import get_db_cursor
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.implementations.gracefo.primary.gnv1a import GraceFOGnv1ADataProduct

log = logging.getLogger()


class TimeSeriesDataset(Dataset):

    product: TimeSeriesDataProduct

    """
    TODO: this is in the child class because it uses aggregations
    """
    def _enumerate_time_series_id_values(self, cur: Cursor) -> Dict:
        if not self.product.has_time_series_id_fields():
            return {}

        time_series_id_column_names = [f.name for f in self.product.get_available_fields() if
                                       f.is_time_series_id_column]
        # To avoid long queries, a view is used rather than the full-res dataset.  The level must be low enough that it
        # is safe to assume all possible values have been written to that materialized view. 5 is a good starting point.
        view_depth = min(([0] + self.product.get_available_aggregation_levels())[-1], 5)
        sql = f"""
            SELECT DISTINCT {','.join(sorted(time_series_id_column_names))}
            FROM {self.get_table_or_view_name(view_depth)};
            """
        try:
            cur.execute(sql)
        except Exception as err:
            raise err.__class__(f'query failed with {err}: {sql}')

        metadata = {column: set() for column in time_series_id_column_names}
        for row in cur.fetchall():
            for column in time_series_id_column_names:
                metadata[column].add(row[column])

        for column in time_series_id_column_names:
            metadata[column] = sorted(metadata[column])

        return metadata

    """TODO: this method is overwritten in a child class because is 
    uses  _enumerate_time_series_id_values which uses aggregations"""
    def get_metadata_properties(self) -> Union[Dict, None]:
        """Get available values from the _meta_dataproducts_versions_instruments table for the corresponding row"""
        supported_properties = {'data_begin', 'data_end', 'last_updated'}

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                metadata = self._get_basic_metadata(cur, supported_properties)
                metadata['time_series_id_enums'] = self._enumerate_time_series_id_values(cur)
            except Exception as err:
                logging.warning(err)
                return None

        return metadata

    def validate_requested_aggregation_level(self, requested_aggregation_level: int, from_dt: datetime,
                                             to_dt: datetime) -> int:
        return max(requested_aggregation_level, self.get_minimum_aggregation_level(from_dt, to_dt))

    def get_downsampling_factor(self, aggregation_level: int) -> int:
        return self.product.aggregation_step_factor ** aggregation_level

    def get_max_query_temporal_span(self, downsampling_factor: int) -> timedelta:
        return self.product.query_result_limit * self.product.time_series_interval * downsampling_factor

    def get_table_name(self) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given instruments"""
        return self.get_table_or_view_name(aggregation_depth=0)

    def get_table_or_view_name(self, aggregation_depth: int) -> str:
        """
        Return the name of the SQL table or view providing access to data for this dataset for a given instruments at a given
        aggregation level
        """
        if self.instrument_id not in self.product.instrument_ids:
            raise ValueError(
                f'instruments id "{self.instrument_id}" not recognized (expected one of {sorted(self.product.instrument_ids)})')

        aggregation_depth_pad_width = 2
        padded_aggregation_depth = str(aggregation_depth).rjust(aggregation_depth_pad_width, "0")
        if len(padded_aggregation_depth) > aggregation_depth_pad_width:
            raise ValueError(
                f'aggregation_depth "{aggregation_depth}" exceeds maximum accounted for ({aggregation_depth_pad_width} digits)')

        # f for factor, l for level - aids in view maintenance
        aggregation_suffix = f'f{self.product.aggregation_step_factor}l{padded_aggregation_depth}'

        # TODO: Remove legacy null-version support when fully implemented/migrated
        table_base_name = super().get_table_name()

        return (table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}').lower()

    # TODO: Find a way to move it to a base class. The problem is that GraceFOGnv1ADataProduct is a time-series product
    def attach_lat_lon(self, from_dt: datetime, to_dt: datetime, data: Iterable[Dict]) -> None:
        """
        Assign approximate locations to a set of results from TimeSeriesDataset.select(), using ingested GNV data to map
        datum timestamps to a lat/lon.  The format is 'location': {'latitude': $value, 'longitude': $value}
        The maximal error will be equal to +/- the satellite's movement in one second (i.e. half the temporal resolution
        of the GNV dataset).
        A value of None will be assigned to input data for which there is no GNV data available.
        """
        gnv_dataset = TimeSeriesDataset(GraceFOGnv1ADataProduct(), self.version, self.instrument_id)
        gnv_field_names = {gnv_dataset.product.TIMESTAMP_COLUMN_NAME, 'location'}
        gnv_fields = [f for f in gnv_dataset.product.get_available_fields() if f.name in gnv_field_names]

        # Need to ensure that the GNV data span fully encloses the input data span
        gnv_from_dt = from_dt - GraceFOGnv1ADataProduct.time_series_interval
        gnv_to_dt = to_dt + GraceFOGnv1ADataProduct.time_series_interval
        gnv_data = gnv_dataset.select(gnv_from_dt, gnv_to_dt, gnv_fields)

        try:
            data_iter = iter(data)
            geo_iter = iter(gnv_data)

            data_el = next(data_iter)
            gnv_pair_begin = None
            gnv_pair_end = next(geo_iter)

            # DEV WARNING: Here be dragons - the nested iteration is easy to mess up and unit tests don't exist yet.
            while True:  # iterate until a StopIteration
                gnv_pair_begin = gnv_pair_end
                gnv_pair_end = next(geo_iter)

                gnv_begin_ts = gnv_pair_begin[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                gnv_end_ts = gnv_pair_end[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                el_ts = data_el[self.product.TIMESTAMP_COLUMN_NAME]

                # If datum exists before start of the GNV pair, assign it a null value and move on
                # This should ONLY occur for the first GNV pair, and should loop through all data elements with
                # timestamps earlier than the available GNV data
                if (el_ts < gnv_begin_ts):
                    data_el[self.product.LOCATION_COLUMN_NAME] = None
                    data_el = next(data_iter)
                    continue

                # for each datum falling within the timespan bounded by the gnv pair, assign it the location of
                #  the closest bounding gnv record
                while (gnv_begin_ts <= el_ts <= gnv_end_ts):
                    if abs(el_ts - gnv_begin_ts) <= abs(el_ts - gnv_end_ts):
                        data_el[self.product.LOCATION_COLUMN_NAME] = gnv_pair_begin[
                            GraceFOGnv1ADataProduct.LOCATION_COLUMN_NAME]
                    else:
                        data_el[self.product.LOCATION_COLUMN_NAME] = gnv_pair_end[
                            GraceFOGnv1ADataProduct.LOCATION_COLUMN_NAME]

                    data_el = next(data_iter)
                    el_ts = data_el[self.product.TIMESTAMP_COLUMN_NAME]

        except StopIteration:
            pass

        # Assign null location to all data after end of available GNV data
        while (data_el := next(data_iter, None)) is not None:
            data_el[self.product.LOCATION_COLUMN_NAME] = None

    def get_minimum_aggregation_level(self, from_dt: datetime, to_dt: datetime, check_data_span: bool = False):
        """
        Given a query span, return the lowest aggregation level required to limit the result to the product's query
        result limit

        Parameters
        ----------
        from_dt: datetime
        to_dt: datetime
        check_data_span: bool - if true, will query the db for actual data span and trim the requested bounds
            accordingly. Gives absolute minimum aggregation level but is slower due to overhead

        """
        if check_data_span:
            extant_data_span = self.get_data_span(use_cache=True)
            span_duration = max(to_dt, extant_data_span.begin) - min(from_dt, extant_data_span.end)
        else:
            span_duration = to_dt - from_dt
        full_res_data_count = span_duration / self.product.time_series_interval
        downsampling_factor_lower_bound = full_res_data_count / self.product.query_result_limit
        # return the lowest index for all factors which meet or exceed the lower bound
        try:
            return min(i for i, f in enumerate(self.product.get_available_downsampling_factors()) if
                   f >= downsampling_factor_lower_bound)
        except ValueError:
            raise TooMuchDataRequestedError(f'No available downsampling factor can reduce query span below {self.product.query_result_limit} expected hits. Please request a smaller data span.')

    @classmethod
    def is_time_series_dataset(cls) -> bool:
        return True
