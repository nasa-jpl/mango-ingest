import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence
from datetime import timedelta
from typing import Dict, Set, Type, List

from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField, \
    TimeSeriesDataProductTimestampField, TimeSeriesDataProductLocationLookupField
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.dataproducts.db.utils import get_db_connection
from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.missions import Mission

log = logging.getLogger()


class TimeSeriesDataProduct(ABC):
    # TODO: Document this class properly
    description: str = ''
    mission: Type[Mission]
    id_suffix: str  # TODO: come up with a better name for this - it's used as a full id in the API so need to iron out the nomenclature
    instrument_ids: Set[str]
    time_series_interval: timedelta
    processing_level: str

    aggregation_step_factor: int = 5  # the factor which is applied at each level of downsampling aggregation
    max_data_span = timedelta(weeks=52 * 30)  # extent of full data span for determining aggregation steps
    query_result_limit = 36000

    TIMESTAMP_COLUMN_NAME = 'timestamp'  # must be considered reserved
    LOCATION_COLUMN_NAME = 'location'  # must be considered reserved, and is treated differently when selecting/formatting

    @classmethod
    def get_full_id(cls) -> str:
        return f'{cls.mission.id}_{cls.id_suffix}'

    @classmethod
    def describe(cls, exclude_available_versions: bool = False,  metadata_cache: List[Dict] = None) -> Dict:
        """
        Returns
        -------
        An object which describes this dataset's attributes/configuration to an end-user, providing details which are
        useful or necessary for querying it.
        """
        description = {
            'description': cls.description,
            'mission': cls.mission.id,
            'id': cls.id_suffix,
            'full_id': cls.get_full_id(),
            'processing_level': cls.processing_level,
            'instruments': sorted(cls.instrument_ids),
            'available_fields': sorted([field.describe() for field in cls.get_available_fields()],
                                       key=lambda description: description['name']),
            'available_resolutions': [
                {
                    'downsampling_factor': factor,
                    'nominal_data_interval_seconds': cls.time_series_interval.total_seconds() * factor
                } for factor in cls.get_available_downsampling_factors()
            ],
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME,
            'query_result_limit': cls.query_result_limit
        }

        try:
            if metadata_cache is not None:
                datasets = [ds for ds in metadata_cache if ds['product'] == cls.get_full_id()]
                description['datasets'] = datasets
                description['available_versions'] = sorted({ds['version'] for ds in datasets})
            elif not exclude_available_versions:
                description['available_versions'] = sorted(str(version) for version in cls.get_available_versions())

        except KeyError as err:
            logging.error(f'Failed to retrieve expected metadata for product {cls.get_full_id()}: {err}')

        return description

    @classmethod
    def get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def validate_requested_fields(cls, requested_fields: Collection[TimeSeriesDataProductField],
                                  using_aggregations: bool) -> None:
        requested_fields = set(requested_fields)
        available_fields = {f for f in cls.get_available_fields() \
                            if (
                                not using_aggregations
                                or f.has_aggregations
                                or f.is_lookup_field
                                or f.name == cls.TIMESTAMP_COLUMN_NAME
                            ) and not f.is_constant}
        if not all([f in available_fields for f in requested_fields]):
            available_field_names = [f.name for f in available_fields]
            # requested fields which aren't available for selection
            unavailable_fields = {f for f in requested_fields.difference(available_fields)}
            unavailable_field_names = {f.name for f in unavailable_fields}
            # requested fields which aren't available for selection due to lack of defined aggregations
            unavailable_aggregate_field_names = {f.name for f in unavailable_fields if
                                                 not f.has_aggregations} if using_aggregations else set()

            msg = f'Some requested fields {sorted(unavailable_field_names)} not present in available fields ({sorted(available_field_names)}).'

            if len(unavailable_aggregate_field_names) > 0:
                msg += f' The following fields are unavailable due to lack of defined aggregations: {sorted(unavailable_aggregate_field_names)}'

            raise ValueError(msg)

    @classmethod
    def structure_results(cls, requested_fields: Collection[TimeSeriesDataProductField], using_aggregations: bool,
                          result: Dict) -> Dict:
        structured_result = {}
        for field in requested_fields:
            # timestamp should not be wrapped with the usual 'value': $value structure
            if field.name == cls.TIMESTAMP_COLUMN_NAME:
                structured_result[field.name] = result[field.name]

            # location, likewise, must be handled exceptionally
            elif field.name == cls.LOCATION_COLUMN_NAME:
                if field.is_lookup_field:
                    # for non-GNV, we want ta avoid wrapping with the usual 'value': $value format
                    structured_result[cls.LOCATION_COLUMN_NAME] = result[cls.LOCATION_COLUMN_NAME]
                else:
                    # for GNV, the component lat/lon must be nested into a 'location' attribute
                    structured_result[cls.LOCATION_COLUMN_NAME] = {
                        'latitude': result.get('latitude'),
                        'longitude': result.get('longitude')
                    }

            # normal fields, with aggregations
            elif using_aggregations and field.has_aggregations:
                aggregate_column_names = {agg.get_aggregated_name(field.name) for agg in field.aggregations}
                for column_name in aggregate_column_names:
                    agg_name = column_name.replace(f'{field.name}_', '', 1)
                    if field.name not in structured_result:
                        structured_result[field.name] = {}
                    structured_result[field.name][agg_name] = result[column_name]

            # normal fields, without aggregations
            else:
                if field.name not in structured_result:
                    structured_result[field.name] = {}
                structured_result[field.name]['value'] = result[field.name]

        return structured_result

    @classmethod
    @abstractmethod
    def get_sql_table_schema(cls) -> str:
        """
        Get the column definitions used in the SQL create table statement.
        Must be defined in every subclass.
        """
        pass

    @classmethod
    @abstractmethod
    def get_reader(cls) -> DataFileReader:
        """Return the DataFileReader used to ingest this dataset"""
        pass

    @classmethod
    def get_available_fields(cls) -> Set[TimeSeriesDataProductField]:
        timestamp_field: TimeSeriesDataProductField = TimeSeriesDataProductTimestampField(cls.TIMESTAMP_COLUMN_NAME,
                                                                                          'n/a')

        special_fields = {timestamp_field}
        if cls.LOCATION_COLUMN_NAME not in [field.name for field in cls.get_reader().get_fields()]:
            # GNV products have an inherent location field.  Other products require the addition of a field for the
            # query-time location lookup sourced from the GNV data
            location_lookup_field: TimeSeriesDataProductField = TimeSeriesDataProductLocationLookupField(cls.LOCATION_COLUMN_NAME,
                                                                                              'Latitude/Longitude (EPSG:4326)')
            special_fields.add(location_lookup_field)

        return special_fields.union(cls.get_reader().get_fields())

    @classmethod
    def get_field_by_name(cls, field_name: str) -> TimeSeriesDataProductField:
        try:
            return next(f for f in cls.get_available_fields() if f.name == field_name)
        except StopIteration:
            raise ValueError(f'No field with name "{field_name}" found in class "{cls.__name__}" '
                             f'(valid names are {[f.name for f in cls.get_available_fields()]})')

    @classmethod
    def has_time_series_id_fields(cls) -> bool:
        return len([f.name for f in cls.get_available_fields() if f.is_time_series_id_column]) > 0

    @classmethod
    def get_available_versions(cls) -> Set[TimeSeriesDatasetVersion]:
        with get_db_connection() as conn, conn.cursor() as cur:
            data_product_name = cls.get_full_id()

            sql = f"""
                SELECT v.name
                FROM _meta_dataproducts_versions as v 
                WHERE v._meta_dataproducts_id in (
                    SELECT dp.id
                    FROM _meta_dataproducts as dp
                    WHERE dp.name=%(data_product_name)s
                );
                """
            cur.execute(sql, {'data_product_name': data_product_name})
            results = [row[0] for row in cur.fetchall()]

        return {TimeSeriesDatasetVersion(version_name) for version_name in results}

    @classmethod
    def get_required_aggregation_level_count(cls) -> int:
        if not any(field.has_aggregations for field in cls.get_available_fields()):
            return 0

        return len(list(cls._generate_cagg_bucket_intervals()))

    @classmethod
    def get_available_aggregation_levels(cls) -> Sequence[int]:
        """
        Return the sorted levels (hierarchical level, not decimation factor) of aggregation which exist for this dataset
        , *exclusive* of level 0 (full-resolution)
        """
        return [x for x in range(1, cls.get_required_aggregation_level_count() + 1)]

    @classmethod
    def get_available_downsampling_factors(cls) -> Sequence[int]:
        """
        Return the sorted downsampling resolution factors (full-res and aggregated) which exist for this dataset.
        Fractional factors (which are a rare/nonexistent edge case) are rounded to the nearest integer.
        The rounded values are used in the API, and when naming data tables
        """
        return [1] + [round(interval / cls.time_series_interval) for interval in list(cls._generate_cagg_bucket_intervals())]

    @classmethod
    def get_nominal_data_interval(cls, downsampling_level: int) -> timedelta:
        """
        For a given downsampling level (hierarchical level, not factor), return the nominal interval between data.  For
        aggregated data, this is the bucket width
        """
        return cls.time_series_interval * cls.aggregation_step_factor ** downsampling_level

    @classmethod
    def _generate_cagg_bucket_intervals(cls) -> Sequence[timedelta]:
        """
        Generate the necessary series of bucket intervals for this dataset, given its raw resolution.

        The step factor is applied repeatedly until a value of aligned_bucket_span is reached, ensuring that
        aligned_bucket_span is included in the series, at which point the step factor is repeatedly applied to
        aligned_bucket_span such that all data from that point onward is time-aligned across datasets.

        The series continues until it reaches a point sufficient to represent the forseeable dataset temporal span in no
        more than ~approximate_pixel_count data.
        """
        # TODO: account for the edge case where the raw time_series_interval is larger than the aligned_bucket_span

        if not any(field.has_aggregations for field in cls.get_available_fields()):
            return


        approximate_pixel_count = 5000
        aligned_bucket_span = timedelta(seconds=10)

        # created the intervals which are smaller than the smallest-common bucket interval
        bucket_interval = cls.time_series_interval * cls.aggregation_step_factor
        while bucket_interval < aligned_bucket_span:
            yield bucket_interval
            bucket_interval *= cls.aggregation_step_factor

        # create the smallest-common bucket interval
        bucket_interval = aligned_bucket_span
        yield bucket_interval
        bucket_interval *= cls.aggregation_step_factor

        # while estimated full-span pixel count is above the desired count, generate intervals using the smallest-common
        # bucket interval as a basis for successive multiplication
        while cls.max_data_span / bucket_interval > approximate_pixel_count:
            yield bucket_interval
            bucket_interval *= cls.aggregation_step_factor
