import logging

from collections.abc import Sequence
from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from typing import Set, Type, Dict, Collection, Union, Mapping

import psycopg2

from masschange.missions import Mission
from masschange.dataproducts.dataproductfield import DataProductField, \
    TimeSeriesDataProductTimestampField, TimeSeriesDataProductLocationLookupField
from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.db.conn import get_db_cursor

log = logging.getLogger()


class DataProduct(ABC):
    # TODO: Document this class properly
    description: str = ''
    mission: Type[Mission]
    id_suffix: str  # TODO: come up with a better name for this - it's used as a full id in the API so need to iron out the nomenclature
    instrument_ids: Set[str]
    processing_level: Union[str, None]

    query_result_limit = 36000

    TIMESTAMP_COLUMN_NAME = 'timestamp'  # must be considered reserved
    # TODO: find a way to move it to the base class - non-timeseries datasets should support location as well
    LOCATION_COLUMN_NAME = 'location'  # must be considered reserved, and is treated differently when selecting/formatting

    @classmethod
    def get_full_id(cls) -> str:
        return f'{cls.mission.id}_{cls.id_suffix}'

    @classmethod
    def get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def describe(cls, exclude_available_versions: bool = False, metadata_cache = None) -> Dict:
        # TODO: break out description into formatter class(es) - type hinting is unavailable here because hinting
        #  BulkMetadataCache causes a circular import between DataProduct and BulkMetadataCache - edunn 20250416
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
            'available_fields': sorted([field.describe(cls) for field in cls.get_available_fields()],
                                       key=lambda description: description['name']),
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME,
            'query_result_limit': cls.query_result_limit
        }

        # TODO: (not urgent) consolidate the concept of "available resolutions" into something common to all DataProduct
        #  as the current approach of using a default in the parent class and overwriting it TimeSeriesDataProduct is a
        #  bit messy.
        description['available_resolutions'] = [
            {
                'downsampling_factor': [1],
                'nominal_data_interval_seconds': None
            }
        ]

        try:
            if metadata_cache is not None:
                datasets = [ds for ds in metadata_cache.datasets if ds.product_id == cls.get_full_id()]
                description['datasets'] = datasets
                description['available_versions'] = sorted({ds.version_id for ds in datasets})
                description['enum_values'] = next(p.channel_enum_values for p in metadata_cache.dataproducts if p.product is cls)

            else:
                description['enum_values'] = cls.fetch_channel_id_values()

                if not exclude_available_versions:
                    description['available_versions'] = sorted(str(version) for version in cls.get_available_versions())

        except KeyError as err:
            logging.error(f'Failed to retrieve expected metadata for product {cls.get_full_id()}: {err}')

        return description

    @classmethod
    def validate_requested_fields(cls, requested_fields: Collection[DataProductField],
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
    def structure_results(cls, requested_fields: Collection[DataProductField], using_aggregations: bool,
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
    def get_available_fields(cls) -> Set[DataProductField]:
        timestamp_field: DataProductField = TimeSeriesDataProductTimestampField(cls.TIMESTAMP_COLUMN_NAME,
                                                                                          'n/a')

        special_fields = {timestamp_field}
        if cls.LOCATION_COLUMN_NAME not in [field.name for field in cls.get_reader().get_fields()]:
            # GNV products have an inherent location field.  Other products require the addition of a field for the
            # query-time location lookup sourced from the GNV data
            location_lookup_field: DataProductField = TimeSeriesDataProductLocationLookupField(
                cls.LOCATION_COLUMN_NAME,
                'Latitude/Longitude (EPSG:4326)')
            special_fields.add(location_lookup_field)

        return special_fields.union(cls.get_reader().get_fields())

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
    def get_field_by_name(cls, field_name: str) -> DataProductField:
        try:
            return next(f for f in cls.get_available_fields() if f.name == field_name)
        except StopIteration:
            raise ValueError(f'No field with name "{field_name}" found in class "{cls.__name__}" '
                             f'(valid names are {[f.name for f in cls.get_available_fields()]})')

    @classmethod
    def get_available_versions(cls) -> Set[DatasetVersion]:
        with get_db_cursor() as cur:
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

        return {DatasetVersion(version_name) for version_name in results}

    @classmethod
    def is_time_series_dataproduct(cls) -> bool:
        return False

    @staticmethod
    def validate_requested_aggregation_level(requested_aggregation_level: int,
                                             from_dt: datetime, to_dt: datetime) -> int:
        """
       Given a requested_aggregation_level and a timespan, check that the requested_aggregation_level is valid for the
       timespan and Dataproduct class/subclass.  Return it if valid, else return the lowest allowable value.

       For non-timeseries data products (which do not support aggregation), this will always be 0.
        """
        return 0

    @classmethod
    def get_downsampling_factor(cls, aggregation_level: int) -> int:
        """
        Given an aggregation level, return the corresponding downsampling factor for the Dataset class/subclass.

        For non-timeseries datasets (which do not support aggregation), this will always be 1, and requesting a nonzero
        aggregation level is not valid.
        """
        if aggregation_level != 0:
            raise ValueError(
                f'{cls.__name__} is a non-timeseries dataset and so get_downsampling_factor() does not accept a nonzero argument - argument "{aggregation_level}" indicates a code error')
        return 1

    @staticmethod
    def get_max_query_temporal_span(downsampling_factor: int) -> timedelta:
        return timedelta(days=365)  # TODO: set to 1 year now

    @classmethod
    def get_available_aggregation_levels(cls) -> Sequence[int]:
        """
        Return the sorted levels (hierarchical level, not decimation factor) of aggregation which exist for this dataset
        , *exclusive* of level 0 (full-resolution)

        For non-timeseries datasets (which do not support aggregation), this will always be an empty list
        """
        return []

    @classmethod
    def has_channel_id_fields(cls) -> bool:
        return len([f.name for f in cls.get_available_fields() if f.is_channel_id_column]) > 0

    @classmethod
    def fetch_channel_id_values(cls) -> Mapping[DataProductField, Set[str]]:
        """
        Pull channel id values from the stateful metadata cache
        """
        channel_id_fields = [f for f in cls.get_available_fields() if f.is_channel_id_column]

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = """
                SELECT *
                FROM _meta_dataproducts_channelidvalues as civ
                WHERE civ.dataproduct_id in (
                    SELECT mdp.id
                        FROM _meta_dataproducts as mdp 
                        JOIN _meta_dataproducts_channelidvalues civ on mdp.id = civ.dataproduct_id
                        WHERE mdp.name = %(product_id_str)s 
                )
                """
            try:
                cur.execute(sql, {'product_id_str': cls.get_full_id()})
            except Exception as err:
                raise err.__class__(f'query failed with {err}: {sql}')

            metadata = {}
            for field in channel_id_fields:
                metadata[field] = set()

            for row in cur.fetchall():
                field = next(f for f in channel_id_fields if f.name == row['field_name'])
                metadata[field].add(row['value'])

            return {f: sorted(values) for f, values in metadata.items()}

    @classmethod
    def ensure(cls):
        # Ensure product exists in db
        with get_db_cursor() as cur:
            sql = """
                INSERT INTO _meta_dataproducts
                VALUES (DEFAULT, %(name)s, %(label)s)
                ON CONFLICT DO NOTHING;
                """
            cur.execute(sql, {'name': cls.get_full_id(), 'label': cls.get_full_id()})