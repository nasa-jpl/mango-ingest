
from collections.abc import Sequence
from datetime import timedelta, datetime
from typing import Dict, List, Union

from masschange.api.errors import TooMuchDataRequestedError
from masschange.dataproducts.dataproduct import DataProduct


class TimeSeriesDataProduct(DataProduct):

    time_series_interval: timedelta
    aggregation_step_factor: int = 5  # the factor which is applied at each level of downsampling aggregation

    # aligned_bucket_span is a "lowest common denominator" for cagg bucket spans used when aggregating.
    # This allows timestamp alignment of datasets which have different raw resolutions, and this base-class value is
    # overridden only for products whose raw resolution is incompatible with the default value.  Cagg bucket spans must
    # be a multiple of their input view/table bucket spans, so the 8Hz IMU1A/1B, for example, is incompatible.
    aligned_bucket_span: timedelta = timedelta(seconds=10)

    max_data_span = timedelta(weeks=52 * 30)  # extent of full data span for determining aggregation steps

    @classmethod
    def describe(cls, exclude_available_versions: bool = False,  metadata_cache: List[Dict] = None) -> Dict:
        """
        Returns
        -------
        An object which describes this data product's attributes/configuration to an end-user, providing details which are
        useful or necessary for querying it.
        """

        description = super().describe(exclude_available_versions, metadata_cache)

        # TODO: (not urgent) consolidate the concept of "available resolutions" into something common to all DataProduct
        #  as the current approach of using a default in the parent class and overwriting it here is a bit messy.
        description['available_resolutions'] = [
            {
                'downsampling_factor': factor,
                'nominal_data_interval_seconds': cls.time_series_interval.total_seconds() * factor
            } for factor in cls.get_available_downsampling_factors()
        ]
        return description

    def validate_requested_aggregation_level(self, requested_aggregation_level: int, from_dt: datetime,
                                             to_dt: datetime) -> int:
        return max(requested_aggregation_level, self.get_minimum_aggregation_level(from_dt, to_dt))

    def get_minimum_aggregation_level(self, from_dt: datetime, to_dt: datetime):
        """
        Given a query span, return the lowest aggregation level required to limit the result to the product's query
        result limit

        Parameters
        ----------
        from_dt: datetime
        to_dt: datetime
        """

        span_duration = to_dt - from_dt

        full_res_data_count = span_duration / self.time_series_interval
        downsampling_factor_lower_bound = full_res_data_count / self.query_result_limit
        # return the lowest index for all factors which meet or exceed the lower bound
        try:
            return min(i for i, f in enumerate(self.get_available_downsampling_factors()) if
                   f >= downsampling_factor_lower_bound)
        except ValueError:
            raise TooMuchDataRequestedError(f'No available downsampling factor can reduce query span below {self.query_result_limit} expected hits. Please request a smaller data span.')

    def get_downsampling_factor(self, aggregation_level: int) -> int:
        return self.aggregation_step_factor ** aggregation_level

    def get_max_query_temporal_span(self, downsampling_factor: int) -> timedelta:
        return self.query_result_limit * self.time_series_interval * downsampling_factor

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
        return cls.get_available_data_intervals()[downsampling_level]

    @classmethod
    def get_available_data_intervals(cls) -> List[timedelta]:
        """Return the full-resolution data interval, plus any data aggregate intervals"""
        return [cls.time_series_interval] + list(cls._generate_cagg_bucket_intervals())

    def get_cagg_bucket_interval(cls, aggregation_level: int) -> Union[timedelta, None]:
        if aggregation_level > cls.get_required_aggregation_level_count():
            raise ValueError(f'Aggregation level {aggregation_level} is too high for {cls.__name__} (max is {cls.get_required_aggregation_level_count()})')

        bucket_invervals_by_aggregation_level = [None] + list(cls._generate_cagg_bucket_intervals())
        return bucket_invervals_by_aggregation_level[aggregation_level]

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

        # created the intervals which are smaller than the smallest-common bucket interval
        bucket_interval = cls.time_series_interval * cls.aggregation_step_factor
        while bucket_interval < cls.aligned_bucket_span:
            yield bucket_interval
            bucket_interval *= cls.aggregation_step_factor

        # create the smallest-common bucket interval
        bucket_interval = cls.aligned_bucket_span
        yield bucket_interval
        bucket_interval *= cls.aggregation_step_factor

        # while estimated full-span pixel count is above the desired count, generate intervals using the smallest-common
        # bucket interval as a basis for successive multiplication
        while cls.max_data_span / bucket_interval > approximate_pixel_count:
            yield bucket_interval
            bucket_interval *= cls.aggregation_step_factor

    @classmethod
    def get_chunk_time_interval(cls) -> timedelta:
        """
        :return: the TimescaleDB chunk time interval for this product.  Per best-practice, chunks should be sized such
        that storing one chunk from each hypertable in memory consumes 25% of available memory.
        Sizing is, by default, coarsely-approximated based on the time-series interval.  This method should be
        overridden at the product level if the approximation is found to be inaccurate for a particular product.
        Changes to existing hypertables only apply to data written after the change is made, so data may have to be
        reingested or migrated (utility script implementation to-do) if a change is made after data is loaded.
        """

        # Stick to some enumerated thresholds, by default
        if cls.time_series_interval <= timedelta(milliseconds=100):
            return timedelta(hours=48)
        elif cls.time_series_interval <= timedelta(milliseconds=500):
            return timedelta(days=10)
        elif cls.time_series_interval <= timedelta(seconds=1):
            return timedelta(days=20)
        elif cls.time_series_interval <= timedelta(seconds=5):
            return timedelta(days=100)
        elif cls.time_series_interval <= timedelta(hours=1):
            return timedelta(days=365)  # chosen arbitrarily
        elif cls.time_series_interval <= timedelta(days=1):
            return timedelta(days=365*50)  # chosen arbitrarily
        else:
            raise ValueError(f'Unexpected time_series_interval value:  {cls.time_series_interval}')

    @classmethod
    def is_time_series_dataproduct(cls) -> bool:
        return True
