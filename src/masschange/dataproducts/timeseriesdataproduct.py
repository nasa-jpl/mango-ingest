
from collections.abc import Collection, Sequence
from datetime import timedelta
from typing import Dict, List

from masschange.dataproducts.dataproduct import DataProduct


class TimeSeriesDataProduct(DataProduct):

    time_series_interval: timedelta
    aggregation_step_factor: int = 5  # the factor which is applied at each level of downsampling aggregation

    # aligned_bucket_span is a "lowest common denominator" for cagg bucket spans used when aggregating.
    # This allows timestamp alignment of datasets which have different raw resolutions, and this base-class value is
    # overridden only for products whose raw resolution is incompatible with the default value.  Cagg bucket spans must
    # be a multiple of their input view/table bucket spans, so the 8Hz IMU1A/1B, for example, is incompatible.
    aligned_bucket_span: timedelta = timedelta(seconds=10)

    @classmethod
    def describe(cls, exclude_available_versions: bool = False,  metadata_cache: List[Dict] = None) -> Dict:
        """
        Returns
        -------
        An object which describes this dataset's attributes/configuration to an end-user, providing details which are
        useful or necessary for querying it.
        """

        description = super().describe()
        description['available_resolutions'] = [
            {
                'downsampling_factor': factor,
                'nominal_data_interval_seconds': cls.time_series_interval.total_seconds() * factor
            } for factor in cls.get_available_downsampling_factors()
        ]
        return description

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
        else:
            return timedelta(days=365 * 30)  # basically just for those datasets which are actually not time-series - this will be cleaned up when an abstraction is created for those