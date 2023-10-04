from datetime import datetime
from typing import Set, List


class TimeSeriesDatasetConfig:
    stream_ids: Set[str | int]
    base_hours_per_partition: int
    decimation_step_factors: List[int]
    reference_epoch: datetime

    partition_epoch_offset_hours: int  # TODO: is a 12hr epoch a project default?

    def __init__(self,
                 stream_ids: Set[str | int],
                 base_hours_per_partition: int,
                 decimation_step_factors: List[int],
                 reference_epoch: datetime):
        """

        Parameters
        ----------
        stream_ids - a set of unique ids for different record streams. These will typically be satellite_ids
        base_hours_per_partition - the hour span of the partitions for the full-resolution dataset.  This will typically
            correspond to the temporal span of the input files (i.e. 24 for daily granules)
        decimation_step_factors - the factors by which to iteratively downsample/decimate the data.  These are
            cumulative, so [2,4] will result in datasets 1:1, 1:2, 1:8 after decimation.
        reference_epoch - the reference epoch used by the input data and parquet storage for their int-based timestamps
        partition_epoch_offset_hours - the offset of the dataset's timestamp epoch, with respect to 0000h.
            Ex. if it is desirable for partitions to align with dates due to input data being provided in daily granules
            but the epoch reference is at 8:00:00, a value equal to the positive distance from the start of the day (8)
            should be provided.
        """

        self.stream_ids = {str(id) for id in stream_ids}
        self.base_hours_per_partition = int(base_hours_per_partition)
        self.decimation_step_factors = [int(val) for val in decimation_step_factors]
        if reference_epoch.minute != 0 or reference_epoch.second != 0 or reference_epoch.microsecond != 0:
            raise ValueError(f'TimeSeriesDatasetConfig.reference_epoch does not currently support sub-hour granularity')
        self.reference_epoch = reference_epoch

    @property
    def partition_epoch_offset_hours(self) -> int:
        return self.reference_epoch.time().hour

    @property
    def available_decimation_ratios(self) -> List[int]:
        available_ratios = [1]
        for factor in self.decimation_step_factors:
            available_ratios.append(factor*available_ratios[-1])
        return available_ratios

    def get_hours_per_partition(self, decimation_factor: int) -> int:
        """Return the temporal span (in hours) for the partition with a given decimation factor"""
        return self.base_hours_per_partition * decimation_factor

    def validate_decimation_ratio(self, ratio: int):
        """Throw ValueError on invalid decimation ratio"""
        if ratio not in self.available_decimation_ratios:
            raise ValueError(f"requested decimation_ratio '{ratio}' not in available list of ratios {self.available_decimation_ratios}")

    def validate_stream_id(self, stream_id: str | int):
        """Throw ValueError on invalid stream_id"""
        if stream_id not in self.stream_ids:
            raise ValueError(
                f"requested stream_id '{stream_id}' not in available list of stream ids {sorted(self.stream_ids)}")