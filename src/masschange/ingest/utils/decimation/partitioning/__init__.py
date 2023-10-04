from masschange.datasets.timeseriesdatasetconfig import TimeSeriesDatasetConfig


def get_partition_id(epoch_timestamp: int, hours_per_partition: int, epoch_offset_hours: int = 0, epoch_unit_factor: int = 1000000) -> int:
    """
    Given an integer epoch-based timestamp and a desired partition temporal span in hours, return the id for the
    relevant partition.

    Parameters
    ----------
    epoch_timestamp - the timestamp for which to obtain a partition id

    hours_per_partition - the span of each partition block, in hours

    epoch_offset_hours - if the epoch doesn't align exactly with the start of a partition, supply an offset to avoid
        unnecessary splitting of files.

        Ex. if it is desirable for partitions to align with dates due to input data
        being provided in daily granules but the epoch reference is at 8:00:00, a value
        equal to the positive distance from the start of the day (8) should be provided.

    epoch_unit_factor - a factor representing the number of epoch units per second.  A timestamp provided in
        seconds-since-epoch would have a factor of 1, whereas one provided in microseconds-since-epoch would have a
        factor of 1000000

    Returns
    -------
    The partition id representing the start of the partition span enclosing the given timestamp.
    Ex. For a seconds-based timestamp t=5400 and hours_per_partition=1, t will be on [n,m), where n, m are the epoch
      timestamps 3600 and 7200 respectively, and the returned id will be 3600 (for the partition spanning 3600 through
      7199).

    """

    epoch_unit_offset = epoch_offset_hours * 3600 * epoch_unit_factor
    first_full_partition_start = (hours_per_partition - epoch_offset_hours) * 3600 * epoch_unit_factor
    if epoch_timestamp < first_full_partition_start:
        return 0

    epoch_units_per_partition = epoch_unit_factor * 3600 * hours_per_partition
    return int((epoch_timestamp - epoch_unit_offset) // epoch_units_per_partition * epoch_units_per_partition + epoch_unit_offset)

