import unittest

from masschange.partitioning import get_partition_id


class GetPartitionIdTestCase(unittest.TestCase):
    def test_produces_correct_keys(self):
        self.assertEqual(3600, get_partition_id(5400, hours_per_partition=1, epoch_unit_factor=1))
        self.assertEqual(0, get_partition_id(5400, hours_per_partition=2, epoch_unit_factor=1))
        self.assertEqual(0, get_partition_id(5400, hours_per_partition=1))
        self.assertEqual(3600000000, get_partition_id(5400000000, hours_per_partition=1))

    def test_offset_correctly_aligns_partition(self):
        # Presume that timestamp of 0 aligns with an epoch of 08:00:00 and that calendar-day partitions are desirable
        epoch_offset_hours = 8
        hours_per_partition = 24

        partial_partition_start_timestamp = 0
        partial_partition_mid_timestamp = 12 * 3600
        first_full_partition_start_timestamp = 16 * 3600
        first_full_partition_mid_timestamp = first_full_partition_start_timestamp + 1
        second_full_partition_start_timestamp = (16 + 24) * 3600
        second_full_partition_mid_timestamp = second_full_partition_start_timestamp + 1

        partial_partition_id = 0
        first_full_partition_id = epoch_offset_hours * 3600
        second_full_partition_id = (epoch_offset_hours + hours_per_partition) * 3600

        self.assertEqual(partial_partition_id,
                         get_partition_id(partial_partition_start_timestamp, hours_per_partition, epoch_offset_hours,
                                          epoch_unit_factor=1))
        self.assertEqual(partial_partition_id,
                         get_partition_id(partial_partition_mid_timestamp, hours_per_partition, epoch_offset_hours,
                                          epoch_unit_factor=1))
        self.assertEqual(first_full_partition_id,
                         get_partition_id(first_full_partition_start_timestamp, hours_per_partition, epoch_offset_hours,
                                          epoch_unit_factor=1))
        self.assertEqual(first_full_partition_id,
                         get_partition_id(first_full_partition_mid_timestamp, hours_per_partition, epoch_offset_hours,
                                          epoch_unit_factor=1))
        self.assertEqual(second_full_partition_id,
                         get_partition_id(second_full_partition_start_timestamp, hours_per_partition,
                                          epoch_offset_hours, epoch_unit_factor=1))
        self.assertEqual(second_full_partition_id,
                         get_partition_id(second_full_partition_mid_timestamp, hours_per_partition, epoch_offset_hours,
                                          epoch_unit_factor=1))

    def test_offset_correctly_aligns_partition_insitu_data(self):
        # GRACE-FO 1A data uses 2020-01-01 12:00:00 epoch
        epoch_offset_hours = 12
        hours_per_partition = 24

        timestamp = 738849600011933  # ~2023-06-01T00:00:00
        expected_partition_id = 738849600000000  # 2023-06-01T00:00:00
        self.assertEqual(expected_partition_id,
                         get_partition_id(timestamp, hours_per_partition, epoch_offset_hours, 1000000))

        timestamp_2 = 738892800035047   # ~2023-06-01T12:00:00
        expected_partition_id_2 = 738849600000000  # 2023-06-01T00:00:00
        self.assertEqual(expected_partition_id_2,
                         get_partition_id(timestamp_2, hours_per_partition, epoch_offset_hours, 1000000))


if __name__ == '__main__':
    unittest.main()
