from masschange.missions import GraceFO
from masschange.missions.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOAcc1ADataset(TimeSeriesDataset):
    mission = GraceFO
    id_suffix = 'ACC1A'
    stream_ids = {'C', 'D'}
    available_fields = {
        'lin_accl_x',
        'lin_accl_y',
        'lin_accl_z',
        'ang_accl_x',
        'ang_accl_y',
        'ang_accl_z',
        'rcvtime',
        'timestamp'
    }