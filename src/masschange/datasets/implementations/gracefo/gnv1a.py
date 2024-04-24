from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefognv1a import GraceFOGnv1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOGnv1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGnv1ADataFileReader()

    mission = GraceFO
    id_suffix = 'GNV1A'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=2)

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        return f"""
            rcv_time bigint not null,
            n_prns int not null,
            GRACEFO_id CHAR not null,

            chisq  double precision not null,
            cov_mult  double precision not null,
            voltage  double precision not null,

            xpos  double precision not null,
            ypos  double precision not null,
            zpos  double precision not null,

            xpos_err  double precision not null,
            ypos_err  double precision not null,
            zpos_err  double precision not null,
            
            xvel  double precision not null,
            yvel  double precision not null,
            zvel  double precision not null,

            xvel_err  double precision not null,
            yvel_err  double precision not null,
            zvel_err  double precision not null,

            timer_offset  double precision not null,
            time_offset_err  double precision not null,
            time_drift  double precision not null,
            err_drift  double precision not null,
            qualflg VARCHAR(8) not null,
          
            location geometry(Point,4326),
            timestamp timestamptz not null
        """
    
