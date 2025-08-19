from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.glv1b import GraceFOGlv1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOGlv1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGlv1BDataFileReader()

    mission = GraceFO
    id_suffix = 'GLV1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            GRACEFO_id CHAR not null,
            coord_ref CHAR not null,

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

            qualflg VARCHAR(8) not null,
          
            location geometry(Point,4326),
            orbit_direction CHAR not null,
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
    
