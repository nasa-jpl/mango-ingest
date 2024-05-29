from masschange.ingest.datafilereaders.gracefoahk_base import  GraceFOAhkBaseDataFileReader

class GraceFOAhk1BDataFileReader(GraceFOAhkBaseDataFileReader):

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^AHK1B_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_time_ref_const_value(cls) -> str:
        return 'G'
