from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.datasets.gracefo.act1a import GraceFOAct1ADataset
from masschange.datasets.gracefo.act1b import GraceFOAct1BDataset
from masschange.datasets.gracefo.ihk1a import GraceFOIhk1ADataset
from masschange.datasets.gracefo.imu1a import GraceFOImu1ADataset
from masschange.datasets.gracefo.mag1a import GraceFOMag1ADataset
from masschange.datasets.gracefo.pci1a import GraceFOPci1ADataset
from masschange.datasets.gracefo.sca1a import GraceFOSca1ADataset
from masschange.datasets.gracefo.thr1a import GraceFOThr1ADataset
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.ingest.ingest import log

DATASETS_BY_NAME = {
    'GRACEFO_ACC1A': GraceFOAcc1ADataset,
    'GRACEFO_ACT1A': GraceFOAct1ADataset,
    'GRACEFO_IHK1A': GraceFOIhk1ADataset,
    'GRACEFO_IMU1A': GraceFOImu1ADataset,
    'GRACEFO_MAG1A': GraceFOMag1ADataset,
    'GRACEFO_PCI1A': GraceFOPci1ADataset,
    'GRACEFO_SCA1A': GraceFOSca1ADataset,
    'GRACEFO_THR1A': GraceFOThr1ADataset,

    'GRACEFO_ACT1B': GraceFOAct1BDataset
}


def resolve_dataset(dataset_id: str) -> TimeSeriesDataset:
    #     hardcode these for now, figure out how to generate them later

    cls = DATASETS_BY_NAME.get(dataset_id)()
    if cls is not None:
        return cls
    else:
        err_msg = f"Failed to resolve provided dataset_id (got '{dataset_id}', expected one of {sorted(DATASETS_BY_NAME.keys())})"
        log.error(err_msg)
        raise ValueError(err_msg)
