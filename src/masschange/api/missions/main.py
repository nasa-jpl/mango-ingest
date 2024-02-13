from typing import Iterable, Type

from fastapi import APIRouter

from masschange.api.timeseriesdatasetrouterconstructor import construct_router

from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.datasets.gracefo.act1a import GraceFOAct1ADataset
from masschange.datasets.gracefo.ihk1a import GraceFOIhk1ADataset
from masschange.datasets.gracefo.imu1a import GraceFOImu1ADataset
from masschange.datasets.gracefo.mag1a import GraceFOMag1ADataset
from masschange.datasets.gracefo.pci1a import GraceFOPci1ADataset
from masschange.datasets.gracefo.sca1a import GraceFOSca1ADataset

from masschange.datasets.gracefo.act1b import GraceFOAct1BDataset

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.missions import Mission

# Declare all implemented TimeSeriesDataset subclasses here, and API endpoints will be automatically constructed
time_series_dataset_classes: Iterable[Type[TimeSeriesDataset]] = [
    GraceFOAcc1ADataset,
    GraceFOAct1ADataset,
    GraceFOIhk1ADataset,
    GraceFOImu1ADataset,
    GraceFOMag1ADataset,
    GraceFOPci1ADataset,
    GraceFOSca1ADataset,
    GraceFOAct1BDataset
]

missions: Iterable[Type[Mission]] = {dataset.mission for dataset in time_series_dataset_classes}

# Constructs routing for everything in the /missions/{id}/datasets/{id} tree
# This isn't super-clean, but saves having an equivalent "vine" of two-line files to navigate through.
# Feel free to break it out into an import tree if it gets complicated when non-TimeSeriesDataset stuff is implemented
missions_router = APIRouter(prefix='/missions')

for mission in missions:
    mission_router = APIRouter(prefix=f'/{mission.id}')

    mission_datasets_router = APIRouter(prefix='/datasets')

    mission_datasets = [dataset for dataset in time_series_dataset_classes if dataset.mission == mission]

    @mission_datasets_router.get('/', tags=['datasets', 'metadata'])
    def get_available_datasets_for_mission():
        return {'data': [ds.describe() for ds in sorted(mission_datasets, key=lambda ds: ds.id_suffix)]}

    for dataset in mission_datasets:
        dataset_router = construct_router(dataset)
        mission_datasets_router.include_router(dataset_router)

    mission_router.include_router(mission_datasets_router)
    missions_router.include_router(mission_router)


@missions_router.get('/', tags=['missions', 'metadata'])
def get_available_missions():
    return {'data': sorted([mission.id for mission in missions])}
