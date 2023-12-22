from typing import Type, Iterable

from fastapi import APIRouter
from masschange.api.missions.gracefo.datasets.datasetrouterconstructor import construct_router
from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.datasets.timeseriesdataset import TimeSeriesDataset

router = APIRouter(prefix='/datasets')

dataset_classes: Iterable[Type[TimeSeriesDataset]] = [
    GraceFOAcc1ADataset
]

for DatasetCls in dataset_classes:
    router.include_router(construct_router(DatasetCls))

