import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

from masschange.api.app import app
from masschange.api.tests.utils import is_nearly_equal, permute_all_datasets
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion

client = TestClient(app)


def test_root():
    response = client.get('/')
    assert response.status_code == 200


@pytest.mark.parametrize("ds", permute_all_datasets())
def test_gracefo_data_select(ds: TimeSeriesDataset):

    data_span = ds.get_data_span()
    test_span_begin = data_span.begin if data_span is not None else datetime(2000, 1, 1)
    test_span_end = test_span_begin + timedelta(minutes=1)
    print(f'test_gracefo_data_select() for {ds.product.get_full_id()} version {ds.version} stream {ds.stream_id}')
    path = f'/missions/{ds.product.mission.id}/datasets/{ds.product.id_suffix}/versions/{ds.version}/streams/{ds.stream_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}'
    response = client.get(path)
    assert response.status_code == 200
    content = response.json()

    # Omit variable-data-span-datasets from the test as the expected data count is unknown
    # Currently, anything not 1Hz or 10Hz is assumed to be variable, though this is not always true
    # TODO: Update once variable data span is implemented properly in dataset classes
    if data_span is not None and (ds.product.time_series_interval == timedelta(
            milliseconds=100) or ds.product.time_series_interval == timedelta(seconds=1)):
        assert is_nearly_equal(600, content['data_count'])
