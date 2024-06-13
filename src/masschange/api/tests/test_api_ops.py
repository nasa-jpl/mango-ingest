import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

from masschange.api.app import app
from masschange.api.tests.utils import is_nearly_equal, permute_all_datasets
from masschange.dataproducts.implementations.gracefo.acc1a import GraceFOAcc1ADataProduct
from masschange.dataproducts.implementations.gracefo.gnv1a import GraceFOGnv1ADataProduct
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

    expected_attributes = ['from_isotimestamp', 'to_isotimestamp', 'data_begin', 'data_end', 'data_count',
                           'downsampling_factor', 'nominal_data_interval_seconds', 'query_elapsed_ms', 'data']
    for k in expected_attributes:
        assert k in content

    if content['data_count'] > 0:
        sample_datum = content['data'][0]
        assert ds.product.TIMESTAMP_COLUMN_NAME in sample_datum
        for field in ds.product.get_reader().get_fields():
            if field.is_constant:
                # const-valued fields are not served in responses by default.  This may be toggleable in future via a qparam
                continue
            assert field.name in sample_datum


def test_location_lookup():
    product = GraceFOAcc1ADataProduct()
    dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion('04'), 'C')
    dataset_data_span = dataset.get_data_span()

    gnv_dataset = TimeSeriesDataset(GraceFOGnv1ADataProduct(), TimeSeriesDatasetVersion('04'), 'C')
    gnv_data_span = gnv_dataset.get_data_span()

    assert dataset_data_span is not None
    assert gnv_data_span is not None

    test_span_begin = max(dataset_data_span.begin, gnv_data_span.begin)
    test_span_end = test_span_begin + timedelta(minutes=1)
    print(
        f'test_gracefo_data_select() for {dataset.product.get_full_id()} version {dataset.version} stream {dataset.stream_id}')
    path = f'/missions/{dataset.product.mission.id}/datasets/{dataset.product.id_suffix}/versions/{dataset.version}/streams/{dataset.stream_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}&fields=location'
    response = client.get(path)
    assert response.status_code == 200
    content = response.json()

    sample_datum = content['data'][0]
    location = sample_datum[product.LOCATION_COLUMN_NAME]
    latitude = location['latitude']
    longitude = location['longitude']
    assert isinstance(latitude, float)
    assert isinstance(longitude, float)
    assert -90.0 <= latitude <= 90.0
    assert -180.0 <= longitude <= 180.0
