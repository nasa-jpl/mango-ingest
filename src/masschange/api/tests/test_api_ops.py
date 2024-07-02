import json

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

from masschange.api.app import app
from masschange.api.routers.datasets import SupportedStatisticsEnum
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
    print(
        f'test_gracefo_data_select() for {ds.product.get_full_id()} version {ds.version} instruments {ds.instrument_id}')
    path = f'/missions/{ds.product.mission.id}/products/{ds.product.id_suffix}/versions/{ds.version}/instruments/{ds.instrument_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}'

    # datasets containing multiple distinct time-series require additional parameters to identify a single time-series
    additional_parameters = {
        'TNK1A': '&filter=tank_id=1'
    }
    if ds.product.id_suffix in additional_parameters:
        path += f'{additional_parameters[ds.product.id_suffix]}'

    response = client.get(path)
    content = response.json()

    if response.status_code != 200:
        print(json.dumps(content))
    assert response.status_code == 200

    # Omit variable-data-span-datasets from the test as the expected data count is unknown
    # Currently, anything not 1Hz or 10Hz is assumed to be variable, though this is not always true
    # TODO: Update once variable data span is implemented properly in dataset classes
    if data_span is not None and (ds.product.time_series_interval == timedelta(
            milliseconds=100) or ds.product.time_series_interval == timedelta(seconds=1)):
        if ds.product.id_suffix == 'AHK1A':
            # AHK1A is 1Hz cadence, but with ten rows per 'tick', each covering different fields
            expected_data_count = 600
        else:
            expected_data_count = (test_span_end - test_span_begin) / ds.product.time_series_interval
        assert is_nearly_equal(expected_data_count, content['data_count'])

    expected_attributes = ['from_isotimestamp', 'to_isotimestamp', 'data_begin', 'data_end', 'data_count',
                           'downsampling_factor', 'nominal_data_interval_seconds', 'query_elapsed_ms', 'data']
    for k in expected_attributes:
        assert k in content

    if content['data_count'] > 0:
        sample_datum = content['data'][0]
        assert ds.product.TIMESTAMP_COLUMN_NAME in sample_datum
        for field in ds.product.get_available_fields():
            if field.is_constant:
                # const-valued fields are not served in responses by default.  This may be toggleable in future via a qparam
                continue

            if field.is_lookup_field:
                # lookup fields should not be present unless explicitly requested (which is tested in other test cases)
                assert field.name not in sample_datum
            else:
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
        f'test_gracefo_data_select() for {dataset.product.get_full_id()} version {dataset.version} instruments {dataset.instrument_id}')
    path = f'/missions/{dataset.product.mission.id}/products/{dataset.product.id_suffix}/versions/{dataset.version}/instruments/{dataset.instrument_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}&fields=location'
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


def test_downsampled_location_lookup():
    product = GraceFOAcc1ADataProduct()
    dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion('04'), 'C')
    dataset_data_span = dataset.get_data_span()

    gnv_dataset = TimeSeriesDataset(GraceFOGnv1ADataProduct(), TimeSeriesDatasetVersion('04'), 'C')
    gnv_data_span = gnv_dataset.get_data_span()

    assert dataset_data_span is not None
    assert gnv_data_span is not None

    test_span_begin = max(dataset_data_span.begin, gnv_data_span.begin)
    test_span_end = test_span_begin + timedelta(minutes=1)
    full_res_path = f'/missions/{dataset.product.mission.id}/products/{dataset.product.id_suffix}/versions/{dataset.version}/instruments/{dataset.instrument_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}&fields=location'
    full_res_response = client.get(full_res_path)
    assert full_res_response.status_code == 200
    full_res_content = full_res_response.json()

    downsampling_factor = dataset.product.get_available_downsampling_factors()[2]
    downsampled_path = f'/missions/{dataset.product.mission.id}/products/{dataset.product.id_suffix}/versions/{dataset.version}/instruments/{dataset.instrument_id}/data?fromisotimestamp={test_span_begin.isoformat()}&toisotimestamp={test_span_end.isoformat()}&fields=location&downsampling_factor={downsampling_factor}'
    downsampled_response = client.get(downsampled_path)
    assert downsampled_response.status_code == 200
    downsampled_content = downsampled_response.json()

    downsampled_datum = downsampled_content['data'][1]
    downsampled_bucket_interval = timedelta(seconds=downsampled_content['nominal_data_interval_seconds'])
    bucket_start = datetime.fromisoformat(downsampled_datum['timestamp'])
    bucket_end = bucket_start + downsampled_bucket_interval
    full_res_data = [d for d in full_res_content['data'] if
                     bucket_start <= datetime.fromisoformat(d['timestamp']) < bucket_end]

    # Crude check that downsampled location is at least within the bounding box encompassing its constituent points
    full_res_min_latitude = min(d['location']['latitude'] for d in full_res_data)
    full_res_max_latitude = max(d['location']['latitude'] for d in full_res_data)
    full_res_min_longitude = min(d['location']['longitude'] for d in full_res_data)
    full_res_max_longitude = max(d['location']['longitude'] for d in full_res_data)
    assert full_res_min_latitude <= downsampled_datum['location']['latitude'] <= full_res_max_latitude
    assert full_res_min_longitude <= downsampled_datum['location']['longitude'] <= full_res_max_longitude


def basic_test_metadata():
    path = f'/missions/GRACEFO/products/'
    response = client.get(path)
    assert response.status_code == 200
    content = response.json()

    # TODO: Turn this into an enum derived from Aggregation if it gets much bigger than it is
    recognised_aggregation_types = {'min', 'max', 'avg', 'centroid'}
    for ds in content['data']:
        for field in ds['available_fields']:
            for agg in field['supported_aggregations']:
                assert agg['type'] in recognised_aggregation_types


def basic_test_statistics():
    """Just tests one field of one dataset to ensure endpoints are generally working"""
    product = GraceFOAcc1ADataProduct()
    dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion('04'), 'C')
    stat_span_begin = dataset.get_data_span().begin
    stat_span_end = stat_span_begin + timedelta(days=7)

    assert len(SupportedStatisticsEnum) > 5

    base_path = f'/missions/GRACEFO/products?fromisotimestamp={stat_span_begin.isoformat()}&toisotimestamp={stat_span_end.isoformat()}&field=lin_accl_x'
    for statistic in SupportedStatisticsEnum:
        path = f'{base_path}/{statistic.value}'
        response = client.get(path)
        assert response.status_code == 200
