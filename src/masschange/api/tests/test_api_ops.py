import json

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

from masschange.api.app import app
from masschange.api.routers.datasets import SupportedStatisticsEnum
from masschange.api.tests.utils import is_nearly_equal, permute_all_datasets
from masschange.dataproducts.implementations.gracefo.primary.acc1a import GraceFOAcc1ADataProduct
from masschange.dataproducts.implementations.gracefo.primary.gnv1a import GraceFOGnv1ADataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


client = TestClient(app)


def test_root():
    response = client.get('/')
    assert response.status_code == 200


timeseries_id_additional_parameters = {
    'TNK1A': '&filter=tank_id=1',
    'TNK1B': '&filter=tank_id=1',
    'SCA1A': '&filter=sca_id=1',
    'SCA1B': '&filter=sca_id=1',
    'IMU1A': '&filter=gyro_id=1',
    'IMU1B': '&filter=gyro_id=1',
    'IHK1A': '&filter=sensorname=39',
    'IHK1B': '&filter=sensorname=39',
    'CLK1B': '&filter=clock_id=-1',
    'GNV1A_PRN': '&filter=prn_id=3',
    'GPS1A': '&filter=prn_id=7&filter=ant_id=0',
    'TIM1B': '&filter=ts_suppid=0',
    'LHK1B': '&filter=sensorname=TMA_PMH_CURRENT',
    'LLT1A': '&filter=rcv_id=C&filter=trx_id=D',
    'PLT1A': '&filter=rcv_id=C&filter=trx_id=D',
    'QSA1B': '&filter=sca_id=1'
}


@pytest.mark.parametrize("ds", permute_all_datasets())
def test_gracefo_data_select(ds: TimeSeriesDataset):
    data_span = ds.get_data_span()
    test_span_begin = data_span.begin if data_span is not None else datetime(2000, 1, 1)
    test_span_end = test_span_begin + timedelta(minutes=1)

    print(
        f'test_gracefo_data_select() for {ds.product.get_full_id()} version {ds.version} instruments {ds.instrument_id}')
    path = f'/missions/{ds.product.mission.id}/products/{ds.product.id_suffix}/versions/{ds.version}/instruments/{ds.instrument_id}/data?from_isotimestamp=' \
           f'{test_span_begin.isoformat()[:19]}&to_isotimestamp={test_span_end.isoformat()[:19]}'
    # datasets containing multiple distinct time-series require additional parameters to identify a single time-series

    if ds.product.id_suffix in timeseries_id_additional_parameters:
        path += f'{timeseries_id_additional_parameters[ds.product.id_suffix]}'

    response = client.get(path)
    content = response.json()

    if response.status_code != 200:
        print(json.dumps(content))
    assert response.status_code == 200

    # Omit variable-data-span-datasets from the test as the expected data count is unknown
    # Currently, anything not 1Hz or 10Hz is assumed to be variable, though this is not always true
    # TODO: Update once variable data span is implemented properly in dataset classes

    if isinstance(ds.product, TimeSeriesDataProduct):
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
    else:

        # no expected data count for non-time-series products
        # no 'downsampling_factor', 'nominal_data_interval_seconds' for non-time-series products
        expected_attributes = ['from_isotimestamp', 'to_isotimestamp', 'data_begin', 'data_end', 'data_count',
                             'query_elapsed_ms', 'data']
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


@pytest.mark.parametrize("ds", permute_all_datasets())
def test_gracefo_data_stats(ds: TimeSeriesDataset):
    data_span = ds.get_data_span()
    test_span_begin = data_span.begin if data_span is not None else datetime(2000, 1, 1)
    test_span_end = test_span_begin + timedelta(minutes=1)

    print(
        f'test_gracefo_data_select() for {ds.product.get_full_id()} version {ds.version} instruments {ds.instrument_id}')

    test_fields = [f for f in ds.product.get_available_fields() if f.is_aggregable]
    for field in test_fields:
        path = f'/missions/{ds.product.mission.id}/products/{ds.product.id_suffix}/versions/{ds.version}/instruments/{ds.instrument_id}/fields/{field.name}/statistics/avg?from_isotimestamp=' \
               f'{test_span_begin.isoformat()[:19]}&to_isotimestamp={test_span_end.isoformat()[:19]}'
        # datasets containing multiple distinct time-series require additional parameters to identify a single time-series

        if ds.product.id_suffix in timeseries_id_additional_parameters:
            path += f'{timeseries_id_additional_parameters[ds.product.id_suffix]}'

        response = client.get(path)
        content = response.json()

        if response.status_code != 200:
            print(json.dumps(content))
        assert response.status_code == 200

        expected_attributes = ['from_isotimestamp', 'to_isotimestamp', 'field', 'statistic', 'result', 'query_elapsed_ms']
        for k in expected_attributes:
            assert k in content


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


def test_product_metadata_basic():
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


@pytest.mark.parametrize("ds", permute_all_datasets())
def test_dataset_metadata(ds: TimeSeriesDataset):
    path = f'/missions/{ds.product.mission.id}/products/{ds.product.id_suffix}/versions/{ds.version}/instruments/{ds.instrument_id}'
    response = client.get(path)
    content = response.json()

    if response.status_code != 200:
        print(json.dumps(content))
    assert response.status_code == 200

    if isinstance(ds.product, TimeSeriesDataProduct):
        expected_attributes = ['description', 'mission', 'id', 'full_id', 'processing_level', 'instruments',
                           'available_fields', 'available_resolutions', 'timestamp_field', 'query_result_limit',
                           'data_begin', 'data_end', 'last_updated']
    else:
        expected_attributes = ['description', 'mission', 'id', 'full_id', 'processing_level', 'instruments',
                               'available_fields', 'timestamp_field', 'query_result_limit',
                               'data_begin', 'data_end', 'last_updated']

    for k in expected_attributes:
        assert k in content

    expected_field_attributes = ['name', 'type', 'description', 'unit', 'supported_aggregations',
                                 'is_time_series_id']
    for field in content['available_fields']:
        for k in expected_field_attributes:
            assert k in field
        if field['is_time_series_id'] is True:
            assert 'enum_values' in field


def test_statistics_basic():
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
