import json

from fastapi.testclient import TestClient
from datetime import datetime, date

from masschange.api.app import app

client = TestClient(app)

#
# def test_gps_sites():
#     response = client.get("/datasets/gps/sites")
#     assert response.status_code == 200
#     assert 100 < len(response.json()['features']) < 200
#     assert response.json()['features'][0] == {
#         'type': 'Feature',
#         'geometry':
#             {
#                 'coordinates': [-120.265, 36.429, 56.523],
#                 'type': 'Point'
#             },
#         'id': '5PTS',
#         'properties': {},
#     }
#
#
# def test_gps_data_spatial_subset():
#     # This json should yield measurements from sites CAFP and 5PTS only
#     query_geojson = {
#         "type": "Feature",
#         "geometry": {
#             "type": "Polygon",
#             "coordinates": [[0.0, 36.42], [-180.0, 36.42], [-180.0, 36.43], [0.0, 36.43]]
#         }
#     }
#
#     response = client.get("/datasets/gps/data",
#                           params={'spatial_bounds': json.dumps(query_geojson)})
#
#     assert response.status_code == 200
#     assert 2000 < len(response.json()['features']) < 10000
#     assert set([datum['properties']['site_id'] for datum in response.json()['features']]) == {'CAFP', '5PTS'}
#
#
# def test_wells_sites():
#     response = client.get("/datasets/wells/sites")
#     assert response.status_code == 200
#     assert 5000 < len(response.json()['features']) < 80000
#
#     target_site = next(filter(lambda site: site.get('id') == 'C_01N04E36Q001M', response.json()['features']))
#     assert target_site['type'] == 'Feature'
#     assert target_site['geometry'] == {
#             'type': 'Point',
#             'coordinates': [-121.48183333, 37.88605556]
#         }
#     assert target_site['properties']['description'] == 'Middle River Barrier well 2W screen 61-81 ft bgs'
#     assert target_site['properties']['ground_surface_elevation'] == 10.3
#     assert target_site['properties']['well_depth'] == 81.0
#     assert  -175.0 < target_site['properties']['latest_groundwater_elevation'] < -100.0
#     assert target_site['properties']['latest_groundwater_depth_from_surface'] == target_site['properties']['ground_surface_elevation'] - target_site['properties']['latest_groundwater_elevation']
#     assert date.fromisoformat(target_site['properties']['latest_groundwater_elevation_timestamp']) > date(2020, 1, 1)
#     assert -3.0 < target_site['properties']['latest_groundwater_elevation_stddevs_from_historical_mean'] < 3.0
#     assert -50.0 < target_site['properties']['latest_groundwater_elevation_historical_mean'] < 50.0
#     assert target_site['properties']['earliest_year_with_data'] == 2005
#     assert target_site['properties']['data_count'] > 5  # this will depend on calendar month of last measurement
#
#
# def test_wells_integrated_data_subset():
#     # This json should yield measurements from sites ?????? only
#     query_geojson = {
#         "type": "Feature",
#         "geometry": {
#             "type": "Polygon",
#             "coordinates": [[0.0, 33.0], [0.0, 34.0], [-180.0, 34.0], [-180.0, 33.0]]
#         }
#     }
#
#     response = client.get("/datasets/wells/data",
#                           params={
#                               'spatial_bounds': json.dumps(query_geojson),
#                               'before': datetime(2019, 11, 5),
#                               'after': datetime(2019, 11, 5)
#                           })
#
#     assert response.status_code == 200
#     assert len(response.json()['features']) == 1
#     assert set([datum['properties']['site_id'] for datum in response.json()['features']]) == {'P_335006N1176517W001'}
#
#
# def test_wells_long_term_mean():
#     query_geojson = {
#         "type": "Feature",
#         "geometry": {
#             "type": "Polygon",
#             "coordinates": [[-116.229, 33.6385], [-116.227, 33.6385], [-116.227, 33.6387], [-116.229, 33.6387]]
#         }
#     }
#
#     response = client.get("/datasets/wells/data/long-term-means",
#                           params={'spatial_bounds': json.dumps(query_geojson)})
#
#     assert response.status_code == 200
#     assert set(response.json().keys()) == {'P_382900N1162285W001'}
#     assert set(response.json()['P_382900N1162285W001']['months'].keys()) == set([str(i) for i in range(1, 13)])
#
#
# # def test_wells_last_known_groundwater_elevation():
# #     query_geojson = {
# #         "type": "Feature",
# #         "geometry": {
# #             "type": "Polygon",
# #             "coordinates": [[-116.229, 33.6385], [-116.227, 33.6385], [-116.227, 33.6387], [-116.229, 33.6387]]
# #         }
# #     }
# #
# #     response = client.get("/datasets/wells/data/latest",
# #                           params={'spatial_bounds': json.dumps(query_geojson)})
# #     assert response.status_code == 200
# #     datum = response.json()['features'][0]
# #     assert set(datum['properties'].keys()) == {'timestamp', 'site_id', 'year', 'month', 'mean_groundwater_elevation'}
# #     assert datum['properties']['site_id'] == 'P_382900N1162285W001'
#
#
# def test_wells_monthly_mean_timeseries():
#     query_geojson = {
#         "type": "Feature",
#         "geometry": {
#             "type": "Polygon",
#             "coordinates": [[33.6385, -116.229], [33.6385, -116.227], [33.6387, -116.227], [33.6387, -116.229]]
#         }
#     }
#
#     response = client.get("/datasets/wells/data/monthly",
#                           params={
#                               'spatial_bounds': json.dumps(query_geojson),
#                               'from_dt': date(2017, 1, 1),
#                               'to_dt': date(2018, 1, 1)
#                           })
#     assert response.status_code == 200
#     assert 1 < len(response.json()['features']) < 12
#     assert set(map(lambda x: date.fromisoformat(x['properties']['timestamp']).year, response.json()['features'])) == {2017}
#     assert set([x['properties']['site_id'] for x in response.json()['features']]) == {'P_382900N1162285W001'}
#
#
# def test_wells_site_by_id():
#     test_site_id = 'C_01N04E36Q001M'
#
#     response = client.get(f'/datasets/wells/sites/{test_site_id}')
#     assert response.status_code == 200
#     assert len(response.json()['features']) == 1
#
#     properties = response.json()['features'][0]['properties']
#     expected_properties = {'description', 'ground_surface_elevation', 'well_depth', 'latest_groundwater_elevation',
#         'latest_groundwater_elevation_timestamp', 'latest_groundwater_elevation_stddevs_from_historical_mean',
#         'latest_groundwater_elevation_historical_mean', 'earliest_year_with_data', 'data_count', 'subbasin',
#         'latest_groundwater_depth_from_surface', 'historical_mean_groundwater_elevation_by_month'}
#
#     assert set(properties.keys()) == expected_properties
#     assert properties['subbasin'] == '5-022.15'
#     assert {int(i) for i in properties['historical_mean_groundwater_elevation_by_month'].keys()} == set(range(1,13))
#     assert all([-75 < val < 25 for val in properties['historical_mean_groundwater_elevation_by_month'].values()])
#
#     latest_groundwater_month = properties['latest_groundwater_elevation_timestamp'][5:7]
#     assert properties['historical_mean_groundwater_elevation_by_month'][latest_groundwater_month] == properties['latest_groundwater_elevation_historical_mean']
#
#
# def test_wells_site_monthly_mean_timeseries():
#     test_site_id = 'C_01N04E36Q001M'
#
#     response = client.get(f'/datasets/wells/sites/{test_site_id}/monthly',
#                           params={})
#
#     expected_properties = {'timestamp', 'site_id', 'site_ground_surface_elevation', 'year', 'month',
#                            'mean_groundwater_elevation', 'mean_groundwater_stddevs_from_historical_mean', 'data_count',
#                            'subbasin'}
#
#     assert response.status_code == 200
#     assert 100 < len(response.json()['features']) < 1000
#     assert set([x['properties']['site_id'] for x in response.json()['features']]) == {'C_01N04E36Q001M'}
#     assert [set(x['properties'].keys()) == expected_properties for x in response.json()['features']]
#
#
# def test_grace_monthly_mean_timeseries():
#     test_subbasin = '5-006.01'
#     test_start_date_str = '2003-01-01'
#     test_end_date_str = '2003-12-31'
#
#     response = client.get(f'datasets/grace/data/monthly/aggregated/subbasins?subbasin={test_subbasin}&from_dt={test_start_date_str}&to_dt={test_end_date_str}',
#                           params={})
#
#     expected_properties = {'timestamp', 'calendar_month', 'calendar_month_historical_mean', 'mean_groundwater_value',
#                            'mean_groundwater_stddevs_from_historical_mean', 'subbasin', 'subbasin_coverage'}
#
#     assert response.status_code == 200
#     assert len(response.json()['data']) == 11  # no GRACE data prior to Feb 2003
#     assert [set(x.keys()) == expected_properties for x in response.json()['data']]
#     assert response.json()['data'][0]['timestamp'] == '2003-02-01'
#     assert response.json()['data'][0]['calendar_month'] == 2
#     assert  -3.0 < response.json()['data'][0]['calendar_month_historical_mean'] < 3.0
#     assert  -2.0 < response.json()['data'][0]['subbasin_coverage'] < 4.0
#
#
#
#
# def test_regions_subbasins():
#     response = client.get(f'/regions/subbasins',
#                           params={})
#
#     assert len(response.json()['features']) == 39
#     test_subbasin = response.json()['features'][0]
#     assert test_subbasin['geometry']['type'] == 'Polygon'
#     assert test_subbasin['properties']['OBJECTID'] == 351
#     assert test_subbasin['properties']['Basin_Numb'] == '5-006'
#     assert test_subbasin['properties']['Basin_Subb'] == '5-006.04'
#
#
#
