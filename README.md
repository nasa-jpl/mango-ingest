# gma-data-backend
Gravity Missions Analysis Tool data ingestion and API

## Prerequisites
- a running TimescaleDB instance
- environment variables set
  ```bash
  export TSDB_HOST='localhost';
  export TSDB_PORT='5433';
  export TSDB_USER='postgres';
  export TSDB_PASSWORD='password';
  export TSDB_DATABASE='masschange';
  
  # optionally, for prod deployment behind a proxy server
  export API_ROOT_PATH='/mango/api/'
  export API_PROXY_HOST='***REMOVED***'
  ```
  

## Docker Quickstart (OUTDATED)
1. Clone repository
2. Build image with `docker build -t masschange path/to/repository`
3. Ensure that input root directory exists and contains data
4. Ensure that output root exists and is globally-writable (this will be removed as a requirement at some point)
4. Run GRACE-FO sample data ingest with `docker run -v path/to/input-data-root:/input -v path/to/output-data-root:/data masschange conda run -n masschange python /app/masschange/src/masschange/ingest/datasets/gracefo/ingest.py /input /data`
5. Confirm presence of ingested data in output root directory

## Dev Quickstart

### Dependencies
 - `conda`

1. Clone repository
2. Create conda env with `conda env create --file ./environment.yml`
3. Activate conda env with `conda activate masschange`
4. Install editable masschange package with `pip install -e /app/masschange`
4. Run ingestion on GRACE-FO data location with `python ./masschange/ingest/datasets/gracefo/ingest.py --dataset GRACEFO_ACC1A --src path/to/input_data_root ` (add `--zipped` if data is in tarballs)

#### To update existing conda environment
1. Edit ./environment.yml
2. Activate conda env with `conda activate masschange`
3. Update env with `conda env update --file ./environment.yml --prune`

### Tests

Run functional and unit tests from repository root with 

`PYTHONPATH='./src' && python -m unittest discover`

A test database named `masschange_functional_tests` will be created, written to, and deleted during functional test execution.

## Dockerized API Deployment (OUTDATED)

### bigdata.jpl.nasa.gov
After building the image from a fresh clone of the repository with 

```docker image build --tag masschange .```

Run a container, exposing the API on port `5463`, with 

```docker container run --name gma-data-backend-api --publish 5463:8000 --volume /data/share/datasets/gravity-missions-analysis-tool/ingested-data/:/data -e MASSCHANGE_DATA_ROOT=/data masschange:latest conda run -n masschange /app/masschange/start_api.sh```

Once running, the endpoints may be tested from your local environment by opening an SSH tunnel with 

```
ssh -L 5463:localhost:5463 bigdata
```

Documentation will then be available at http://0.0.0.0:5463/docs
