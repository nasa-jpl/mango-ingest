# gma-data-backend
Gravity Missions Analysis Tool data ingestion and API

## Docker Quickstart
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
4. Run ingestion on GRACE-FO data location with `python ./masschange/ingest/datasets/gracefo/ingest.py path/to/input_data_root path/to/output_data_root` (add `--zipped` if data is in tarballs)
