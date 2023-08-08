import json
import logging
import os

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# from fastapi_utils.timing import add_timing_middleware  # TODO: resolve/re-check pydantic v1/v2 dependency conflict between fastapi_utils and recent fastapi versions

from masschange.api.datasets import main as datasets
from masschange.datasets.interface import get_spark_session

app = FastAPI()

# add_timing_middleware(app, record=logging.getLogger(__name__).info, prefix='api')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasets.router)

# @app.get('/last-ingest', tags=['monitoring'])
# async def last_ingest_status():
#     last_ingest_status_filepath = os.path.join(os.environ["VIRGO_DATA_ROOT"], 'last_ingest_status.json')
#
#     with open(last_ingest_status_filepath) as data:
#         return json.load(data)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
