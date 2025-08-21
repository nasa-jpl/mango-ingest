import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse

from masschange.api.routers.missions import router as missions_router
from masschange.api.routers.dataproducts import router as dataproducts_router
from masschange.api.routers.datasets import router as datasets_router
from masschange.api.routers.webui import router as webui_router
from masschange.api.routers.ingestion import router as ingestion_router
from masschange.api.utils.memleak import run_memleak_cleanup_thread

# Set log level to INFO by default - this may need to be refined later
logging.root.setLevel(logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Runs anything before the yield prior to app startup, and runs anything after the yield upon app exit"""

    # Run a memory cleanup service to compensate for a memory leak inherent to Python
    # See details at https://github.com/python/cpython/issues/109534
    logging.info('Running memory cleanup thread in background')
    run_memleak_cleanup_thread()

    yield


app = FastAPI(lifespan=lifespan)

# TODO: Resolve case where this is not set - should it throw a descriptive error or continue gracefully?
origins = [
    os.environ["API_PROXY_HOST"],
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/', include_in_schema=False)
def view_documentation_message(request: Request):
    api_host_override = os.environ.get('API_PROXY_HOST')
    if api_host_override is not None:
        root_url = f'{request.url.scheme}://{api_host_override}{request.url.path}'
    else:
        root_url = str(request.url)[:-1] if str(request.url).endswith('//') else str(request.url)

    documentation_url = f'{root_url}docs'
    return HTMLResponse(
        f'Welcome to the MassChange API!  View interactive documentation <a href="{documentation_url}">HERE</a>')


dataproducts_router.include_router(datasets_router, prefix='/{product_id_suffix}')
missions_router.include_router(dataproducts_router, prefix='/{mission_id}/products')
app.include_router(missions_router, prefix='/missions')
app.include_router(webui_router, prefix='/ui')

app.include_router(ingestion_router, prefix='/ingestion', tags=['ingestion'])

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
