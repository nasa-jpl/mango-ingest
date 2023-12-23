import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse

from masschange.api.missions import main as missions

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/', include_in_schema=False)
def view_documentation_message(request: Request):
    documentation_url = f'{request.url}docs'
    return HTMLResponse(
        f'Welcome to the MassChange API!  View interactive documentation <a href="{documentation_url}">HERE</a>')


app.include_router(missions.missions_router)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
