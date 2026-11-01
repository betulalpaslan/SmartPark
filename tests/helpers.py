import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def install_data_ingestion_stubs():
    requests = types.ModuleType("requests")
    requests.get = lambda *args, **kwargs: None
    sys.modules["requests"] = requests

    # data-ingestion "from curl_cffi import requests" yapiyor; stub oraya kurulmali.
    curl_cffi = types.ModuleType("curl_cffi")
    curl_cffi.requests = requests
    sys.modules["curl_cffi"] = curl_cffi
    sys.modules["curl_cffi.requests"] = requests

    pika = types.ModuleType("pika")
    pika.BlockingConnection = object
    pika.URLParameters = lambda value: value
    pika.BasicProperties = lambda **kwargs: kwargs
    sys.modules["pika"] = pika

    apscheduler = types.ModuleType("apscheduler")
    schedulers = types.ModuleType("apscheduler.schedulers")
    background = types.ModuleType("apscheduler.schedulers.background")
    background.BackgroundScheduler = object
    sys.modules["apscheduler"] = apscheduler
    sys.modules["apscheduler.schedulers"] = schedulers
    sys.modules["apscheduler.schedulers.background"] = background


def install_fastapi_service_stubs():
    fastapi = types.ModuleType("fastapi")

    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def add_middleware(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def websocket(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    class HTTPException(Exception):
        def __init__(self, status_code=None, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi.FastAPI = FastAPI
    fastapi.HTTPException = HTTPException
    fastapi.Request = object
    sys.modules["fastapi"] = fastapi

    cors = types.ModuleType("fastapi.middleware.cors")
    cors.CORSMiddleware = object
    sys.modules["fastapi.middleware"] = types.ModuleType("fastapi.middleware")
    sys.modules["fastapi.middleware.cors"] = cors

    responses = types.ModuleType("fastapi.responses")
    responses.JSONResponse = dict
    sys.modules["fastapi.responses"] = responses

    starlette_base = types.ModuleType("starlette.middleware.base")
    starlette_base.BaseHTTPMiddleware = object
    sys.modules["starlette"] = types.ModuleType("starlette")
    sys.modules["starlette.middleware"] = types.ModuleType("starlette.middleware")
    sys.modules["starlette.middleware.base"] = starlette_base

    httpx = types.ModuleType("httpx")

    class RequestError(Exception):
        pass

    class HTTPStatusError(Exception):
        pass

    class AsyncClient:
        def __init__(self, *args, **kwargs):
            pass

    httpx.RequestError = RequestError
    httpx.HTTPStatusError = HTTPStatusError
    httpx.AsyncClient = AsyncClient
    sys.modules["httpx"] = httpx

    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.connect = lambda *args, **kwargs: None
    extras = types.ModuleType("psycopg2.extras")
    extras.RealDictCursor = object
    psycopg2.extras = extras
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = extras
