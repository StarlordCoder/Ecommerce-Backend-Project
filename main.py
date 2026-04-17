from fastapi import FastAPI
from routes import router

app = FastAPI(title="FastAPI MongoDB Auth")

app.include_router(router)
