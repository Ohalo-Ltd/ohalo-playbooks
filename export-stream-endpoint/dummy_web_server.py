import json
import time

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()


def generate_jsonl():
    for i in range(10):
        yield json.dumps({"id": i, "value": f"item_{i}"}) + "\n"
        time.sleep(0.5)


@app.get("/api/indexed-files")
def export_jsonl():
    return StreamingResponse(generate_jsonl(), media_type="application/jsonl")
