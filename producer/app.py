# producer/app.py

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict
import httpx
from datetime import datetime
import asyncio

app = FastAPI()

class Item(BaseModel):
    asset: str
    readings: Dict[str, float] = None
    timestamp: Optional[str] = None

@app.post("/items/")
async def create_item(item: Item):
    item.timestamp = datetime.utcnow().isoformat()

    async with httpx.AsyncClient() as client:
        r = await client.post("http://consumer:8001/items/", data=item.json())
        return {"response": r.json()}
    
async def send_message(item: Item, hertz, number):
    delay = 1.0 / float(hertz)
    headers = {'Content-Type': 'application/json'}
    async with httpx.AsyncClient() as client:
        for _ in range(number):
            item.timestamp = datetime.utcnow().isoformat()
            r = await client.post("http://consumer:8001/items/", data=item.model_dump_json(), headers=headers)
            await asyncio.sleep(delay)

@app.post("/items/{hertz}")
async def create_item(item: Item, hertz: str, number: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_message, item, hertz, number)
    return {"status": "Item sending scheduled"}