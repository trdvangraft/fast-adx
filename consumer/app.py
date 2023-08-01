from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, Dict

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    KustoStreamingIngestClient,
    IngestionProperties,
    StreamDescriptor
)
from azure.kusto.data.data_format import DataFormat

import io
from datetime import datetime
import json
import os

app = FastAPI()

CLIENT_ID=os.getenv("CLIENT_ID")
CLIENT_SECRET=os.getenv("CLIENT_SECRET")
TENANT_ID=os.getenv("TENANT_ID")
CONNECTION_STRING=os.getenv("CONNECTION_STRING")

csb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    connection_string=CONNECTION_STRING,
    aad_app_id=CLIENT_ID,
    app_key=CLIENT_SECRET,
    authority_id=TENANT_ID
)

client = KustoStreamingIngestClient(csb)
ingestionProperties = IngestionProperties(
    database="adx-python-test",
    table="readings",
    data_format=DataFormat.JSON,
    ingestion_mapping_reference="readings_mapping"
)


class Item(BaseModel):
    asset: str
    readings: Dict[str, float] = None
    timestamp: Optional[str] = None

@app.post("/items/")
async def create_item(item: Item):
    print("Received item:", item)

    json_string = item.model_dump_json()

    stream = io.StringIO(json_string)
    stream_descriptor = StreamDescriptor(stream)
    client.ingest_from_stream(
        stream_descriptor, ingestionProperties
    )

    return {"status": "Item received"}