import os
import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from typing import List
from pymongo import MongoClient
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables if .env exists
load_dotenv(".env") if os.path.exists(".env") else None

# Kafka & Mongo settings
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka1:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "earthquakes-topic")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017")
SAVE_TO_MONGO = os.environ.get("SAVE_TO_MONGO", "true").lower() in ("1", "true", "yes")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[""],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=[""],
    allow_headers=["*"],
)

# --- WebSocket Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: str):
        to_remove = []
        for ws in self.active:
            try:
                await ws.send_text(message)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            self.disconnect(ws)

manager = ConnectionManager()

# --- Optional MongoDB client ---
mongo_client = None
raw_collection = None
if SAVE_TO_MONGO:
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client.get_database("earthquake_db")
    raw_collection = mongo_db.get_collection("raw_earthquakes")

# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await asyncio.sleep(10)  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(ws)

# --- Kafka consumer background task with retry ---
async def consume_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    # Retry until Kafka is available
    while True:
        try:
            print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP}...")
            await consumer.start()
            print("Connected to Kafka!")
            break
        except KafkaConnectionError:
            print("Kafka not ready, retrying in 5 seconds...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            payload = msg.value
            # Optional Mongo insert
            if SAVE_TO_MONGO and raw_collection is not None:
                try:
                    raw_collection.insert_one(payload)
                except Exception as e:
                    print("Mongo insert error:", e)
            # Broadcast to websockets
            await manager.broadcast(json.dumps(payload, default=str))
    finally:
        await consumer.stop()
        print("Kafka consumer stopped.")

# --- FastAPI startup event ---
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    loop.create_task(consume_loop())
    print("Kafka consumer background task scheduled.")

# --- Simple health endpoint ---
@app.get("/")
def read_root():
    return {"status": "ok"}
