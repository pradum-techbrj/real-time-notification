from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, Any
import redis.asyncio as redis
import asyncio
import json
import uvicorn
from collections import defaultdict

app = FastAPI()

REDIS_URL = "redis://default:vN7dv7ziHZceqpPu0s3MJ5ec04DBed78@redis-14511.crce217.ap-south-1-1.ec2.cloud.redislabs.com:14511"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

class ConnectionManager:

    def __init__(self):
        self.active_connections = {}
        self.channel_subscribers = defaultdict(set)

    async def connect(self, websocket, websocket_id):
        await websocket.accept()

        self.active_connections[websocket_id] = websocket


    def disconnect(self, websocket_id):

        self.active_connections.pop(
            websocket_id,
            None
        )
        
        for subscribers in self.channel_subscribers.values():

            subscribers.discard(websocket_id)
            
    def subscribe(self, websocket_id, channel):
        self.channel_subscribers[channel].add(
            websocket_id
        )
        
    def unsubscribe(self, websocket_id, channel):

        self.channel_subscribers[channel].discard(
            websocket_id
        )
        
    async def send_to_channel(self, channel, payload):

        subscribers = self.channel_subscribers.get(
            channel,
            set()
        )
        
        for websocket_id in subscribers:

            websocket = self.active_connections.get(
                websocket_id
            )

            if websocket:

                try:

                    await websocket.send_json(payload)

                except:

                    pass


manager = ConnectionManager()


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str
    ):

    websocket_id = str(id(websocket))

    await manager.connect(
        websocket,
        websocket_id
    )

    manager.subscribe(
        websocket_id,
        user_id
    )

    try:

        while True:

            message = await websocket.receive_json()

            action = message.get("action")

            if action == "subscribe":

                manager.subscribe(
                    websocket_id,
                    message["channel"]
                )

            elif action == "unsubscribe":

                manager.unsubscribe(
                    websocket_id,
                    message["channel"]
                )

            elif action == "publish":

                payload = {
                    "from": user_id,
                    "channel": message["channel"],
                    "event": message["event"],
                    "data": message["data"]
                }

                await redis_client.publish(
                    "socket_events",
                    json.dumps(payload)
                )

    except WebSocketDisconnect:

        manager.disconnect(websocket_id)

async def redis_listener():

    pubsub = redis_client.pubsub()

    await pubsub.subscribe("socket_events")

    async for message in pubsub.listen():

        if message["type"] != "message":
            continue

        payload = json.loads(message["data"])

        await manager.send_to_channel(
            payload["channel"],
            payload
        )


@app.on_event("startup")
async def startup():

    asyncio.create_task(
        redis_listener()
    )


@app.get("/")
async def home():

    return {
        "success": True,
        "message": "Realtime Redis Socket Server Running"
    }
