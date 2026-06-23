from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
import asyncio
import json
from collections import defaultdict

app = FastAPI()

REDIS_URL = "rediss://default:vN7dv7ziHZceqpPu0s3MJ5ec04DBed78@redis-14511.crce217.ap-south-1-1.ec2.cloud.redislabs.com:14511"

redis_client = redis.from_url(REDIS_URL)

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}  # user_id -> websocket

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

        await redis_client.sadd("online_users", user_id)

    def disconnect(self, user_id: str):
        self.active_connections.pop(user_id, None)

    async def send_personal(self, user_id: str, payload: dict):
        ws = self.active_connections.get(user_id)
        if ws:
            try:
                await ws.send_json(payload)
            except Exception as e:
                print("Send error:", e)

    async def send_group(self, channel: str, payload: dict):
       
        members = await redis_client.smembers(f"channel:{channel}")

        for user_id in members:
            ws = self.active_connections.get(user_id)
            if ws:
                try:
                    await ws.send_json(payload)
                except Exception as e:
                    print("Group send error:", e)


manager = ConnectionManager()

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):

    await manager.connect(websocket, user_id)

    await redis_client.sadd(f"channel:{user_id}", user_id)

    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")
            
            if action == "ping":
                await websocket.send_json({"action": "pong"})
                continue

            if action == "subscribe":
                channel = message["channel"]
                await redis_client.sadd(f"channel:{channel}", user_id)
                continue

            if action == "unsubscribe":
                channel = message["channel"]
                await redis_client.srem(f"channel:{channel}", user_id)
                continue

            if action == "publish":

                payload = {
                    "from": user_id,
                    "channel": message.get("channel"),
                    "event": message["event"],
                    "data": message["data"],
                    "to": message.get("to")
                }

                await redis_client.publish(
                    "socket_events",
                    json.dumps(payload)
                )

    except WebSocketDisconnect:
        manager.disconnect(user_id)
        await redis_client.srem("online_users", user_id)


async def redis_listener():

    pubsub = redis_client.pubsub()
    await pubsub.subscribe("socket_events")

    async for message in pubsub.listen():

        if message["type"] != "message":
            continue

        payload = json.loads(message["data"])

        if payload.get("to"):
            await manager.send_personal(
                payload["to"],
                payload
            )
            continue

        channel = payload.get("channel")

        if channel:
            await manager.send_group(channel, payload)


@app.on_event("startup")
async def startup():
    asyncio.create_task(redis_listener())


@app.get("/")
async def home():
    return {
        "success": True,
        "message": "Realtime Redis WebSocket Server Running"
    }