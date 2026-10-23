import asyncio
import json
import math
import os
import re
import secrets
import time
from pathlib import Path

from aiohttp import WSMsgType, web

BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
WORLD_WIDTH = 2400
WORLD_HEIGHT = 1800
MAX_PLAYERS = 40
BROADCAST_INTERVAL = 0.12  # about 8 live snapshots per second
clients = {}  # WebSocketResponse -> player state; one shared game room per process


def clean_name(value, fallback):
    raw = str(value or "")
    raw = "".join(ch for ch in raw if ch.isprintable() and ch not in "<>/\\")
    raw = re.sub(r"\s+", " ", raw).strip()[:18]
    return raw or fallback


def safe_float(value, fallback, low, high):
    try:
        number = float(value)
        if not math.isfinite(number):
            return fallback
        return max(low, min(high, number))
    except (TypeError, ValueError, OverflowError):
        return fallback


def public_player(state):
    return {
        "id": state["id"],
        "name": state["name"],
        "x": state["x"],
        "y": state["y"],
        "angle": state["angle"],
        "driving": state["driving"],
        "vehicleId": state["vehicleId"],
    }


async def index_page(_request):
    response = web.FileResponse(INDEX_FILE)
    response.headers["Cache-Control"] = "no-store"
    return response


async def health(_request):
    return web.json_response({"status": "ok", "players": len(clients)})


async def multiplayer_socket(request):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=4096, compress=False)
    await ws.prepare(request)

    if len(clients) >= MAX_PLAYERS:
        await ws.send_json({"type": "error", "message": f"شهر فعلاً پر است؛ سقف این نسخه {MAX_PLAYERS} بازیکن است."})
        await ws.close(code=1008, message=b"room full")
        return ws

    player_id = secrets.token_hex(5)
    offset = len(clients) % 8
    state = {
        "id": player_id,
        "name": f"شهروند-{player_id[-4:].upper()}",
        "x": 1480 + (offset % 4) * 26,
        "y": 890 + (offset // 4) * 30,
        "angle": math.pi,
        "driving": False,
        "vehicleId": "hatch",
        "last_seen": time.monotonic(),
    }
    clients[ws] = state
    await ws.send_json({"type": "welcome", "id": player_id, "x": state["x"], "y": state["y"]})

    try:
        async for message in ws:
            if message.type == WSMsgType.TEXT:
                try:
                    data = json.loads(message.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(data, dict):
                    continue
                if data.get("type") == "ping":
                    # Small round-trip probe so the client can display connection latency.
                    await ws.send_json({"type": "pong", "t": data.get("t")})
                    continue
                if data.get("type") != "state":
                    continue

                state["name"] = clean_name(data.get("name"), state["name"])
                state["x"] = safe_float(data.get("x"), state["x"], 25, WORLD_WIDTH - 25)
                state["y"] = safe_float(data.get("y"), state["y"], 25, WORLD_HEIGHT - 25)
                state["angle"] = safe_float(data.get("angle"), state["angle"], -100000, 100000) % (math.pi * 2)
                state["driving"] = bool(data.get("driving", False))
                requested_car = str(data.get("vehicleId", "hatch"))
                state["vehicleId"] = requested_car if requested_car in {"hatch", "sedan", "sport", "suv"} else "hatch"
                state["last_seen"] = time.monotonic()
            elif message.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED):
                break
    finally:
        clients.pop(ws, None)

    return ws


async def broadcast_loop(app):
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)
        if not clients:
            continue
        players = [public_player(state) for ws, state in list(clients.items()) if not ws.closed]
        if not players:
            continue
        payload = json.dumps({"type": "players", "count": len(players), "players": players}, ensure_ascii=False, separators=(",", ":"))
        active_sockets = [ws for ws in list(clients.keys()) if not ws.closed]
        if active_sockets:
            results = await asyncio.gather(*(ws.send_str(payload) for ws in active_sockets), return_exceptions=True)
            # Closed or broken sockets are removed by their own handler; this is a safe extra cleanup.
            for ws, result in zip(active_sockets, results):
                if isinstance(result, Exception):
                    clients.pop(ws, None)


async def on_startup(app):
    app["broadcast_task"] = asyncio.create_task(broadcast_loop(app))


async def on_cleanup(app):
    task = app.get("broadcast_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def make_app():
    if not INDEX_FILE.exists():
        raise RuntimeError("index.html is missing. Put index.html beside server.py.")
    app = web.Application(client_max_size=16 * 1024)
    app.router.add_get("/", index_page)
    app.router.add_get("/healthz", health)
    app.router.add_get("/ws", multiplayer_socket)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    web.run_app(make_app(), host="0.0.0.0", port=port, access_log=None)
