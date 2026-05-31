import asyncio
import json
import random
import time
import uuid
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from idols import IDOLS

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GUESS_TIMER = 30  # seconds per guess

# ── Room state ──────────────────────────────────────────────────────────────

class Player:
    def __init__(self, ws: WebSocket, name: str, pid: str):
        self.ws = ws
        self.name = name
        self.id = pid
        self.score = 0
        self.guesses: list = []
        self.found = False
        self.found_at: Optional[float] = None

class Room:
    def __init__(self, code: str, host_id: str):
        self.code = code
        self.host_id = host_id
        self.players: Dict[str, Player] = {}
        self.target: Optional[dict] = None
        self.phase = "lobby"   # lobby | playing | results
        self.round = 0
        self.total_rounds = 5
        self.timer_task: Optional[asyncio.Task] = None
        self.guess_start: Optional[float] = None

    def to_lobby_state(self):
        return {
            "type": "lobby_state",
            "code": self.code,
            "host": self.host_id,
            "players": [{"id": p.id, "name": p.name, "score": p.score} for p in self.players.values()],
            "round": self.round,
            "total_rounds": self.total_rounds,
        }

    def pick_target(self):
        used = [g["name"] for p in self.players.values() for g in p.guesses]
        pool = [i for i in IDOLS if i["name"] not in used]
        if not pool:
            pool = IDOLS
        self.target = random.choice(pool)

    async def broadcast(self, msg: dict):
        dead = []
        for p in self.players.values():
            try:
                await p.ws.send_json(msg)
            except Exception:
                dead.append(p.id)
        for d in dead:
            self.players.pop(d, None)

    async def start_round(self):
        self.phase = "playing"
        self.pick_target()
        self.guess_start = time.time()
        for p in self.players.values():
            p.found = False
            p.found_at = None
        await self.broadcast({
            "type": "round_start",
            "round": self.round + 1,
            "total_rounds": self.total_rounds,
            "timer": GUESS_TIMER,
            "idol_count": len(IDOLS),
        })
        self.timer_task = asyncio.create_task(self.run_timer())

    async def run_timer(self):
        await asyncio.sleep(GUESS_TIMER)
        if self.phase == "playing":
            await self.end_round()

    async def end_round(self):
        if self.phase != "playing":
            return
        self.phase = "results"
        if self.timer_task:
            self.timer_task.cancel()

        self.round += 1
        scores_this_round = {}
        for p in self.players.values():
            if p.found and p.found_at is not None:
                elapsed = p.found_at - self.guess_start
                n_guesses = len([g for g in p.guesses if g.get("round") == self.round - 1])
                time_bonus = max(0, int((GUESS_TIMER - elapsed) * 10))
                guess_bonus = max(0, (10 - n_guesses) * 50)
                pts = 500 + time_bonus + guess_bonus
                p.score += pts
                scores_this_round[p.id] = pts
            else:
                scores_this_round[p.id] = 0

        await self.broadcast({
            "type": "round_end",
            "round": self.round,
            "answer": self.target,
            "scores_this_round": scores_this_round,
            "leaderboard": sorted(
                [{"id": p.id, "name": p.name, "score": p.score} for p in self.players.values()],
                key=lambda x: -x["score"]
            ),
            "game_over": self.round >= self.total_rounds,
        })

# ── Global rooms store ───────────────────────────────────────────────────────

rooms: Dict[str, Room] = {}

def make_code():
    while True:
        code = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=4))
        if code not in rooms:
            return code

# ── Guess comparison logic ────────────────────────────────────────────────────

def compare(guess: dict, target: dict) -> dict:
    def num_cmp(g, t):
        if g == t: return "correct"
        return "high" if g > t else "low"

    return {
        "name": guess["name"],
        "group":    {"value": guess["group"],   "result": "correct" if guess["group"]   == target["group"]   else "wrong"},
        "agency":   {"value": guess["agency"],  "result": "correct" if guess["agency"]  == target["agency"]  else "wrong"},
        "country":  {"value": guess["country"], "result": "correct" if guess["country"] == target["country"] else "wrong"},
        "age":      {"value": guess["age"],     "result": num_cmp(guess["age"],     target["age"])},
        "debut":    {"value": guess["debut"],   "result": num_cmp(guess["debut"],   target["debut"])},
        "members":  {"value": guess["members"], "result": num_cmp(guess["members"], target["members"])},
        "correct": guess["name"] == target["name"],
    }

# ── WebSocket handler ────────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    player: Optional[Player] = None
    room: Optional[Room] = None

    try:
        async for raw in websocket.iter_json():
            action = raw.get("action")

            # ── CREATE ROOM ──
            if action == "create_room":
                code = make_code()
                pid = str(uuid.uuid4())[:8]
                player = Player(websocket, raw["name"], pid)
                room = Room(code, pid)
                room.players[pid] = player
                rooms[code] = room
                await websocket.send_json({"type": "room_created", "code": code, "player_id": pid})
                await room.broadcast(room.to_lobby_state())

            # ── JOIN ROOM ──
            elif action == "join_room":
                code = raw["code"].upper()
                if code not in rooms:
                    await websocket.send_json({"type": "error", "msg": "Salon introuvable."})
                    continue
                room = rooms[code]
                if room.phase != "lobby":
                    await websocket.send_json({"type": "error", "msg": "Partie déjà en cours."})
                    continue
                pid = str(uuid.uuid4())[:8]
                player = Player(websocket, raw["name"], pid)
                room.players[pid] = player
                await websocket.send_json({"type": "room_joined", "code": code, "player_id": pid})
                await room.broadcast(room.to_lobby_state())

            # ── START GAME ──
            elif action == "start_game":
                if room and player and player.id == room.host_id and room.phase == "lobby":
                    if len(room.players) < 1:
                        await websocket.send_json({"type": "error", "msg": "Il faut au moins 1 joueur."})
                        continue
                    await room.start_round()

            # ── GUESS ──
            elif action == "guess":
                if not room or not player or room.phase != "playing":
                    continue
                idol_name = raw["name"]
                idol = next((i for i in IDOLS if i["name"] == idol_name), None)
                if not idol:
                    await websocket.send_json({"type": "error", "msg": "Idole inconnue."})
                    continue

                result = compare(idol, room.target)
                result["round"] = room.round
                player.guesses.append(result)

                await websocket.send_json({"type": "guess_result", "result": result})

                if result["correct"] and not player.found:
                    player.found = True
                    player.found_at = time.time()
                    await room.broadcast({
                        "type": "player_found",
                        "player_name": player.name,
                        "player_id": player.id,
                        "guesses": len(player.guesses),
                    })
                    all_found = all(p.found for p in room.players.values())
                    if all_found:
                        await room.end_round()

            # ── NEXT ROUND ──
            elif action == "next_round":
                if room and player and player.id == room.host_id:
                    if room.phase == "results" and room.round < room.total_rounds:
                        room.phase = "lobby"
                        await room.start_round()

            # ── PING ──
            elif action == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        pass
    finally:
        if room and player:
            room.players.pop(player.id, None)
            if room.players:
                if player.id == room.host_id:
                    room.host_id = next(iter(room.players))
                await room.broadcast(room.to_lobby_state())
            else:
                rooms.pop(room.code, None)
