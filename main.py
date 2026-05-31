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

MAX_GUESSES = 10
GUESS_TIMER = 10  # seconds per guess turn

# ── Comparison ───────────────────────────────────────────────────────────────

def compare(guess: dict, target: dict) -> dict:
    def num_cmp(g, t):
        if g == t: return "correct"
        return "high" if g > t else "low"

    def dob_year(dob): return int(dob.split("/")[2])
    def dob_cmp(g, t):
        gy, ty = dob_year(g), dob_year(t)
        if g == t: return "correct"
        if gy == ty: return "partial"
        return "high" if gy > ty else "low"

    def nat_cmp(g_nats, t_nats):
        g_set, t_set = set(g_nats), set(t_nats)
        if g_set == t_set: return {"result": "correct", "matching": list(g_set)}
        inter = g_set & t_set
        if inter: return {"result": "partial", "matching": list(inter)}
        return {"result": "wrong", "matching": []}

    def roles_cmp(g_roles, t_roles):
        g_set, t_set = set(g_roles), set(t_roles)
        if g_set == t_set: return {"result": "correct", "matching": list(g_set)}
        inter = g_set & t_set
        if inter: return {"result": "partial", "matching": list(inter)}
        return {"result": "wrong", "matching": []}

    def height_cmp(g, t):
        if g == t: return "correct"
        if abs(g - t) <= 2: return "partial"
        return "high" if g > t else "low"

    return {
        "name": guess["name"],
        "group":   {"value": guess["group"],   "result": "correct" if guess["group"]  == target["group"]  else "wrong"},
        "agency":  {"value": guess["agency"],  "result": "correct" if guess["agency"] == target["agency"] else "wrong"},
        "nationalities": {"value": guess["nationalities"], **nat_cmp(guess["nationalities"], target["nationalities"])},
        "dob":     {"value": guess["dob"],     "result": dob_cmp(guess["dob"], target["dob"])},
        "height":  {"value": guess["height"],  "result": height_cmp(guess["height"], target["height"])},
        "roles":   {"value": guess["roles"],   **roles_cmp(guess["roles"], target["roles"])},
        "debut":   {"value": guess["debut"],   "result": num_cmp(guess["debut"],  target["debut"])},
        "members": {"value": guess["members"], "result": num_cmp(guess["members"], target["members"])},
        "correct": guess["name"] == target["name"],
    }

# ── Room ─────────────────────────────────────────────────────────────────────

class Player:
    def __init__(self, ws: WebSocket, name: str, pid: str):
        self.ws = ws
        self.name = name
        self.id = pid
        self.score = 0
        self.found = False
        self.guesses_this_round = 0

class Room:
    def __init__(self, code: str, host_id: str):
        self.code = code
        self.host_id = host_id
        self.players: Dict[str, Player] = {}
        self.target: Optional[dict] = None
        self.phase = "lobby"
        self.round = 0
        self.total_rounds = 5
        self.guess_count = 0       # total guesses this round
        self.timer_task: Optional[asyncio.Task] = None
        self.round_start_time: Optional[float] = None
        self.used_idols: list = []

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
        pool = [i for i in IDOLS if i["name"] not in self.used_idols]
        if not pool:
            self.used_idols = []
            pool = IDOLS
        self.target = random.choice(pool)
        self.used_idols.append(self.target["name"])

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
        self.guess_count = 0
        self.round_start_time = time.time()
        for p in self.players.values():
            p.found = False
            p.guesses_this_round = 0

        await self.broadcast({
            "type": "round_start",
            "round": self.round + 1,
            "total_rounds": self.total_rounds,
            "max_guesses": MAX_GUESSES,
            "timer": GUESS_TIMER,
        })
        self.timer_task = asyncio.create_task(self.run_guess_timer())

    async def run_guess_timer(self):
        await asyncio.sleep(GUESS_TIMER)
        if self.phase == "playing":
            # time's up for this guess turn — broadcast timeout
            await self.broadcast({"type": "guess_timeout", "guess_count": self.guess_count})
            # check if round should end
            await self.check_round_end()

    async def check_round_end(self):
        if self.phase != "playing":
            return
        all_found = all(p.found for p in self.players.values())
        if all_found or self.guess_count >= MAX_GUESSES:
            await self.end_round()
        else:
            # next guess timer
            if self.timer_task:
                self.timer_task.cancel()
            self.timer_task = asyncio.create_task(self.run_guess_timer())

    async def end_round(self):
        if self.phase != "playing":
            return
        self.phase = "results"
        if self.timer_task:
            self.timer_task.cancel()

        self.round += 1
        scores_this_round = {}
        for p in self.players.values():
            if p.found:
                speed_bonus = max(0, int((GUESS_TIMER - (time.time() - self.round_start_time)) * 5))
                guess_bonus = max(0, (MAX_GUESSES - p.guesses_this_round) * 50)
                pts = 300 + speed_bonus + guess_bonus
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

rooms: Dict[str, Room] = {}

def make_code():
    while True:
        code = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=4))
        if code not in rooms:
            return code

# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    player: Optional[Player] = None
    room: Optional[Room] = None

    try:
        async for raw in websocket.iter_json():
            action = raw.get("action")

            if action == "create_room":
                code = make_code()
                pid = str(uuid.uuid4())[:8]
                player = Player(websocket, raw["name"], pid)
                room = Room(code, pid)
                room.players[pid] = player
                rooms[code] = room
                await websocket.send_json({"type": "room_created", "code": code, "player_id": pid})
                await room.broadcast(room.to_lobby_state())

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

            elif action == "start_game":
                if room and player and player.id == room.host_id and room.phase == "lobby":
                    await room.start_round()

            elif action == "guess":
                if not room or not player or room.phase != "playing":
                    continue
                if player.found:
                    await websocket.send_json({"type": "error", "msg": "Tu as déjà trouvé ce round !"})
                    continue
                idol_name = raw["name"]
                idol = next((i for i in IDOLS if i["name"] == idol_name), None)
                if not idol:
                    await websocket.send_json({"type": "error", "msg": "Idole inconnue."})
                    continue

                result = compare(idol, room.target)
                player.guesses_this_round += 1
                room.guess_count += 1

                # broadcast to everyone so all see the guess
                await room.broadcast({
                    "type": "guess_result",
                    "result": result,
                    "player_name": player.name,
                    "player_id": player.id,
                    "guess_count": room.guess_count,
                    "max_guesses": MAX_GUESSES,
                })

                if result["correct"]:
                    player.found = True
                    await room.broadcast({
                        "type": "player_found",
                        "player_name": player.name,
                        "player_id": player.id,
                    })

                # cancel current timer and check round end
                if room.timer_task:
                    room.timer_task.cancel()
                await room.check_round_end()

            elif action == "next_round":
                if room and player and player.id == room.host_id and room.phase == "results":
                    room.phase = "lobby"
                    await room.start_round()

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
