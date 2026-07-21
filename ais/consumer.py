"""Live AIS consumer (aisstream.io websocket) -> bronze ndjson.

Subscribes to the bounding boxes marked `subscribed: true` in config/ports.yml
(Phase 0: LA/Long Beach) for PositionReport + ShipStaticData messages and
appends them to data/bronze/ais/YYYY-MM-DD/positions.ndjson with a received_at
stamp. Reconnects forever with exponential backoff — an always-on service.

Requires AISSTREAM_API_KEY (free key from https://aisstream.io). Without it,
exits with a clear message (dev machines without a key just skip this service).

Usage:
    AISSTREAM_API_KEY=... python -m ais.consumer
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
import yaml

ROOT = Path(__file__).resolve().parents[1]
BRONZE_AIS = ROOT / "data" / "bronze" / "ais"
URL = "wss://stream.aisstream.io/v0/stream"

PORTS_CFG = yaml.safe_load((ROOT / "config" / "ports.yml").read_text())


def subscribed_boxes() -> list[list[list[float]]]:
    return [b["box"] for b in PORTS_CFG["ais_boxes"].values() if b.get("subscribed")]


def out_path(now: datetime) -> Path:
    p = BRONZE_AIS / now.strftime("%Y-%m-%d") / "positions.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


async def consume(api_key: str) -> None:
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL) as ws:
                await ws.send(json.dumps({
                    "APIKey": api_key,
                    "BoundingBoxes": subscribed_boxes(),
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
                }))
                print(f"connected, boxes={subscribed_boxes()}", file=sys.stderr)
                backoff = 1
                async for raw in ws:
                    now = datetime.now(timezone.utc)
                    msg = json.loads(raw)
                    msg["received_at"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                    with out_path(now).open("a") as f:
                        f.write(json.dumps(msg) + "\n")
        except (websockets.WebSocketException, OSError) as e:
            print(f"disconnected ({e!r}); reconnecting in {backoff}s", file=sys.stderr)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def main() -> None:
    api_key = os.environ.get("AISSTREAM_API_KEY")
    if not api_key:
        sys.exit("AISSTREAM_API_KEY not set — get a free key at https://aisstream.io. "
                 "Skipping AIS consumption (dev mode).")
    asyncio.run(consume(api_key))


if __name__ == "__main__":
    main()
