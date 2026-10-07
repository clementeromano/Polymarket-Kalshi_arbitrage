import gzip
import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests
import websocket
from check_keys_Polymarket import create_headers

slug_to_long_outcomes = {}
slug_to_short_outcomes = {}
slug_to_market_id = {}
slug_to_market_state = {}
slug_to_fee = {}
stop = threading.Event()
shutdown = threading.Event()
restart_requested = threading.Event()
metadata_active = set()
metadata_removed = set()
metadata_lock = threading.Lock()
ws = None

ws_url = "wss://api.polymarket.us/v1/ws/markets"
url = "https://gateway.polymarket.us/v1/sports/16/events"

STARTED_AT = datetime.now(timezone.utc)
CURRENT_DAY = STARTED_AT.strftime("%Y_%m_%d")
RUN_TIMESTAMP = STARTED_AT.strftime("%Y-%m-%d_%H-%M-%S")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "Polymarket"
DAY_FOLDER = DATA_ROOT / CURRENT_DAY
DATA_FILE = DAY_FOLDER / f"order_books_Polymarket_{RUN_TIMESTAMP}.jsonl"
METADATA_FILE = DAY_FOLDER / f"metadata_Polymarket_{RUN_TIMESTAMP}.jsonl"


def prepare_daily_files():
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.touch(exist_ok=True)
    metadata_file().touch(exist_ok=True)
    load_metadata_state()


def metadata_file():
    """Keep metadata beside the order-book file, including in small offline tests."""
    if METADATA_FILE.parent == DATA_FILE.parent:
        return METADATA_FILE
    return DATA_FILE.parent / METADATA_FILE.name


def load_metadata_state():
    """Remember existing metadata if this process restarts during the same day."""
    metadata_active.clear()
    metadata_removed.clear()

    path = metadata_file()
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            try:
                row = json.loads(line)
                slug = row["slug"]
            except (json.JSONDecodeError, KeyError):
                continue

            if row.get("record_type") == "start":
                metadata_active.add(slug)
            elif row.get("record_type") == "end":
                metadata_active.discard(slug)
                metadata_removed.add(slug)

            if "market_id" in row:
                slug_to_market_id[slug] = row["market_id"]
            if "long_outcome" in row:
                slug_to_long_outcomes[slug] = row["long_outcome"]
            if "short_outcome" in row:
                slug_to_short_outcomes[slug] = row["short_outcome"]
            if "market_state" in row:
                slug_to_market_state[slug] = row["market_state"]
            if "fee" in row:
                slug_to_fee[slug] = row["fee"]


def record_metadata(slug, record_type, timestamp, first_orderbook=None):
    with metadata_lock:
        if record_type == "start":
            if slug in metadata_active or slug in metadata_removed:
                return
        elif slug not in metadata_active:
            return

        row = {
            "record_type": record_type,
            "first_timestamp" if record_type == "start" else "last_timestamp": timestamp,
            "market_id": slug_to_market_id[slug],
            "slug": slug,
            "long_outcome": slug_to_long_outcomes[slug],
            "short_outcome": slug_to_short_outcomes[slug],
            "market_state": slug_to_market_state.get(slug),
            "fee": slug_to_fee.get(slug),
        }
        if record_type == "start":
            row["first_orderbook"] = first_orderbook

        path = metadata_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(row, separators=(",", ":")) + "\n")

        if record_type == "start":
            metadata_active.add(slug)
        else:
            metadata_active.remove(slug)
            metadata_removed.add(slug)


def compress_file(path):
    """Compress a closed JSONL file and delete it only after verification."""
    compressed = Path(str(path) + ".gz")
    temporary = Path(str(compressed) + ".tmp")

    try:
        temporary.unlink(missing_ok=True)
        with path.open("rb") as source, gzip.open(
            temporary,
            "wb",
            compresslevel=1,
        ) as destination:
            shutil.copyfileobj(source, destination)

        with path.open("rb") as source, gzip.open(temporary, "rb") as restored:
            while chunk := source.read(1024 * 1024):
                if restored.read(len(chunk)) != chunk:
                    raise ValueError("Compression verification failed")
            if restored.read(1):
                raise ValueError("Unexpected extra data after decompression")

        temporary.replace(compressed)
        path.unlink()
        print("Compressed:", compressed)
    except Exception as error:  # noqa: BLE001 -- Never delete raw data after a failure.
        temporary.unlink(missing_ok=True)
        print("Compression failed; original file retained:", path, error)


def compress_old_files():
    """Compress closed order-book files from previous runs, never metadata."""
    if not DATA_ROOT.exists():
        return

    for path in DATA_ROOT.glob("*/order_books_Polymarket_*.jsonl"):
        if path != DATA_FILE:
            compress_file(path)


def seconds_until_midnight():
    now = datetime.now(timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return (next_midnight - now).total_seconds()


def request_daily_restart():
    print("UTC day finished. Restarting collector...")
    restart_requested.set()
    shutdown.set()
    stop.set()

    if ws is not None:
        ws.close()


def restart_script():
    sys.stdout.flush()
    sys.stderr.flush()
    os.execv(sys.executable, [sys.executable, *sys.argv])


def on_open(ws):
    prepare_daily_files()
    print("\nConnection established\n")
    discovery_thread.start()


def on_message(ws, message):
    received_at = datetime.now(timezone.utc).isoformat()
    data = json.loads(message)

    if data.get("error"):
        raise RuntimeError("Polymarket subscription error: " + str(data["error"]))

    if "marketData" not in data:
        return

    book = data["marketData"]
    slug = book["marketSlug"]
    slug_to_market_state[slug] = book.get("state")

    row = {
        "update_time": book.get("transactTime"),
        "received_at": received_at,
        "market_id": slug_to_market_id[slug],
        "slug": slug,
        "long_outcome": slug_to_long_outcomes[slug],
        "short_outcome": slug_to_short_outcomes[slug],
        "market_state": book.get("state"),
        "order_book": {
            "bids": book.get("bids"),
            "offers": book.get("offers"),
        },
    }

    print(
        "message received...",
        slug_to_long_outcomes[slug],
        "VS",
        slug_to_short_outcomes[slug],
    )

    with DATA_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row) + "\n")

    record_metadata(slug, "start", received_at, row["order_book"])


def check_updates():
    updated_slugs = set()

    response = requests.get(
        url,
        params={"limit": 50, "offset": 0},
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()

    for event in data["events"]:
        if not event.get("ended", False):
            for market in event.get("markets", []):
                if (
                    market.get("sportsMarketType")
                    == "tennis_match_winner"
                    and not market.get("closed", False)
                ):
                    new_slug = market["slug"]
                    slug_to_market_id[new_slug] = market["id"]
                    slug_to_fee[new_slug] = {
                        "fee_coefficient": market.get("feeCoefficient"),
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                    }

                    for side in market["marketSides"]:
                        if side["long"]:
                            slug_to_long_outcomes[new_slug] = side["description"]
                        else:
                            slug_to_short_outcomes[new_slug] = side["description"]

                    updated_slugs.add(new_slug)

    return updated_slugs


def check_updates_perpetually():
    current_slugs = set()
    subscription_id = None

    while not stop.wait(10):
        try:
            updated_slugs = check_updates()
        except requests.RequestException as error:
            print("Market discovery failed; retrying:", error)
            continue
        if stop.is_set():
            return

        with metadata_lock:
            removed_slugs = (current_slugs | metadata_active) - updated_slugs
            metadata_removed.difference_update(updated_slugs)
        for slug in removed_slugs:
            record_metadata(slug, "end", datetime.now(timezone.utc).isoformat())

        if updated_slugs != current_slugs:
            # One subscription holds the whole list; replace it when the list changes.
            if subscription_id is not None:
                ws.send(json.dumps({
                    "unsubscribe": {
                        "requestId": subscription_id,
                    }
                }))
                subscription_id = None

            if updated_slugs:
                subscription_id = str(uuid4())
                print("Subscribing to", len(updated_slugs), "markets")

                ws.send(json.dumps({
                    "subscribe": {
                        "requestId": subscription_id,
                        "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                        "marketSlugs": sorted(updated_slugs),
                    }
                }))

            current_slugs = updated_slugs


def run_discovery():
    try:
        check_updates_perpetually()
    except Exception as error:  # noqa: BLE001 -- Restart the connection if this worker fails.
        on_error(ws, error)


def on_error(ws, error):
    print("Collector error:", error)
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        shutdown.set()
    stop.set()
    ws.close()


def on_close(ws, status_code, message):
    print("Connection closed:", status_code, message)
    stop.set()


if __name__ == "__main__":
    prepare_daily_files()
    compression_thread = threading.Thread(target=compress_old_files, daemon=True)
    compression_thread.start()

    daily_timer = threading.Timer(
        seconds_until_midnight(),
        request_daily_restart,
    )
    daily_timer.daemon = True
    daily_timer.start()

    try:
        while not shutdown.is_set():
            stop.clear()
            discovery_thread = threading.Thread(target=run_discovery, daemon=True)
            ws = websocket.WebSocketApp(
                ws_url,
                header=create_headers,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            try:
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as error:  # noqa: BLE001 -- Retry recoverable Python failures.
                on_error(ws, error)
            finally:
                stop.set()
                ws.close()
                if discovery_thread.ident is not None:
                    discovery_thread.join()
            if shutdown.is_set():
                break
            print("Reconnecting in 5 seconds...")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Collector stopped.")
    finally:
        daily_timer.cancel()

    if restart_requested.is_set():
        restart_script()
