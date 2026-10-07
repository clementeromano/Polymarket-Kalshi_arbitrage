import gzip
import json
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import count
from pathlib import Path

import requests
import websocket
from check_keys_Kalshi import create_headers

ticker_to_long_outcomes = {}
ticker_to_short_outcomes = {}
ticker_to_market_id = {}
ticker_to_market_state = {}
ticker_to_event_ticker = {}
ticker_to_series = {}
series_to_fee = {}
ticker_to_current_order_book = {}
subscription_id = None
last_sequence = None
subscription_ready = threading.Event()
request_ids = count(1)
stop = threading.Event()
shutdown = threading.Event()
restart_requested = threading.Event()
ws = None

ws_url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
url = "https://external-api.kalshi.com/trade-api/v2/markets"
SERIES = ("KXATPMATCH", "KXATPCHALLENGERMATCH")

STARTED_AT = datetime.now(timezone.utc)
CURRENT_DAY = STARTED_AT.strftime("%Y_%m_%d")
RUN_TIMESTAMP = STARTED_AT.strftime("%Y-%m-%d_%H-%M-%S_%f")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "Kalshi"
DAY_FOLDER = DATA_ROOT / CURRENT_DAY
DATA_FILE = DAY_FOLDER / f"order_books_Kalshi_{RUN_TIMESTAMP}.jsonl"
METADATA_FILE = DAY_FOLDER / f"metadata_Kalshi_{RUN_TIMESTAMP}.jsonl"
archived_tickers = set()
metadata_active = set()
metadata_removed = set()
metadata_lock = threading.Lock()
compression_worker = ThreadPoolExecutor(max_workers=1)


def compress_file(path):
    """Compress a CLOSED file; keep the original if anything fails."""
    compressed = Path(str(path) + ".gz")
    temporary = Path(str(compressed) + ".tmp")
    try:
        temporary.unlink(missing_ok=True)
        if compressed.exists():
            raise FileExistsError(f"Compressed file already exists: {compressed}")

        with path.open("rb") as source, temporary.open("xb") as destination:
            with gzip.GzipFile(fileobj=destination, mode="wb", compresslevel=1) as packed:
                shutil.copyfileobj(source, packed)
            destination.flush()
            os.fsync(destination.fileno())

        # Verify an exact round trip before removing the uncompressed copy.
        with path.open("rb") as source, gzip.open(temporary, "rb") as restored:
            while chunk := source.read(1024 * 1024):
                if restored.read(len(chunk)) != chunk:
                    raise ValueError("Compression verification failed")
            if restored.read(1):
                raise ValueError("Unexpected extra data after decompression")
        temporary.replace(compressed)
        path.unlink()
        print("Compressed:", compressed.name)
    except Exception as error:  # noqa: BLE001 -- Keep raw data and report compression failures.
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        print("Compression failed; original file retained:", path, error)


def prepare_daily_files():
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.touch(exist_ok=True)
    METADATA_FILE.touch(exist_ok=True)
    print("Saving data to:", DATA_FILE)


def compress_previous_files():
    """Compress old daily order-book files, never metadata or today's live file."""
    for path in sorted(DATA_ROOT.glob("*/order_books_Kalshi_*.jsonl")):
        if path != DATA_FILE:
            compression_worker.submit(compress_file, path)


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


def write_row(row):
    with DATA_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, separators=(",", ":")) + "\n")


def write_snapshot(row, ticker, source):
    row["snapshot_source"] = source
    row["order_book"] = ticker_to_current_order_book[ticker]
    if ticker not in archived_tickers:
        row.update({
            "event_ticker": ticker_to_event_ticker[ticker],
            "long_outcome": ticker_to_long_outcomes[ticker],
            "short_outcome": ticker_to_short_outcomes[ticker],
        })
    row["market_state"] = ticker_to_market_state[ticker]  # Last REST observation.
    write_row(row)
    archived_tickers.add(ticker)
    record_metadata(ticker, "start", row["received_at"])


def record_metadata(ticker, record_type, timestamp):
    """Append one start/end pair per collected market; reconnects are not market removals."""
    with metadata_lock:
        if record_type == "start":
            if ticker in metadata_active or ticker in metadata_removed:
                return
        else:
            metadata_removed.add(ticker)
            if ticker not in metadata_active:
                return

        row = {
            "record_type": record_type,
            "first_timestamp" if record_type == "start" else "last_timestamp": timestamp,
            "market_id": ticker_to_market_id[ticker],
            "ticker": ticker,
            "event_ticker": ticker_to_event_ticker[ticker],
            "long_outcome": ticker_to_long_outcomes[ticker],
            "short_outcome": ticker_to_short_outcomes[ticker],
            "fee": series_to_fee.get(ticker_to_series[ticker]),
        }
        if record_type == "start":
            row["first_orderbook"] = ticker_to_current_order_book[ticker]
        with METADATA_FILE.open("a", encoding="utf-8") as file:
            file.write(json.dumps(row, separators=(",", ":")) + "\n")
        if record_type == "start":
            metadata_active.add(ticker)
        else:
            metadata_active.remove(ticker)


def refresh_fee(series):
    now = datetime.now(timezone.utc)
    previous = series_to_fee.get(series)
    if previous and (now - datetime.fromisoformat(previous["observed_at"])).total_seconds() < 300:
        return
    try:
        response = requests.get(url.rsplit("/", 1)[0] + "/series/" + series, timeout=10)
        response.raise_for_status()
        details = response.json()["series"]
        series_to_fee[series] = {
            "series_ticker": series,
            "fee_type": details["fee_type"],
            "fee_multiplier": details["fee_multiplier"],
            "observed_at": now.isoformat(),
        }
    except (requests.RequestException, KeyError, ValueError) as error:
        # Unknown fees remain null; a previous rule keeps its original observation time.
        print("Fee lookup failed; retrying:", series, error)


def update_subscription(ws, tickers, action):
    ws.send(json.dumps({
        "id": next(request_ids),
        "cmd": "update_subscription",
        "params": {
            "sid": subscription_id,
            "market_tickers": list(tickers),
            "action": action,
        },
    }))


def on_open(ws):
    global subscription_id, last_sequence
    prepare_daily_files()
    ticker_to_current_order_book.clear()
    subscription_id = None
    last_sequence = None
    subscription_ready.clear()
    ws.connection_id = datetime.now(timezone.utc).isoformat()
    print("\nConnection established\n")
    discovery_thread.start()


def on_message(ws, message):
    global subscription_id, last_sequence
    received_at = datetime.now(timezone.utc).isoformat()
    data = json.loads(message)
    message_type = data["type"]

    if message_type == "error":
        raise RuntimeError(f"Kalshi rejected a request: {data['msg']}")
    if message_type == "subscribed":
        subscription_id = data["msg"]["sid"]
        subscription_ready.set()
        return

    # Sequence numbers cover ALL markets and acknowledgements on this subscription.
    if "seq" in data and data.get("sid") == subscription_id:
        sequence = data["seq"]
        if last_sequence is not None:
            if sequence <= last_sequence:
                return
            if sequence != last_sequence + 1:
                raise ValueError("Sequence gap; reconnecting for fresh snapshots")
        last_sequence = sequence
    if data.get("sid") != subscription_id or "seq" not in data:
        return

    row = {
        "received_at": received_at,
        "message_type": message_type,
        "connection_id": ws.connection_id,
        "sid": data["sid"],
        "seq": data["seq"],
    }
    if message_type not in ("orderbook_snapshot", "orderbook_delta"):
        # Keep sequenced acknowledgements so archive gaps mean genuinely missing messages.
        write_row(row)
        return

    book = data["msg"]
    ticker = book["market_ticker"]
    ticker_to_market_id[ticker] = book["market_id"]
    row.update({
        "update_time": book.get("ts"),  # Initial snapshots may have no exchange time.
        "market_id": ticker_to_market_id[ticker],
        "ticker": ticker,
    })
    if "ts_ms" in book:
        row["ts_ms"] = book["ts_ms"]

    if message_type == "orderbook_snapshot":
        # Replace the entire book, including during the five-minute refresh.
        ticker_to_current_order_book[ticker] = {
            "yes": {f"{Decimal(p):.4f}": q for p, q in (book.get("yes_dollars_fp") or [])},
            "no": {f"{Decimal(p):.4f}": q for p, q in (book.get("no_dollars_fp") or [])},
        }
        write_snapshot(row, ticker, "exchange")
    else:
        if ticker not in ticker_to_current_order_book:
            raise ValueError("Delta arrived without a snapshot: " + ticker)

        side = ticker_to_current_order_book[ticker][book["side"]]
        price = f"{Decimal(book['price_dollars']):.4f}"
        quantity = Decimal(side.get(price, "0")) + Decimal(book["delta_fp"])
        if quantity < 0:
            raise ValueError("Negative book quantity; requesting a fresh connection")

        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = str(quantity)
        row.update({"side": book["side"], "price_dollars": price,
                    "delta_fp": book["delta_fp"]})
        write_row(row)
    print("message received...", ticker_to_long_outcomes[ticker], message_type)


def check_updates():
    updated_tickers = set()
    event_to_tickers = {}

    for series in SERIES:
        refresh_fee(series)
        cursor = ""
        while not stop.is_set():
            response = requests.get(url, params={
                "series_ticker": series,
                "mve_filter": "exclude",
                "status": "open",
                "limit": 100,
                "cursor": cursor,
            }, timeout=10)
            response.raise_for_status()
            data = response.json()

            for market in data["markets"]:
                ticker = market["ticker"]
                event_ticker = market["event_ticker"]
                ticker_to_long_outcomes[ticker] = market["title"]
                ticker_to_market_state[ticker] = market["status"]
                ticker_to_event_ticker[ticker] = event_ticker
                ticker_to_series[ticker] = series
                event_to_tickers.setdefault(event_ticker, set()).add(ticker)
                updated_tickers.add(ticker)

            cursor = data.get("cursor", "")
            if not cursor:
                break

    for event_tickers in event_to_tickers.values():
        for ticker in event_tickers:
            opponents = event_tickers - {ticker}
            if len(opponents) == 1:
                opponent_ticker = next(iter(opponents))
                ticker_to_short_outcomes[ticker] = ticker_to_long_outcomes[opponent_ticker]
            else:
                ticker_to_short_outcomes[ticker] = None

    return updated_tickers


def check_updates_perpetually():
    current_tickers = set()
    last_refresh = time.monotonic()

    try:
        while not stop.is_set():
            try:
                updated_tickers = check_updates()
            except requests.RequestException as error:
                print("Market discovery failed; retrying:", error)
                updated_tickers = current_tickers  # Keep subscriptions; still refresh books.
            else:
                with metadata_lock:
                    removed = (current_tickers | metadata_active) - updated_tickers
                    metadata_removed.difference_update(updated_tickers)
                for ticker in removed:
                    record_metadata(ticker, "end", datetime.now(timezone.utc).isoformat())
            if stop.is_set():
                return

            to_add = updated_tickers - current_tickers
            to_remove = current_tickers - updated_tickers

            if subscription_id is None and updated_tickers:
                print("Subscribing to", len(updated_tickers), "markets")
                ws.send(json.dumps({
                    "id": next(request_ids),
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": list(updated_tickers),
                    },
                }))
                if not subscription_ready.wait(10):
                    raise TimeoutError("No subscription acknowledgement received")
            else:
                if to_add:
                    print("Sending add request...", len(to_add), "markets")
                    update_subscription(ws, to_add, "add_markets")
                if to_remove:
                    print("Sending remove request...", len(to_remove), "markets")
                    update_subscription(ws, to_remove, "delete_markets")
            current_tickers = updated_tickers

            # Actual elapsed time, so slow REST requests do not accumulate timer drift.
            if time.monotonic() - last_refresh >= 300:
                if current_tickers:
                    update_subscription(ws, current_tickers, "get_snapshot")
                last_refresh = time.monotonic()
                print("Requested five-minute snapshots")

            if stop.wait(10):
                return
    except Exception as error:  # noqa: BLE001 -- Restart the connection if this worker fails.
        on_error(ws, error)


def on_error(ws, error):
    print("Collector error:", error)
    ws.has_errored = not isinstance(error, (KeyboardInterrupt, SystemExit))
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        shutdown.set()
    stop.set()
    ws.close()


def on_close(ws, status_code, message):
    print("Connection closed:", status_code, message)
    stop.set()


if __name__ == "__main__":
    compress_previous_files()
    daily_timer = threading.Timer(
        seconds_until_midnight(),
        request_daily_restart,
    )
    daily_timer.daemon = True
    daily_timer.start()

    try:
        while not shutdown.is_set():
            stop.clear()
            discovery_thread = threading.Thread(target=check_updates_perpetually, daemon=True)
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
        compression_worker.shutdown(wait=True)

    if restart_requested.is_set():
        restart_script()
