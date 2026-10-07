# Polymarket-Kalshi_arbitrage

Collect tennis match-winner order-book data from **Polymarket US** and **Kalshi** for future cross-exchange arbitrage research. The current focus is ATP and ATP Challenger matches.

**Current stage: data collection.** Market matching, arbitrage detection, and trading are not implemented yet.

## What the collectors do

- Discover available match-winner markets periodically and update WebSocket subscriptions.
- Save order-book updates and separate market metadata as JSON Lines (`.jsonl`).
- Reconnect after connection errors; Kalshi requests fresh snapshots when its book becomes invalid.
- Restart at midnight **UTC**, creating a new dated folder and timestamped files.
- Compress closed order-book files into `.jsonl.gz` at subsequent startup. The original is deleted only after verifying that decompression reproduces its contents.
- Keep metadata files uncompressed.

There are two independent scripts, one per exchange.

## Project structure

```text
Polymarket-Kalshi_arbitrage/
├── collector/
│   ├── collector_Polymarket.py
│   ├── collector_Kalshi.py
│   ├── check_keys_Polymarket.py
│   └── check_keys_Kalshi.py
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
└── LICENSE
```

The authentication helpers create signed connection headers. Credentials are read from a local `.env` in the **repository root**, and the Kalshi private key is stored in a separate local file.

The collectors create `data/` automatically. Paths are resolved from the script location, so credentials and output do not depend on the terminal's working directory.

## Setup

Use Python 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/). The requirements file pins the direct dependency versions used in the offline checks.

Clone the repository and create its environment:

```shell
git clone https://github.com/clementeromano/Polymarket-Kalshi_arbitrage.git
cd Polymarket-Kalshi_arbitrage
uv venv --python 3.13
uv pip install -r requirements.txt
```

The dependencies are `requests`, `websocket-client`, `python-dotenv`, and `cryptography`. The package named `websocket-client` is imported as `websocket` in Python.

These commands use the repository's `.venv`, following the [uv environment workflow](https://docs.astral.sh/uv/pip/environments/).

### Configure credentials

Copy `.env.example` to `.env`.

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux/macOS:

```bash
cp .env.example .env
```

Edit the local `.env` with your own values:

```dotenv
POLYMARKET_KEY_ID=YOUR_POLYMARKET_KEY_ID
POLYMARKET_SECRET_KEY=YOUR_BASE64_POLYMARKET_SECRET_KEY

KALSHI_KEY_ID=YOUR_KALSHI_KEY_ID
KALSHI_PRIVATE_KEY_PATH=./Kalshi_key.txt
```

Save the RSA private key downloaded from Kalshi as `Kalshi_key.txt` in the repository root. Preserve the key's original PEM contents. A relative key path is resolved from that root; an absolute path also works.

The Polymarket collector uses **Polymarket US** credentials and endpoints. It does not use the international Polymarket CLOB API.

Real `.env` files, private keys, virtual environments, and collected data are excluded by `.gitignore`. Keep only placeholder credentials in `.env.example`.

## Run

Open two terminals in the repository root and run one collector in each. These commands explicitly use the Python environment created above, so activation is optional.

Windows PowerShell:

```powershell
# Terminal 1
.\.venv\Scripts\python.exe collector\collector_Polymarket.py
```

```powershell
# Terminal 2
.\.venv\Scripts\python.exe collector\collector_Kalshi.py
```

Linux/macOS:

```bash
# Terminal 1
./.venv/bin/python collector/collector_Polymarket.py
```

```bash
# Terminal 2
./.venv/bin/python collector/collector_Kalshi.py
```

Use **Ctrl+C** to stop a collector. Run only one instance of each collector against its output directory: startup compression assumes the other files in that directory are closed.

## Output

Files are grouped by exchange and UTC date. Their names include the process start time, allowing multiple runs within the same day. Kalshi names also include microseconds.

```text
data/
├── Polymarket/
│   └── 2026_10_07/
│       ├── order_books_Polymarket_2026-10-07_14-30-00.jsonl
│       └── metadata_Polymarket_2026-10-07_14-30-00.jsonl
└── Kalshi/
    └── 2026_10_07/
        ├── order_books_Kalshi_2026-10-07_14-30-00_123456.jsonl
        └── metadata_Kalshi_2026-10-07_14-30-00_123456.jsonl
```

A brief WebSocket reconnect continues the current files. A full process restart creates new files. When the next run starts, older order-book files are compressed in the background; metadata remains `.jsonl`.

### Order-book records

Each line is an independent JSON object.

| Collector | Stored records | Market identifier |
| --- | --- | --- |
| Polymarket US | Full `bids` / `offers` books from `marketData` messages | `slug` and `market_id` |
| Kalshi | Full `yes` / `no` snapshots plus individual price-level deltas | `ticker` and `market_id` |

Kalshi snapshots map prices to quantities, with decimal values stored as strings. A delta contains `side`, `price_dollars`, and `delta_fp`: add the signed delta to the quantity at that price, removing the level if the result is zero. A snapshot replaces the previous book for that ticker.

Kalshi also stores `connection_id`, `sid`, and `seq`, including sequenced acknowledgements. Sequence numbers are shared across markets within a subscription. The collector requests fresh snapshots approximately every five minutes, writing them into the same file.

`received_at` records the collector's UTC receipt time. `update_time` records the exchange timestamp when provided; it can be `null`, particularly on Kalshi snapshots.

### Metadata

Metadata uses `record_type: "start"` and `record_type: "end"`:

- A start record contains `first_timestamp`, identifiers, outcome labels, fee information, and `first_orderbook`.
- An end record contains `last_timestamp`, identifiers, outcome labels, and fee information when discovery removes a previously collected market.

These timestamps describe the collector's observations. A market still available when the process stops or the day changes may have no end record in that run.

Kalshi metadata includes `event_ticker`, which groups the player markets belonging to a match. Its opponent label is populated when discovery finds exactly two markets for that event; otherwise it is `null`.

Kalshi fees are saved as the API's `fee_type` and `fee_multiplier`. Polymarket saves `feeCoefficient` under `fee_coefficient`. Fee records include `observed_at`; missing fee information can be `null`. These are observed fee parameters, not a calculated dollar fee or a complete fee-change history.

## Current limitations

- Kalshi discovery covers `KXATPMATCH` and `KXATPCHALLENGERMATCH` and follows pagination. Polymarket uses its sports `16` endpoint, filters `tennis_match_winner`.
- Discovery includes available upcoming matches as well as matches already underway. An open market does not establish that a match is live.
- The two collectors have different discovery coverage and record formats. Cross-exchange match mapping is not implemented.
- Reconnection restores current books but does not backfill updates missed during an outage.
- Daily restart and reconnection require the Python process to remain alive. An external service manager is needed to recover from a machine reboot or process termination on a server.

## License

[MIT](LICENSE).
