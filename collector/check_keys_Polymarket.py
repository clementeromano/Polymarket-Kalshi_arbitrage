import os

from dotenv import load_dotenv
import base64
import time
from pathlib import Path

import websocket
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def create_headers():
    load_dotenv(PROJECT_ROOT / ".env")

    key_id = os.getenv("POLYMARKET_KEY_ID")
    secret_key = os.getenv("POLYMARKET_SECRET_KEY")

    print("Key ID loaded:", bool(key_id))
    print("Secret key loaded:", bool(secret_key))


    # Convert the stored secret into a signing key.
    secret_bytes = base64.b64decode(secret_key)
    private_key = Ed25519PrivateKey.from_private_bytes(secret_bytes[:32])

    # Create the message that Polymarket requires us to sign.
    path = "/v1/ws/markets"
    timestamp = str(int(time.time() * 1000))
    message = timestamp + "GET" + path

    # Sign the message and convert the signature into text.
    signature_bytes = private_key.sign(message.encode())
    signature = base64.b64encode(signature_bytes).decode()

    # Attach authentication information to the connection request.
    headers = {
        "X-PM-Access-Key": key_id,
        "X-PM-Timestamp": timestamp,
        "X-PM-Signature": signature,
    }

    return headers
#ws = websocket.create_connection(
#    "wss://api.polymarket.us" + path,
#    header=headers,
#    timeout=10,
#)
#
#print("Connected to Polymarket US!")
#ws.close()
#print("Connection closed.")
