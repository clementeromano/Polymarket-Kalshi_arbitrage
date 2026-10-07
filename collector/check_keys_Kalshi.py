import os
from dotenv import load_dotenv
import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def create_headers():
    load_dotenv(PROJECT_ROOT / ".env")

    key_id = os.getenv("KALSHI_KEY_ID")
    private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")

    print("Key ID loaded:", bool(key_id))

    if not key_id or not private_key_path:
        raise ValueError("Set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH in .env")

    # Load the downloaded RSA private key.
    with (PROJECT_ROOT / private_key_path).open("rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )

    # Create the message that Kalshi requires us to sign.
    path = "/trade-api/ws/v2"
    timestamp = str(int(time.time() * 1000))
    message = timestamp + "GET" + path

    # Sign the message and convert the signature into text.
    signature_bytes = private_key.sign(
        message.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    signature = base64.b64encode(signature_bytes).decode()

    # Attach authentication information to the connection request.
    headers = {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
    }

    return headers
